package com.kaushal.riskengine.topology;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.CardProfile;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.EnrichedTransaction;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.Transaction;
import com.kaushal.riskengine.config.AvroSerdes;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * Enrichment: attach the card profile and the merchant to every transaction.
 *
 * <p>Three different join types, each chosen for a reason. That reasoning is the point of
 * this class; any of the three would technically "work" in the other two positions.
 *
 * <pre>
 *   cards JOIN customers                KTable-KTable foreign-key join -&gt; cardProfiles
 *   transactions LEFT JOIN cardProfiles KStream-KTable, co-partitioned
 *   ... LEFT JOIN merchants             KStream-GlobalKTable, by a key extractor
 * </pre>
 *
 * <p>The returned stream feeds {@link DecisionTopology}.
 */
@Component
public class EnrichmentTopology {

    private final AvroSerdes avroSerdes;

    public EnrichmentTopology(AvroSerdes avroSerdes) {
        this.avroSerdes = avroSerdes;
    }

    @Bean
    public KStream<String, EnrichedTransaction> enrichmentStream(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();

        SpecificAvroSerde<Card> cardSerde = avroSerdes.value();
        SpecificAvroSerde<Customer> customerSerde = avroSerdes.value();
        SpecificAvroSerde<Merchant> merchantSerde = avroSerdes.value();
        SpecificAvroSerde<CardProfile> cardProfileSerde = avroSerdes.value();
        SpecificAvroSerde<Transaction> transactionSerde = avroSerdes.value();
        SpecificAvroSerde<EnrichedTransaction> enrichedSerde = avroSerdes.value();

        // --- Reference data -------------------------------------------------------------
        //
        // Every reference store below has record caching DISABLED, deliberately.
        //
        // By default a KTable store sits behind a cache that only flushes downstream on
        // commit - every 30 seconds under at-least-once. For the foreign-key join that
        // means a new card is not forwarded to the join until the next commit, so its
        // CardProfile can take up to 30s to exist. Any authorisation on that card in the
        // meantime finds no profile and is dropped by the inner join below.
        //
        // TopologyTestDriver commits after every record, so the unit tests can never show
        // this. It was found running the real stack: a transaction 13s after its card was
        // written was dropped, and one sent after the next commit went through.
        //
        // Caching exists to collapse rapid updates to the same key into one downstream
        // record. Reference data barely changes, so here it buys nothing and costs up to
        // 30s of staleness on the one path where freshness matters.

        KTable<String, Card> cards = builder.table(
                Topics.CARDS,
                Consumed.with(stringSerde, cardSerde).withName("cards-source"),
                Materialized.<String, Card, KeyValueStore<Bytes, byte[]>>as(Topics.CARDS_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(cardSerde)
                        .withCachingDisabled()
        );

        KTable<String, Customer> customers = builder.table(
                Topics.CUSTOMERS,
                Consumed.with(stringSerde, customerSerde).withName("customers-source"),
                Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as(Topics.CUSTOMERS_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(customerSerde)
                        .withCachingDisabled()
        );

        // Join the *tables*, not the stream.
        //
        // A card's customer changes almost never; transactions arrive constantly. Doing
        // this join on the table side pays the cost once per card change. Carrying the
        // customer onto each transaction instead would mean a foreign-key lookup on every
        // authorisation, inside the latency budget that this whole project exists to
        // protect.
        KTable<String, CardProfile> cardProfiles = cards.join(
                customers,
                Card::getCustomerId,
                EnrichmentTopology::toCardProfile,
                TableJoined.as("card-customer-fk"),
                Materialized.<String, CardProfile, KeyValueStore<Bytes, byte[]>>as(Topics.CARD_PROFILE_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(cardProfileSerde)
                        .withCachingDisabled()
        );

        // Merchants are a GlobalKTable rather than a KTable.
        //
        // A KStream-KTable join requires co-partitioning: the stream must already be keyed
        // by the table's key. This stream is keyed by cardId, so joining a partitioned
        // merchant table would force a rekey to merchantId and a repartition topic - a
        // broker round trip added to every authorisation, just to look up a category code.
        //
        // A GlobalKTable is replicated in full to every instance and can be joined by any
        // key extractor with no repartition. The costs are memory and the fact that global
        // tables are not time-synchronised with the stream. Both are fine here: the
        // merchant table is small and near-static.
        GlobalKTable<String, Merchant> merchants = builder.globalTable(
                Topics.MERCHANTS,
                Consumed.with(stringSerde, merchantSerde).withName("merchants-source"),
                Materialized.<String, Merchant, KeyValueStore<Bytes, byte[]>>as(Topics.MERCHANT_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(merchantSerde)
        );

        // --- The stream -----------------------------------------------------------------

        KStream<String, Transaction> transactions = builder.stream(
                Topics.TRANSACTIONS,
                Consumed.with(stringSerde, transactionSerde)
                        .withTimestampExtractor(new TransactionTimestampExtractor())
                        .withName("transactions-source")
        );

        // leftJoin: a transaction for a card we have never seen still goes through, with a
        // null profile.
        //
        // Stage 1 used an inner join, which silently dropped these. For a risk engine that
        // is backwards: an authorisation on a card the issuer has no record of is itself a
        // signal. The decision path turns a null profile into a REVIEW with an UNKNOWN_CARD
        // reason. It is also the safety net for the race documented in docs/NOTES.md, where
        // a transaction arrives before its card's profile has been built.
        KStream<String, EnrichedTransaction> enriched = transactions
                .leftJoin(
                        cardProfiles,
                        (transaction, profile) -> EnrichedTransaction.newBuilder()
                                .setTransaction(transaction)
                                .setCardProfile(profile)
                                .build(),
                        Joined.with(stringSerde, transactionSerde, cardProfileSerde)
                                .withName("transaction-card-join")
                )
                .leftJoin(
                        merchants,
                        (cardId, enrichedTransaction) -> enrichedTransaction.getTransaction().getMerchantId(),
                        // leftJoin, so a merchant missing from the global table yields null
                        // rather than dropping the authorisation. The rules downstream have
                        // to cope with a null merchant instead of assuming it is populated.
                        (enrichedTransaction, merchant) -> EnrichedTransaction.newBuilder(enrichedTransaction)
                                .setMerchant(merchant)
                                .build(),
                        Named.as("transaction-merchant-join")
                );

        enriched.to(Topics.ENRICHED, Produced.with(stringSerde, enrichedSerde).withName("enriched-sink"));

        return enriched;
    }

    private static CardProfile toCardProfile(Card card, Customer customer) {
        return CardProfile.newBuilder()
                .setCardId(card.getCardId())
                .setCustomerId(card.getCustomerId())
                .setStatus(card.getStatus())
                .setDailyLimitMinor(card.getDailyLimitMinor())
                .setCurrency(card.getCurrency())
                .setRiskTier(customer.getRiskTier())
                .setHomeCountry(customer.getHomeCountry())
                .setHomeLat(customer.getHomeLat())
                .setHomeLon(customer.getHomeLon())
                .build();
    }
}
