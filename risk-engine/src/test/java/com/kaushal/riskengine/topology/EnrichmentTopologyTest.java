package com.kaushal.riskengine.topology;

import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.CardStatus;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.EnrichedTransaction;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.RiskTier;
import com.kaushal.riskengine.avro.Transaction;
import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.config.AvroSerdes;
import com.kaushal.riskengine.config.RiskEngineProperties;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stage 1 enrichment, driven through {@link TopologyTestDriver}.
 *
 * <p>No broker, no Schema Registry, no Docker - the registry URL is a {@code mock://}
 * scope that the Confluent client resolves in-process. Each test runs in single-digit
 * milliseconds, which is what makes it realistic to have one per rule later on.
 */
class EnrichmentTopologyTest {

    private static final String SCOPE = "enrichment-test";
    private static final String REGISTRY_URL = "mock://" + SCOPE;

    private TopologyTestDriver driver;

    private TestInputTopic<String, Card> cards;
    private TestInputTopic<String, Customer> customers;
    private TestInputTopic<String, Merchant> merchants;
    private TestInputTopic<String, Transaction> transactions;
    private TestOutputTopic<String, EnrichedTransaction> enriched;

    @BeforeEach
    void setUp() {
        RiskEngineProperties properties = new RiskEngineProperties(
                "enrichment-test", "dummy:9092", REGISTRY_URL, "build/test-state",
                "localhost:8080", 0, "at_least_once"
        );
        AvroSerdes avroSerdes = new AvroSerdes(properties);

        StreamsBuilder builder = new StreamsBuilder();
        new EnrichmentTopology(avroSerdes).enrichmentStream(builder);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrichment-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), config);

        SpecificAvroSerde<Card> cardSerde = avroSerdes.value();
        SpecificAvroSerde<Customer> customerSerde = avroSerdes.value();
        SpecificAvroSerde<Merchant> merchantSerde = avroSerdes.value();
        SpecificAvroSerde<Transaction> transactionSerde = avroSerdes.value();
        SpecificAvroSerde<EnrichedTransaction> enrichedSerde = avroSerdes.value();

        cards = driver.createInputTopic(Topics.CARDS, Serdes.String().serializer(), cardSerde.serializer());
        customers = driver.createInputTopic(Topics.CUSTOMERS, Serdes.String().serializer(), customerSerde.serializer());
        merchants = driver.createInputTopic(Topics.MERCHANTS, Serdes.String().serializer(), merchantSerde.serializer());
        transactions = driver.createInputTopic(Topics.TRANSACTIONS, Serdes.String().serializer(), transactionSerde.serializer());
        enriched = driver.createOutputTopic(Topics.ENRICHED, Serdes.String().deserializer(), enrichedSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        // The mock registry is a static map keyed by scope. Without this, schema ids leak
        // between tests and the second test in a class fails for no visible reason.
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @Test
    @DisplayName("a transaction is enriched with its card limit, the customer risk tier, and the merchant")
    void enrichesTransaction() {
        givenReferenceData();

        transactions.pipeInput("CARD-1", transaction("TXN-1", "CARD-1", "MERCH-1", 4_250L));

        assertThat(enriched.getQueueSize()).isEqualTo(1);
        EnrichedTransaction result = enriched.readValue();

        // From the card
        assertThat(result.getCardProfile().getDailyLimitMinor()).isEqualTo(150_000L);
        assertThat(result.getCardProfile().getStatus()).isEqualTo(CardStatus.ACTIVE);
        // From the customer, carried across the foreign-key table join
        assertThat(result.getCardProfile().getRiskTier()).isEqualTo(RiskTier.HIGH);
        assertThat(result.getCardProfile().getHomeCountry()).isEqualTo("DE");
        // From the merchant GlobalKTable
        assertThat(result.getMerchant()).isNotNull();
        assertThat(result.getMerchant().getMcc()).isEqualTo("7995");
        // And the transaction itself is intact
        assertThat(result.getTransaction().getAmountMinor()).isEqualTo(4_250L);
    }

    @Test
    @DisplayName("a missing merchant still produces an enriched transaction, with a null merchant")
    void toleratesUnknownMerchant() {
        givenReferenceData();

        transactions.pipeInput("CARD-1", transaction("TXN-2", "CARD-1", "MERCH-UNKNOWN", 999L));

        EnrichedTransaction result = enriched.readValue();
        assertThat(result.getMerchant()).isNull();
        assertThat(result.getCardProfile()).isNotNull();
    }

    @Test
    @DisplayName("a transaction for an unknown card is currently dropped - see TODO(stage-2)")
    void dropsUnknownCard() {
        givenReferenceData();

        transactions.pipeInput("CARD-NOT-SEEN", transaction("TXN-3", "CARD-NOT-SEEN", "MERCH-1", 1_000L));

        // Pinning the behaviour rather than endorsing it. Stage 2 must turn this into a
        // REVIEW decision with an UNKNOWN_CARD reason; when it does, this test should fail
        // and be rewritten.
        assertThat(enriched.isEmpty()).isTrue();
    }

    private void givenReferenceData() {
        customers.pipeInput("CUST-1", Customer.newBuilder()
                .setCustomerId("CUST-1")
                .setRiskTier(RiskTier.HIGH)
                .setHomeCountry("DE")
                .setHomeLat(52.52)
                .setHomeLon(13.405)
                .build());

        cards.pipeInput("CARD-1", Card.newBuilder()
                .setCardId("CARD-1")
                .setCustomerId("CUST-1")
                .setStatus(CardStatus.ACTIVE)
                .setDailyLimitMinor(150_000L)
                .setCurrency("EUR")
                .build());

        merchants.pipeInput("MERCH-1", Merchant.newBuilder()
                .setMerchantId("MERCH-1")
                .setName("Casino Royale")
                .setMcc("7995")
                .setCountry("DE")
                .setLat(52.50)
                .setLon(13.38)
                .build());
    }

    private static Transaction transaction(String id, String cardId, String merchantId, long amountMinor) {
        return Transaction.newBuilder()
                .setTransactionId(id)
                .setCardId(cardId)
                .setMerchantId(merchantId)
                .setAmountMinor(amountMinor)
                .setCurrency("EUR")
                .setEventTime(Instant.parse("2026-09-18T10:15:30Z"))
                .setLat(52.52)
                .setLon(13.405)
                .build();
    }
}
