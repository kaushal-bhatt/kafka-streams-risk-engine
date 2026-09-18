package com.kaushal.riskengine.query;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.CardStatus;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.RiskTier;
import com.kaushal.riskengine.avro.Transaction;
import com.kaushal.riskengine.config.AvroSerdes;
import com.kaushal.riskengine.config.RiskEngineProperties;
import com.kaushal.riskengine.topology.DecisionTopology;
import com.kaushal.riskengine.topology.EnrichmentTopology;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives real transactions through the full topology, then reads the result back out of the
 * same five stores Interactive Queries will read in production.
 */
class CardRiskAssemblerTest {

    private static final String SCOPE = "assembler-test";
    private static final Instant T0 = Instant.parse("2026-09-18T10:00:00Z");
    private static final Merchant BERLIN = merchant("M-BERLIN", "DE", 52.52, 13.405);
    private static final Merchant SAO_PAULO = merchant("M-SAOPAULO", "BR", -23.5505, -46.6333);

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, Transaction> transactions;

    @BeforeEach
    void setUp() {
        AvroSerdes serdes = new AvroSerdes(new RiskEngineProperties(
                "assembler-test", "dummy:9092", "mock://" + SCOPE, stateDir.toString(),
                "localhost:8088", 0, "at_least_once"));
        StreamsBuilder builder = new StreamsBuilder();
        new DecisionTopology(serdes, new SimpleMeterRegistry())
                .decisionStream(new EnrichmentTopology(serdes).enrichmentStream(builder));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "assembler-test-" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(builder.build(), config, T0);

        driver.createInputTopic(Topics.CUSTOMERS, Serdes.String().serializer(), serdes.<Customer>value().serializer())
                .pipeInput("CUST-1", Customer.newBuilder().setCustomerId("CUST-1").setRiskTier(RiskTier.LOW)
                        .setHomeCountry("DE").setHomeLat(52.52).setHomeLon(13.405).build());
        var cards = driver.createInputTopic(Topics.CARDS, Serdes.String().serializer(), serdes.<Card>value().serializer());
        cards.pipeInput("CARD-1", card("CARD-1"));
        cards.pipeInput("CARD-QUIET", card("CARD-QUIET"));
        var merchants = driver.createInputTopic(Topics.MERCHANTS, Serdes.String().serializer(), serdes.<Merchant>value().serializer());
        merchants.pipeInput(BERLIN.getMerchantId(), BERLIN);
        merchants.pipeInput(SAO_PAULO.getMerchantId(), SAO_PAULO);

        transactions = driver.createInputTopic(Topics.TRANSACTIONS, Serdes.String().serializer(),
                serdes.<Transaction>value().serializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @Test
    @DisplayName("the view reflects what the decision path did - including what it refused to record")
    void assemblesFromAllFiveStores() {
        send("CARD-1", BERLIN, 40_000, T0);
        send("CARD-1", BERLIN, 10_000, T0.plusSeconds(30));
        // Declined as impossible travel.
        Instant saoPaulo = T0.plus(Duration.ofMinutes(4));
        send("CARD-1", SAO_PAULO, 8_900, saoPaulo);

        CardRiskView view = assemble("CARD-1", saoPaulo).orElseThrow();

        assertThat(view.servedBy()).isEqualTo("localhost:8088");
        assertThat(view.routedVia()).isNull();
        assertThat(view.profile().dailyLimitMinor()).isEqualTo(100_000);
        assertThat(view.profile().riskTier()).isEqualTo("LOW");

        // The declined EUR 89 never moved, so it isn't in today's spend...
        assertThat(view.spendToday().spentMinor()).isEqualTo(50_000);
        assertThat(view.spendToday().remainingMinor()).isEqualTo(50_000);
        // ...and it didn't move the card: last seen is still Germany.
        assertThat(view.lastSeen().place()).isEqualTo("DE");

        // But every attempt counts for velocity, declined or not.
        assertThat(view.recentActivity().attemptsLast10m()).isEqualTo(3);
        assertThat(view.recentActivity().attemptsLast60s()).isEqualTo(1);
        assertThat(view.recentActivity().lastAttemptAt()).isEqualTo(saoPaulo);

        assertThat(view.lastDecision().decision()).isEqualTo("DECLINE");
        assertThat(view.lastDecision().reasons()).extracting(CardRiskView.Reason::rule).containsExactly("GEO_VELOCITY");
    }

    @Test
    @DisplayName("a known card with no activity shows its profile and full remaining limit")
    void quietCard() {
        CardRiskView view = assemble("CARD-QUIET", T0).orElseThrow();

        assertThat(view.profile()).isNotNull();
        assertThat(view.spendToday().spentMinor()).isZero();
        assertThat(view.spendToday().remainingMinor()).isEqualTo(100_000);
        assertThat(view.lastSeen()).isNull();
        assertThat(view.lastDecision()).isNull();
        assertThat(view.recentActivity().attemptsLast10m()).isZero();
    }

    @Test
    @DisplayName("a card the engine has never seen is not found")
    void unknownCard() {
        assertThat(assemble("CARD-NOBODY", T0)).isEmpty();
    }

    @Test
    @DisplayName("yesterday's spend is not today's")
    void spendResetsDaily() {
        send("CARD-1", BERLIN, 40_000, T0);

        CardRiskView tomorrow = assemble("CARD-1", T0.plus(Duration.ofDays(1))).orElseThrow();

        assertThat(tomorrow.spendToday().spentMinor()).isZero();
        assertThat(tomorrow.spendToday().remainingMinor()).isEqualTo(100_000);
    }

    private Optional<CardRiskView> assemble(String cardId, Instant now) {
        CardStores stores = new CardStores(
                driver.getKeyValueStore(Topics.CARD_PROFILE_STORE),
                driver.getKeyValueStore(Topics.SPEND_STORE),
                driver.getKeyValueStore(Topics.GEO_STORE),
                driver.getWindowStore(Topics.VELOCITY_STORE),
                driver.getKeyValueStore(Topics.LAST_DECISION_STORE));
        return CardRiskAssembler.assemble(cardId, 0, "localhost:8088", now, stores);
    }

    private void send(String cardId, Merchant at, long amountMinor, Instant eventTime) {
        transactions.pipeInput(cardId, Transaction.newBuilder()
                .setTransactionId(UUID.randomUUID().toString())
                .setCardId(cardId)
                .setMerchantId(at.getMerchantId())
                .setAmountMinor(amountMinor)
                .setCurrency("EUR")
                .setEventTime(eventTime)
                .setLat(at.getLat())
                .setLon(at.getLon())
                .build());
    }

    private static Card card(String id) {
        return Card.newBuilder().setCardId(id).setCustomerId("CUST-1").setStatus(CardStatus.ACTIVE)
                .setDailyLimitMinor(100_000L).setCurrency("EUR").build();
    }

    private static Merchant merchant(String id, String country, double lat, double lon) {
        return Merchant.newBuilder().setMerchantId(id).setName(id).setMcc("5411").setCountry(country)
                .setLat(lat).setLon(lon).build();
    }
}
