package com.kaushal.riskengine.decision;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.CardStatus;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.DailySpend;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.DecisionType;
import com.kaushal.riskengine.avro.LastSeen;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.RiskTier;
import com.kaushal.riskengine.avro.RuleHit;
import com.kaushal.riskengine.avro.Transaction;
import com.kaushal.riskengine.config.AvroSerdes;
import com.kaushal.riskengine.config.RiskEngineProperties;
import com.kaushal.riskengine.topology.DecisionTopology;
import com.kaushal.riskengine.topology.EnrichmentTopology;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The decision path end to end - enrichment, the RiskEvaluator and its three stores - driven
 * through {@link TopologyTestDriver}. One nested class per rule.
 *
 * <p>Time is fully controlled. Event time comes from each transaction's {@code eventTime}
 * (via the timestamp extractor), and wall-clock time - which drives the eviction punctuator -
 * is advanced by hand. Nothing here sleeps.
 */
class DecisionTopologyTest {

    private static final String SCOPE = "decision-test";
    private static final Instant T0 = Instant.parse("2026-09-18T10:00:00Z");

    // Merchants. The three Berlin ones are 1-2 km apart.
    private static final Merchant BERLIN = merchant("M-BERLIN", "DE", 52.5200, 13.4050, "5411");
    private static final Merchant BERLIN_2 = merchant("M-BERLIN-2", "DE", 52.5300, 13.4200, "5812");
    private static final Merchant BERLIN_3 = merchant("M-BERLIN-3", "DE", 52.5100, 13.3900, "5999");
    private static final Merchant CASINO = merchant("M-CASINO", "DE", 52.5050, 13.3800, "7995");
    private static final Merchant SAO_PAULO = merchant("M-SAOPAULO", "BR", -23.5505, -46.6333, "5411");

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, Transaction> transactions;
    private TestOutputTopic<String, Decision> decisions;

    @BeforeEach
    void setUp() {
        AvroSerdes serdes = new AvroSerdes(new RiskEngineProperties(
                "decision-test", "dummy:9092", "mock://" + SCOPE, stateDir.toString(),
                "localhost:8088", 0, "at_least_once"));

        StreamsBuilder builder = new StreamsBuilder();
        new DecisionTopology(serdes).decisionStream(new EnrichmentTopology(serdes).enrichmentStream(builder));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "decision-test-" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        // The wall clock starts at T0 and only moves when a test moves it.
        driver = new TopologyTestDriver(builder.build(), config, T0);

        var customers = driver.createInputTopic(Topics.CUSTOMERS, Serdes.String().serializer(), serdes.<Customer>value().serializer());
        var cards = driver.createInputTopic(Topics.CARDS, Serdes.String().serializer(), serdes.<Card>value().serializer());
        var merchants = driver.createInputTopic(Topics.MERCHANTS, Serdes.String().serializer(), serdes.<Merchant>value().serializer());
        transactions = driver.createInputTopic(Topics.TRANSACTIONS, Serdes.String().serializer(), serdes.<Transaction>value().serializer());
        decisions = driver.createOutputTopic(Topics.DECISIONS, Serdes.String().deserializer(), serdes.<Decision>value().deserializer());

        customers.pipeInput("CUST-LOW", customer("CUST-LOW", RiskTier.LOW));
        customers.pipeInput("CUST-HIGH", customer("CUST-HIGH", RiskTier.HIGH));
        // Daily limit EUR 1,000.
        cards.pipeInput("CARD-1", card("CARD-1", "CUST-LOW", CardStatus.ACTIVE));
        cards.pipeInput("CARD-HIGH", card("CARD-HIGH", "CUST-HIGH", CardStatus.ACTIVE));
        cards.pipeInput("CARD-BLOCKED", card("CARD-BLOCKED", "CUST-LOW", CardStatus.BLOCKED));
        for (Merchant m : List.of(BERLIN, BERLIN_2, BERLIN_3, CASINO, SAO_PAULO)) {
            merchants.pipeInput(m.getMerchantId(), m);
        }
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @Test
    @DisplayName("an ordinary purchase is approved with no reasons")
    void approvesOrdinaryPurchase() {
        Decision decision = send("CARD-1", BERLIN, 2_500, T0);

        assertThat(decision.getDecision()).isEqualTo(DecisionType.APPROVE);
        assertThat(decision.getScore()).isZero();
        assertThat(decision.getReasons()).isEmpty();
    }

    @Nested
    @DisplayName("card state")
    class CardState {

        @Test
        @DisplayName("an unknown card is sent to REVIEW, not dropped")
        void unknownCardIsReviewed() {
            Decision decision = send("CARD-NEVER-SEEN", BERLIN, 2_500, T0);

            assertThat(decision.getDecision()).isEqualTo(DecisionType.REVIEW);
            assertThat(rules(decision)).containsExactly("UNKNOWN_CARD");
        }

        @Test
        @DisplayName("a blocked card is always declined")
        void blockedCardIsDeclined() {
            Decision decision = send("CARD-BLOCKED", BERLIN, 100, T0);

            assertThat(decision.getDecision()).isEqualTo(DecisionType.DECLINE);
            assertThat(rules(decision)).containsExactly("CARD_NOT_ACTIVE");
        }
    }

    @Nested
    @DisplayName("velocity")
    class Velocity {

        @Test
        @DisplayName("the 6th attempt inside 60s is flagged, and the window slides")
        void sixthAttemptInAMinuteIsFlagged() {
            for (int i = 0; i < 5; i++) {
                assertThat(send("CARD-1", BERLIN, 1_000, T0.plusSeconds(i * 5L)).getDecision())
                        .isEqualTo(DecisionType.APPROVE);
            }

            Decision sixth = send("CARD-1", BERLIN, 1_000, T0.plusSeconds(25));
            assertThat(sixth.getDecision()).isEqualTo(DecisionType.REVIEW);
            assertThat(rules(sixth)).containsExactly("VELOCITY");

            // Two minutes later the burst has left the window.
            Decision later = send("CARD-1", BERLIN, 1_000, T0.plusSeconds(145));
            assertThat(later.getDecision()).isEqualTo(DecisionType.APPROVE);
        }
    }

    @Nested
    @DisplayName("card testing")
    class CardTesting {

        @Test
        @DisplayName("4 tiny amounts across 3 merchants is flagged for review")
        void tinyAmountsAcrossMerchantsAreReviewed() {
            send("CARD-1", BERLIN, 50, T0);
            send("CARD-1", BERLIN_2, 50, T0.plusSeconds(30));
            Decision third = send("CARD-1", BERLIN_3, 50, T0.plusSeconds(60));
            assertThat(third.getDecision()).isEqualTo(DecisionType.APPROVE);

            Decision fourth = send("CARD-1", BERLIN, 50, T0.plusSeconds(90));
            assertThat(fourth.getDecision()).isEqualTo(DecisionType.REVIEW);
            assertThat(rules(fourth)).containsExactly("CARD_TESTING");
        }

        @Test
        @DisplayName("card testing at bot speed also trips velocity, and is declined")
        void cardTestingAtBotSpeedIsDeclined() {
            List<Merchant> rotation = List.of(BERLIN, BERLIN_2, BERLIN_3);
            Decision last = null;
            for (int i = 0; i < 6; i++) {
                last = send("CARD-1", rotation.get(i % 3), 50, T0.plusSeconds(i * 10L));
            }

            assertThat(last.getDecision()).isEqualTo(DecisionType.DECLINE);
            assertThat(rules(last)).containsExactlyInAnyOrder("VELOCITY", "CARD_TESTING");
        }
    }

    @Nested
    @DisplayName("daily limit")
    class DailyLimit {

        @Test
        @DisplayName("the transaction that crosses the limit is declined, and does not count toward it")
        void declinedAmountIsNotCounted() {
            // Ten minutes apart, so velocity stays out of it.
            assertThat(send("CARD-1", BERLIN, 40_000, T0).getDecision()).isEqualTo(DecisionType.APPROVE);
            assertThat(send("CARD-1", BERLIN, 40_000, T0.plus(Duration.ofMinutes(10))).getDecision())
                    .isEqualTo(DecisionType.APPROVE);

            // EUR 800 spent + EUR 300 > EUR 1,000.
            Decision over = send("CARD-1", BERLIN, 30_000, T0.plus(Duration.ofMinutes(20)));
            assertThat(over.getDecision()).isEqualTo(DecisionType.DECLINE);
            assertThat(rules(over)).containsExactly("DAILY_LIMIT");

            // The declined EUR 300 never moved, so EUR 800 + EUR 150 is still inside the limit.
            Decision after = send("CARD-1", BERLIN, 15_000, T0.plus(Duration.ofMinutes(30)));
            assertThat(after.getDecision()).isEqualTo(DecisionType.APPROVE);
        }

        @Test
        @DisplayName("the limit resets on a new day")
        void resetsOnNewDay() {
            send("CARD-1", BERLIN, 90_000, T0);

            Decision nextDay = send("CARD-1", BERLIN, 90_000, T0.plus(Duration.ofDays(1)));
            assertThat(nextDay.getDecision()).isEqualTo(DecisionType.APPROVE);
        }
    }

    @Nested
    @DisplayName("impossible travel")
    class ImpossibleTravel {

        @Test
        @DisplayName("Berlin then Sao Paulo four minutes later is declined")
        void berlinThenSaoPauloIsDeclined() {
            send("CARD-1", BERLIN, 3_000, T0);

            Decision saoPaulo = send("CARD-1", SAO_PAULO, 8_900, T0.plus(Duration.ofMinutes(4)));

            assertThat(saoPaulo.getDecision()).isEqualTo(DecisionType.DECLINE);
            assertThat(rules(saoPaulo)).containsExactly("GEO_VELOCITY");
            assertThat(saoPaulo.getReasons().get(0).getDetail()).contains("DE to BR").contains("km/h");
        }

        @Test
        @DisplayName("a declined location does not move the card: the cardholder's next purchase at home is fine")
        void declinedLocationDoesNotMoveTheCard() {
            send("CARD-1", BERLIN, 3_000, T0);
            send("CARD-1", SAO_PAULO, 8_900, T0.plus(Duration.ofMinutes(4)));

            // If the fraudulent Sao Paulo charge had updated the location, this genuine
            // purchase back in Berlin would itself look like impossible travel.
            Decision home = send("CARD-1", BERLIN, 1_200, T0.plus(Duration.ofMinutes(10)));

            assertThat(home.getDecision()).isEqualTo(DecisionType.APPROVE);
            assertThat(home.getReasons()).isEmpty();
        }

        @Test
        @DisplayName("nearby merchants seconds apart are not flagged")
        void nearbyMerchantsAreFine() {
            send("CARD-1", BERLIN, 1_000, T0);

            // ~1.5 km in 10 seconds is 540 km/h - under the minimum distance, so ignored.
            Decision nextDoor = send("CARD-1", BERLIN_2, 1_000, T0.plusSeconds(10));

            assertThat(nextDoor.getReasons()).isEmpty();
        }

        @Test
        @DisplayName("a real flight is not impossible travel")
        void realFlightIsFine() {
            send("CARD-1", BERLIN, 1_000, T0);

            // ~10,000 km in 14 hours is ~715 km/h.
            Decision landed = send("CARD-1", SAO_PAULO, 1_000, T0.plus(Duration.ofHours(14)));

            assertThat(landed.getDecision()).isEqualTo(DecisionType.APPROVE);
        }
    }

    @Nested
    @DisplayName("merchant risk and customer tier")
    class MerchantAndTier {

        @Test
        @DisplayName("a high-risk merchant adds score but does not block on its own")
        void highRiskMerchant() {
            Decision decision = send("CARD-1", CASINO, 5_000, T0);

            assertThat(decision.getDecision()).isEqualTo(DecisionType.APPROVE);
            assertThat(rules(decision)).containsExactly("MERCHANT_RISK");
        }

        @Test
        @DisplayName("HIGH tier escalates a decision that another rule started, and never starts one")
        void tierOnlyEscalates() {
            Decision ordinary = send("CARD-HIGH", BERLIN, 5_000, T0);
            assertThat(ordinary.getReasons()).isEmpty();

            Decision casino = send("CARD-HIGH", CASINO, 5_000, T0.plus(Duration.ofMinutes(10)));
            assertThat(rules(casino)).containsExactlyInAnyOrder("MERCHANT_RISK", "CUSTOMER_RISK_TIER");
            assertThat(casino.getScore()).isEqualTo(30);
        }
    }

    @Nested
    @DisplayName("bounded state")
    class BoundedState {

        @Test
        @DisplayName("the wall-clock punctuator evicts locations older than 7 days, and nothing younger")
        void punctuatorEvictsStaleState() {
            send("CARD-1", BERLIN, 1_000, T0);
            KeyValueStore<String, LastSeen> geo = driver.getKeyValueStore(Topics.GEO_STORE);
            KeyValueStore<String, DailySpend> spend = driver.getKeyValueStore(Topics.SPEND_STORE);
            assertThat(geo.get("CARD-1")).isNotNull();

            // The punctuator fires, but a two-hour-old location is still relevant.
            driver.advanceWallClockTime(Duration.ofHours(2));
            assertThat(geo.get("CARD-1")).isNotNull();
            assertThat(spend.get("CARD-1")).isNotNull();

            // Eight days on, both entries are stale and go.
            driver.advanceWallClockTime(Duration.ofDays(8));
            assertThat(geo.get("CARD-1")).isNull();
            assertThat(spend.get("CARD-1")).isNull();
        }
    }

    // --- helpers --------------------------------------------------------------------------

    private Decision send(String cardId, Merchant at, long amountMinor, Instant eventTime) {
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
        List<Decision> out = decisions.readValuesToList();
        assertThat(out).as("exactly one decision per transaction").hasSize(1);
        return out.get(0);
    }

    private static List<String> rules(Decision decision) {
        return decision.getReasons().stream().map(RuleHit::getRule).toList();
    }

    private static Customer customer(String id, RiskTier tier) {
        return Customer.newBuilder()
                .setCustomerId(id)
                .setRiskTier(tier)
                .setHomeCountry("DE")
                .setHomeLat(52.52)
                .setHomeLon(13.405)
                .build();
    }

    private static Card card(String id, String customerId, CardStatus status) {
        return Card.newBuilder()
                .setCardId(id)
                .setCustomerId(customerId)
                .setStatus(status)
                .setDailyLimitMinor(100_000L)
                .setCurrency("EUR")
                .build();
    }

    private static Merchant merchant(String id, String country, double lat, double lon, String mcc) {
        return Merchant.newBuilder()
                .setMerchantId(id)
                .setName(id)
                .setMcc(mcc)
                .setCountry(country)
                .setLat(lat)
                .setLon(lon)
                .build();
    }
}
