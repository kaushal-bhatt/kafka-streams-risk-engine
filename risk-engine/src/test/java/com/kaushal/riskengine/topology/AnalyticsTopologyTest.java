package com.kaushal.riskengine.topology;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.CardStatus;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.DecisionCounts;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.MerchantStats;
import com.kaushal.riskengine.avro.RiskTier;
import com.kaushal.riskengine.avro.Transaction;
import com.kaushal.riskengine.config.AvroSerdes;
import com.kaushal.riskengine.config.RiskEngineProperties;
import com.kaushal.riskengine.query.MerchantStatsAssembler;
import com.kaushal.riskengine.query.MerchantStatsView;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * The analytics path, through the full topology: transactions are decided, rekeyed by merchant,
 * counted per 5-minute window, and published once each window closes.
 *
 * <p>Stream time drives everything here: a window closes when a record with a later event time
 * arrives, not when the wall clock moves. The tests advance it by sending a transaction at a
 * different merchant.
 */
class AnalyticsTopologyTest {

    private static final String SCOPE = "analytics-test";
    /** Aligned to a window boundary: windows run 10:00-10:05, 10:05-10:10, ... */
    private static final Instant T0 = Instant.parse("2026-09-18T10:00:00Z");
    private static final Merchant SHOP = merchant("M-SHOP", 52.520, 13.405);
    private static final Merchant OTHER = merchant("M-OTHER", 52.530, 13.420);

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, Transaction> transactions;
    private TestOutputTopic<String, MerchantStats> merchantStats;

    @BeforeEach
    void setUp() {
        AvroSerdes serdes = new AvroSerdes(new RiskEngineProperties(
                "analytics-test", "dummy:9092", "mock://" + SCOPE, stateDir.toString(), "localhost:8088", 0, "at_least_once"));

        StreamsBuilder builder = new StreamsBuilder();
        var decisions = new DecisionTopology(serdes, new SimpleMeterRegistry())
                .decisionStream(new EnrichmentTopology(serdes).enrichmentStream(builder));
        new AnalyticsTopology(serdes).merchantStatsStream(decisions);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "analytics-test-" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(builder.build(), config, T0);

        driver.createInputTopic(Topics.CUSTOMERS, Serdes.String().serializer(), serdes.<Customer>value().serializer())
                .pipeInput("CUST-1", Customer.newBuilder().setCustomerId("CUST-1").setRiskTier(RiskTier.LOW)
                        .setHomeCountry("DE").setHomeLat(52.52).setHomeLon(13.405).build());
        var cards = driver.createInputTopic(Topics.CARDS, Serdes.String().serializer(), serdes.<Card>value().serializer());
        cards.pipeInput("CARD-1", card("CARD-1", CardStatus.ACTIVE));
        cards.pipeInput("CARD-2", card("CARD-2", CardStatus.ACTIVE));
        // A blocked card is always declined: a deterministic way to produce a DECLINE.
        cards.pipeInput("CARD-BLOCKED", card("CARD-BLOCKED", CardStatus.BLOCKED));
        var merchants = driver.createInputTopic(Topics.MERCHANTS, Serdes.String().serializer(), serdes.<Merchant>value().serializer());
        merchants.pipeInput(SHOP.getMerchantId(), SHOP);
        merchants.pipeInput(OTHER.getMerchantId(), OTHER);

        transactions = driver.createInputTopic(Topics.TRANSACTIONS, Serdes.String().serializer(), serdes.<Transaction>value().serializer());
        merchantStats = driver.createOutputTopic(Topics.MERCHANT_STATS, Serdes.String().deserializer(), serdes.<MerchantStats>value().deserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @Test
    @DisplayName("nothing is published while a window is open; exactly one final result once it closes")
    void oneFinalResultPerWindow() {
        send("CARD-1", SHOP, T0.plus(Duration.ofMinutes(1)));
        send("CARD-BLOCKED", SHOP, T0.plus(Duration.ofMinutes(2)));
        send("CARD-2", SHOP, T0.plus(Duration.ofMinutes(3)));

        assertThat(merchantStats.isEmpty()).as("window still open: suppressed").isTrue();

        // Stream time moves past the window's end plus grace (10:05:30).
        send("CARD-2", OTHER, T0.plus(Duration.ofMinutes(5)).plusSeconds(31));

        List<MerchantStats> out = merchantStats.readValuesToList();
        assertThat(out).hasSize(1);
        MerchantStats shop = out.get(0);
        assertThat(shop.getMerchantId()).isEqualTo("M-SHOP");
        assertThat(shop.getTotal()).isEqualTo(3);
        assertThat(shop.getDeclined()).isEqualTo(1);
        assertThat(shop.getDeclineRate()).isCloseTo(1.0 / 3, within(1e-9));
        assertThat(shop.getWindowStart()).isEqualTo(T0);
        assertThat(shop.getWindowEnd()).isEqualTo(T0.plus(Duration.ofMinutes(5)));
    }

    @Test
    @DisplayName("a decision up to 30 s late still counts; one later than that is dropped")
    void gracePeriod() {
        send("CARD-1", SHOP, T0.plus(Duration.ofMinutes(1)));
        // Stream time 10:05:10 - past the window's end, but inside its 30 s grace.
        send("CARD-2", OTHER, T0.plus(Duration.ofMinutes(5)).plusSeconds(10));

        // Late, but within grace: counted.
        send("CARD-BLOCKED", SHOP, T0.plus(Duration.ofMinutes(4)));

        // Stream time 10:06 closes the window.
        send("CARD-2", OTHER, T0.plus(Duration.ofMinutes(6)));
        List<MerchantStats> closed = merchantStats.readValuesToList();
        assertThat(closed).hasSize(1);
        assertThat(closed.get(0).getTotal()).isEqualTo(2);
        assertThat(closed.get(0).getDeclined()).isEqualTo(1);

        // Too late: the window closed at 10:05:30. Dropped - no second result, no change.
        send("CARD-1", SHOP, T0.plus(Duration.ofMinutes(2)));
        send("CARD-2", OTHER, T0.plus(Duration.ofMinutes(11)));
        assertThat(merchantStats.readValuesToList())
                .noneMatch(s -> s.getMerchantId().equals("M-SHOP") && s.getWindowStart().equals(T0));
        ReadOnlyWindowStore<String, DecisionCounts> store = driver.getWindowStore(Topics.MERCHANT_STATS_STORE);
        assertThat(store.fetch("M-SHOP", T0.toEpochMilli()).getTotal()).isEqualTo(2);
    }

    @Test
    @DisplayName("the query API sees an open window's live counts - which the topic never gets")
    void queryShowsLiveCounts() {
        send("CARD-1", SHOP, T0.plus(Duration.ofMinutes(1)));
        send("CARD-BLOCKED", SHOP, T0.plus(Duration.ofMinutes(2)));
        ReadOnlyWindowStore<String, DecisionCounts> store = driver.getWindowStore(Topics.MERCHANT_STATS_STORE);

        MerchantStatsView open = MerchantStatsAssembler.assemble(
                "M-SHOP", 0, "localhost:8088", T0.plus(Duration.ofMinutes(3)), store).orElseThrow();
        assertThat(open.windows()).hasSize(1);
        assertThat(open.windows().get(0).total()).isEqualTo(2);
        assertThat(open.windows().get(0).closed()).isFalse();
        assertThat(merchantStats.isEmpty()).isTrue();

        MerchantStatsView later = MerchantStatsAssembler.assemble(
                "M-SHOP", 0, "localhost:8088", T0.plus(Duration.ofMinutes(6)), store).orElseThrow();
        assertThat(later.windows().get(0).closed()).isTrue();

        assertThat(MerchantStatsAssembler.assemble("M-NOBODY", 0, "x", T0, store)).isEmpty();
    }

    @Test
    @DisplayName("the query finds a merchant's windows even when the wall clock is far from their event time")
    void queryIgnoresWallClockDistance() {
        // One decision per window for 14 consecutive windows.
        for (int i = 0; i < 14; i++) {
            send("CARD-1", SHOP, T0.plus(Duration.ofMinutes(5L * i + 1)));
        }
        ReadOnlyWindowStore<String, DecisionCounts> store = driver.getWindowStore(Topics.MERCHANT_STATS_STORE);

        // Asked eight days later - as after a fast replay, or a quiet merchant. A wall-clock
        // "last hour" range would find nothing; the store still holds these windows.
        MerchantStatsView view = MerchantStatsAssembler.assemble(
                "M-SHOP", 0, "localhost:8088", T0.plus(Duration.ofDays(8)), store).orElseThrow();

        assertThat(view.windows()).hasSize(MerchantStatsAssembler.MAX_WINDOWS);
        assertThat(view.windows().get(0).start()).isEqualTo(T0.plus(Duration.ofMinutes(5 * 13)));
        assertThat(view.windows().get(0).start()).isAfter(view.windows().get(1).start());
    }

    private void send(String cardId, Merchant at, Instant eventTime) {
        transactions.pipeInput(cardId, Transaction.newBuilder()
                .setTransactionId(UUID.randomUUID().toString())
                .setCardId(cardId)
                .setMerchantId(at.getMerchantId())
                .setAmountMinor(1_000)
                .setCurrency("EUR")
                .setEventTime(eventTime)
                .setLat(at.getLat())
                .setLon(at.getLon())
                .build());
    }

    private static Card card(String id, CardStatus status) {
        return Card.newBuilder().setCardId(id).setCustomerId("CUST-1").setStatus(status)
                .setDailyLimitMinor(1_000_000L).setCurrency("EUR").build();
    }

    private static Merchant merchant(String id, double lat, double lon) {
        return Merchant.newBuilder().setMerchantId(id).setName(id).setMcc("5411").setCountry("DE")
                .setLat(lat).setLon(lon).build();
    }
}
