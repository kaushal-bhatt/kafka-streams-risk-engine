package com.kaushal.riskengine.evaluation;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.Transaction;
import com.kaushal.riskengine.config.AvroSerdes;
import com.kaushal.riskengine.config.RiskEngineProperties;
import com.kaushal.riskengine.topology.DecisionTopology;
import com.kaushal.riskengine.topology.EnrichmentTopology;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * The production topology - {@link EnrichmentTopology} and {@link DecisionTopology}, the same
 * classes the engine runs - hosted in a {@link TopologyTestDriver} instead of a Kafka cluster.
 *
 * <p>Same joins, same RiskEvaluator, same state stores and serdes, driven one transaction at a
 * time in event-time order. What it leaves out is everything that makes a live run
 * non-deterministic: network timing, rebalances, commit intervals. That's what makes the
 * evaluation reproducible to the transaction. What doesn't carry over is wall-clock
 * behaviour: the hourly eviction punctuator never fires here. That has no effect on decisions,
 * since stale entries are evicted precisely because they no longer influence one.
 *
 * <p>Two deliberate differences from production, neither of which changes a decision:
 * <ul>
 *   <li><b>In-memory state stores instead of RocksDB.</b> TopologyTestDriver commits after
 *       every record, and a commit flushes RocksDB to disk. Across 1.85M transactions that is
 *       1.85M disk flushes per store. The first attempt ran for minutes without finishing
 *       100k rows. The stores' semantics are identical; only where the bytes live differs.</li>
 *   <li><b>Every output topic is drained periodically.</b> The driver keeps every record
 *       produced - decisions, enriched transactions, changelogs, repartition topics - in memory
 *       until it is read. Left alone, that's millions of records by the end of the run.</li>
 * </ul>
 */
final class OfflineEngine implements AutoCloseable {

    private static final String SCOPE = "offline-evaluation-" + UUID.randomUUID();
    private static final int DRAIN_EVERY = 5_000;

    private final TopologyTestDriver driver;
    private final TestInputTopic<String, Customer> customers;
    private final TestInputTopic<String, Card> cards;
    private final TestInputTopic<String, Merchant> merchants;
    private final TestInputTopic<String, Transaction> transactions;
    private final TestOutputTopic<String, Decision> decisions;
    private final Map<String, TestOutputTopic<byte[], byte[]>> drains = new HashMap<>();
    private int sinceDrain;

    OfflineEngine(Path stateDir) {
        AvroSerdes serdes = new AvroSerdes(new RiskEngineProperties(
                "offline-evaluation", "offline:9092", "mock://" + SCOPE, stateDir.toString(),
                "offline:0", 0, "at_least_once"));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "offline-evaluation");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "offline:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        // In-memory for every DSL table - cards, customers, card profiles, merchants, last
        // decision, and the foreign-key join's internal subscription store. That last one only
        // honours this setting from Kafka Streams 3.8, which is why this module runs 3.8.1
        // (see build.gradle.kts). With any store on RocksDB, the driver's commit after every
        // record writes and fsyncs a checkpoint file: ~250 transactions/s. All in memory:
        // ~5,000/s, with reports identical line for line on the same 50,000 transactions.
        // The processor's own stores are switched by the constructor flag below.
        config.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);

        // The config has to reach the builder, or DSL stores are chosen before it's read.
        StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(config)));
        new DecisionTopology(serdes, new SimpleMeterRegistry(), true)
                .decisionStream(new EnrichmentTopology(serdes).enrichmentStream(builder));

        driver = new TopologyTestDriver(builder.build(), config);

        customers = driver.createInputTopic(Topics.CUSTOMERS, Serdes.String().serializer(), serdes.<Customer>value().serializer());
        cards = driver.createInputTopic(Topics.CARDS, Serdes.String().serializer(), serdes.<Card>value().serializer());
        merchants = driver.createInputTopic(Topics.MERCHANTS, Serdes.String().serializer(), serdes.<Merchant>value().serializer());
        transactions = driver.createInputTopic(Topics.TRANSACTIONS, Serdes.String().serializer(), serdes.<Transaction>value().serializer());
        decisions = driver.createOutputTopic(Topics.DECISIONS, Serdes.String().deserializer(), serdes.<Decision>value().deserializer());
    }

    void seed(Customer customer, Card card) {
        customers.pipeInput(customer.getCustomerId(), customer);
        cards.pipeInput(card.getCardId(), card);
    }

    void seed(Merchant merchant) {
        merchants.pipeInput(merchant.getMerchantId(), merchant);
    }

    /** One transaction in, the engine's one decision out. */
    Decision decide(Transaction transaction) {
        transactions.pipeInput(transaction.getCardId(), transaction, transaction.getEventTime());
        if (decisions.isEmpty()) {
            throw new IllegalStateException("no decision for " + transaction.getTransactionId());
        }
        Decision decision = decisions.readValue();

        if (++sinceDrain >= DRAIN_EVERY) {
            drainEverythingButDecisions();
            sinceDrain = 0;
        }
        return decision;
    }

    /** Reads and discards everything the driver has buffered, apart from decisions. */
    private void drainEverythingButDecisions() {
        for (String topic : driver.producedTopicNames()) {
            if (!topic.equals(Topics.DECISIONS)) {
                drains.computeIfAbsent(topic, t ->
                                driver.createOutputTopic(t, new ByteArrayDeserializer(), new ByteArrayDeserializer()))
                        .readRecordsToList();
            }
        }
    }

    @Override
    public void close() {
        driver.close();
        MockSchemaRegistry.dropScope(SCOPE);
    }
}
