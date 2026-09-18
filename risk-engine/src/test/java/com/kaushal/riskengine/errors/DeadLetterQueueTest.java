package com.kaushal.riskengine.errors;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.Transaction;
import com.kaushal.riskengine.config.AvroSerdes;
import com.kaushal.riskengine.config.RiskEngineProperties;
import com.kaushal.riskengine.topology.DecisionTopology;
import com.kaushal.riskengine.topology.EnrichmentTopology;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A poison pill on the transactions topic: bytes that are not valid Avro.
 */
class DeadLetterQueueTest {

    private static final String SCOPE = "dlq-test";
    private static final byte[] POISON = "{\"this is\": \"not avro\"}".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path stateDir;

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private TopologyTestDriver driver;
    private AvroSerdes serdes;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @Test
    @DisplayName("a poison pill is dead-lettered byte-for-byte with its provenance, and the partition keeps moving")
    void poisonPillIsDeadLetteredAndProcessingContinues() {
        MockProducer<byte[], byte[]> dlq = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        start(new DeadLetterPublisher(dlq, meters));
        TestOutputTopic<String, Decision> decisions = driver.createOutputTopic(
                Topics.DECISIONS, Serdes.String().deserializer(), serdes.<Decision>value().deserializer());

        driver.createInputTopic(Topics.TRANSACTIONS, new StringSerializer(), new ByteArraySerializer())
                .pipeInput("CARD-1", POISON);

        assertThat(dlq.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> dead = dlq.history().get(0);
        assertThat(dead.topic()).isEqualTo(Topics.TRANSACTIONS_DLQ);
        assertThat(new String(dead.key(), StandardCharsets.UTF_8)).isEqualTo("CARD-1");
        assertThat(dead.value()).isEqualTo(POISON);
        assertThat(header(dead, DeadLetterPublisher.H_TOPIC)).isEqualTo(Topics.TRANSACTIONS);
        assertThat(header(dead, DeadLetterPublisher.H_OFFSET)).isEqualTo("0");
        assertThat(header(dead, DeadLetterPublisher.H_EXCEPTION)).contains("SerializationException");
        assertThat(meters.counter("risk.dlq.records", "source", Topics.TRANSACTIONS).count()).isEqualTo(1);
        assertThat(decisions.isEmpty()).isTrue();

        // The very next, valid record on the same partition still gets a decision.
        driver.createInputTopic(Topics.TRANSACTIONS, Serdes.String().serializer(), serdes.<Transaction>value().serializer())
                .pipeInput("CARD-1", Transaction.newBuilder()
                        .setTransactionId(UUID.randomUUID().toString())
                        .setCardId("CARD-1")
                        .setMerchantId("M-1")
                        .setAmountMinor(1_000)
                        .setEventTime(Instant.parse("2026-09-18T10:00:00Z"))
                        .setLat(52.52)
                        .setLon(13.405)
                        .build());
        assertThat(decisions.readValuesToList()).hasSize(1);
    }

    @Test
    @DisplayName("if the DLQ itself is down, the engine stops rather than silently dropping the record")
    void failsWhenTheDeadLetterCannotBeSaved() {
        start(new DeadLetterPublisher(new MockProducer<>(), meters) {
            @Override
            public void publish(ConsumerRecord<byte[], byte[]> failed, Exception cause, String taskId) throws Exception {
                throw new IOException("dead-letter topic unavailable");
            }
        });

        assertThatThrownBy(() -> driver.createInputTopic(Topics.TRANSACTIONS, new StringSerializer(), new ByteArraySerializer())
                .pipeInput("CARD-1", POISON))
                .isInstanceOf(StreamsException.class);
    }

    private void start(DeadLetterPublisher publisher) {
        serdes = new AvroSerdes(new RiskEngineProperties(
                "dlq-test", "dummy:9092", "mock://" + SCOPE, stateDir.toString(), "localhost:8088", 0, "at_least_once"));
        StreamsBuilder builder = new StreamsBuilder();
        new DecisionTopology(serdes, meters).decisionStream(new EnrichmentTopology(serdes).enrichmentStream(builder));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dlq-test-" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        // Exactly the wiring StreamsConfiguration uses in the application.
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeadLetterQueueHandler.class);
        config.put(DeadLetterQueueHandler.PUBLISHER_CONFIG, publisher);
        driver = new TopologyTestDriver(builder.build(), config);
    }

    private static String header(ProducerRecord<byte[], byte[]> record, String key) {
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }
}
