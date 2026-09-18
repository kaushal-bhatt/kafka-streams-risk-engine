package com.kaushal.riskengine.errors;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.config.RiskEngineProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Writes a record that could not be deserialized to the dead-letter topic, byte for byte,
 * with headers saying where it came from and why it failed.
 *
 * <p>The original bytes are kept exactly as they arrived, because the fix for a poison pill is
 * usually in the producer or the schema, and the only way to prove the fix is to replay the
 * same bytes through it.
 *
 * <p>This producer is separate from the one Kafka Streams uses, so under exactly-once the DLQ
 * write is <em>not</em> part of the Streams transaction. If a task aborts and reprocesses, a
 * record can reach the DLQ twice. For a dead-letter queue that's the right trade: a duplicate
 * in the DLQ costs nothing, and a missing one hides a problem.
 */
@Component
public class DeadLetterPublisher implements DisposableBean {

    static final String H_TOPIC = "dlq.original.topic";
    static final String H_PARTITION = "dlq.original.partition";
    static final String H_OFFSET = "dlq.original.offset";
    static final String H_TIMESTAMP = "dlq.original.timestamp";
    static final String H_EXCEPTION = "dlq.exception.class";
    static final String H_MESSAGE = "dlq.exception.message";
    static final String H_TASK = "dlq.task.id";

    private static final int MAX_MESSAGE_LENGTH = 1_000;

    private final Producer<byte[], byte[]> producer;
    private final MeterRegistry meterRegistry;

    @Autowired
    public DeadLetterPublisher(RiskEngineProperties properties, MeterRegistry meterRegistry) {
        this(new KafkaProducer<>(producerProperties(properties)), meterRegistry);
    }

    DeadLetterPublisher(Producer<byte[], byte[]> producer, MeterRegistry meterRegistry) {
        this.producer = producer;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Synchronous on purpose. The caller continues past the bad record only once it is safely
     * in the DLQ; if the DLQ write fails, this throws and the record is not skipped.
     */
    public void publish(ConsumerRecord<byte[], byte[]> failed, Exception cause, String taskId) throws Exception {
        ProducerRecord<byte[], byte[]> out =
                new ProducerRecord<>(Topics.TRANSACTIONS_DLQ, null, failed.key(), failed.value());

        for (Header header : failed.headers()) {
            out.headers().add(header);
        }
        out.headers()
                .add(H_TOPIC, bytes(failed.topic()))
                .add(H_PARTITION, bytes(String.valueOf(failed.partition())))
                .add(H_OFFSET, bytes(String.valueOf(failed.offset())))
                .add(H_TIMESTAMP, bytes(String.valueOf(failed.timestamp())))
                .add(H_EXCEPTION, bytes(cause.getClass().getName()))
                .add(H_MESSAGE, bytes(truncate(rootMessage(cause))))
                .add(H_TASK, bytes(taskId));

        producer.send(out).get(5, TimeUnit.SECONDS);
        meterRegistry.counter("risk.dlq.records", "source", failed.topic()).increment();
    }

    @Override
    public void destroy() {
        producer.close(Duration.ofSeconds(5));
    }

    private static Properties producerProperties(RiskEngineProperties properties) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "dead-letter-" + properties.applicationServer());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }

    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root == t ? t.getMessage() : t.getMessage() + " | caused by: " + root.getMessage();
        return message == null ? t.getClass().getSimpleName() : message;
    }

    private static String truncate(String s) {
        return s.length() <= MAX_MESSAGE_LENGTH ? s : s.substring(0, MAX_MESSAGE_LENGTH) + "...";
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
