package com.kaushal.trafficgen;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Decision;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Times transactions from send to decision, as a downstream consumer would see it.
 *
 * <p>The consumer is {@code read_committed}, like any real consumer of an exactly-once
 * topic should be. That is the point of the measurement: under exactly-once, a decision is
 * only readable once the engine commits its transaction, and that commit is what the probe
 * captures. Both clocks are this JVM's {@code nanoTime}, so there is no cross-machine skew.
 */
class LatencyProbe implements AutoCloseable {

    private final KafkaConsumer<String, Decision> consumer;
    private final Map<String, Long> sentAt = new ConcurrentHashMap<>();
    private final Map<String, Long> latencyNanos = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private Thread poller;

    LatencyProbe(Config config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // Deliver each record as soon as it's readable rather than batching up to 500 ms.
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 10);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        this.consumer = new KafkaConsumer<>(props);
    }

    /** Positions at the end of the decisions topic before anything is sent. */
    void start() {
        List<TopicPartition> partitions = consumer.partitionsFor(Topics.DECISIONS).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        // seekToEnd is lazy; resolve it now, or the first poll could start after our records.
        partitions.forEach(consumer::position);

        poller = new Thread(this::poll, "latency-probe");
        poller.setDaemon(true);
        poller.start();
    }

    void sent(String transactionId) {
        sentAt.put(transactionId, System.nanoTime());
    }

    private void poll() {
        try {
            while (running) {
                for (ConsumerRecord<String, Decision> record : consumer.poll(Duration.ofMillis(50))) {
                    long now = System.nanoTime();
                    Long sent = sentAt.get(record.value().getTransactionId());
                    if (sent != null) {
                        latencyNanos.putIfAbsent(record.value().getTransactionId(), now - sent);
                    }
                }
            }
        } catch (WakeupException e) {
            // close() was called.
        }
    }

    void awaitAndReport(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (latencyNanos.size() < sentAt.size() && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }

        List<Double> millis = new ArrayList<>(latencyNanos.values().stream().map(n -> n / 1_000_000.0).toList());
        Collections.sort(millis);
        System.out.printf("latency: %d sent, %d decisions received, %d missing%n",
                sentAt.size(), millis.size(), sentAt.size() - millis.size());
        if (!millis.isEmpty()) {
            System.out.printf("latency: send -> decision readable (ms)   p50 %.0f   p95 %.0f   p99 %.0f   max %.0f%n",
                    percentile(millis, 50), percentile(millis, 95), percentile(millis, 99), millis.get(millis.size() - 1));
        }
    }

    private static double percentile(List<Double> sorted, int p) {
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    @Override
    public void close() throws InterruptedException {
        running = false;
        consumer.wakeup();
        if (poller != null) {
            poller.join(2_000);
        }
        consumer.close();
    }
}
