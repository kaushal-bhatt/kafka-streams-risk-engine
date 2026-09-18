package com.kaushal.riskengine.dashboard;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.config.RiskEngineProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tails {@code payments.decisions.v1} and pushes each decision to connected dashboards over
 * server-sent events.
 *
 * <p>A plain consumer with {@code assign()}, deliberately not part of the Streams topology and
 * not in a consumer group:
 * <ul>
 *   <li>A {@code peek()} in the topology would only see the partitions this instance owns, so
 *       with two instances each dashboard would show half the traffic.</li>
 *   <li>Assigning every partition directly means every instance's dashboard shows everything,
 *       and no group rebalance ever involves the dashboard.</li>
 * </ul>
 * On start it rewinds a little on each partition, so a freshly opened page has history.
 */
@Component
public class DecisionFeed implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DecisionFeed.class);
    private static final int HISTORY = 200;
    private static final int REWIND_PER_PARTITION = 40;

    private final RiskEngineProperties properties;
    private final Deque<DecisionView> recent = new ArrayDeque<>();
    private final Map<String, LongAdder> byDecision = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> byRule = new ConcurrentHashMap<>();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    private volatile boolean running;
    private volatile KafkaConsumer<String, Decision> consumer;
    private Thread thread;

    public DecisionFeed(RiskEngineProperties properties) {
        this.properties = properties;
    }

    public SseEmitter subscribe() {
        // No timeout: the stream stays open as long as the page does.
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        try {
            emitter.send(SseEmitter.event().name("snapshot").data(snapshot()));
        } catch (Exception e) {
            emitters.remove(emitter);
        }
        return emitter;
    }

    Map<String, Object> snapshot() {
        List<DecisionView> history;
        synchronized (recent) {
            history = recent.stream()
                    .sorted(Comparator.comparing(DecisionView::evaluatedAt).reversed())
                    .toList();
        }
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("recent", history);
        snapshot.put("decisions", totals(byDecision));
        snapshot.put("rules", totals(byRule));
        return snapshot;
    }

    private void publish(DecisionView view) {
        synchronized (recent) {
            recent.addFirst(view);
            while (recent.size() > HISTORY) {
                recent.removeLast();
            }
        }
        byDecision.computeIfAbsent(view.decision(), k -> new LongAdder()).increment();
        view.reasons().forEach(r -> byRule.computeIfAbsent(r.rule(), k -> new LongAdder()).increment());

        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("decision").data(view));
            } catch (Exception e) {
                // The browser went away. Drop it; EventSource reconnects on its own if it wants to.
                emitters.remove(emitter);
            }
        }
    }

    private void run() {
        while (running) {
            try (KafkaConsumer<String, Decision> c = new KafkaConsumer<>(consumerProperties())) {
                consumer = c;
                List<TopicPartition> partitions = c.partitionsFor(Topics.DECISIONS).stream()
                        .map(p -> new TopicPartition(p.topic(), p.partition()))
                        .toList();
                c.assign(partitions);

                Map<TopicPartition, Long> begin = c.beginningOffsets(partitions);
                Map<TopicPartition, Long> end = c.endOffsets(partitions);
                partitions.forEach(tp -> c.seek(tp, Math.max(begin.get(tp), end.get(tp) - REWIND_PER_PARTITION)));
                log.info("decision feed tailing {} partitions of {}", partitions.size(), Topics.DECISIONS);

                while (running) {
                    for (ConsumerRecord<String, Decision> record : c.poll(Duration.ofMillis(500))) {
                        if (record.value() != null) {
                            publish(DecisionView.of(record.value()));
                        }
                    }
                }
            } catch (WakeupException e) {
                // stop() was called.
            } catch (Exception e) {
                if (running) {
                    log.warn("decision feed interrupted ({}); retrying in 5s", e.getMessage());
                    sleep(5_000);
                }
            } finally {
                consumer = null;
            }
        }
    }

    private Properties consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "decision-feed-" + properties.applicationServer());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        // assign(), no group: nothing to commit.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // The engine writes decisions inside exactly-once transactions. read_uncommitted would
        // show decisions from a transaction that later aborted - ones that officially never
        // happened, and that get produced again on retry.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, properties.schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return props;
    }

    private static Map<String, Long> totals(Map<String, LongAdder> counters) {
        Map<String, Long> out = new TreeMap<>();
        counters.forEach((k, v) -> out.put(k, v.sum()));
        return out;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void start() {
        running = true;
        thread = new Thread(this::run, "decision-feed");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void stop() {
        running = false;
        KafkaConsumer<String, Decision> c = consumer;
        if (c != null) {
            c.wakeup();
        }
        emitters.forEach(SseEmitter::complete);
        if (thread != null) {
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
