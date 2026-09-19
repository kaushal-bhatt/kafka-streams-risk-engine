package com.kaushal.riskengine.topology;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.DecisionCounts;
import com.kaushal.riskengine.avro.DecisionType;
import com.kaushal.riskengine.avro.MerchantStats;
import com.kaushal.riskengine.config.AvroSerdes;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Stage 4 - the analytics path: each merchant's decline rate, per 5-minute window.
 *
 * <pre>
 *   decisions (keyed by card)
 *     -&gt; rekey by merchant -&gt; repartition
 *     -&gt; 5-minute event-time windows, 30 s grace
 *     -&gt; count total and declined          (queryable: merchant-stats-store)
 *     -&gt; suppress until the window closes
 *     -&gt; risk.merchant-stats.v1             (one final record per merchant per window)
 * </pre>
 *
 * <p>This is where the DSL is the right tool and the Processor API isn't - the opposite of
 * the decision path. The output genuinely <em>is</em> a stream of windowed statistics: late
 * data should be absorbed by a grace period, and downstream wants one final number per window,
 * not every intermediate update. The DSL expresses all of that directly.
 *
 * <p>The rekey to merchantId forces a repartition topic: an extra hop through the broker.
 * On the decision path that would have cost every authorisation a network round trip (which
 * is why merchants are a GlobalKTable there). Here nothing is waiting on the result, so the
 * hop costs nothing that matters.
 */
@Component
public class AnalyticsTopology {

    public static final Duration WINDOW = Duration.ofMinutes(5);
    /** A decision up to 30 s late (in event time) still counts in its window. */
    public static final Duration GRACE = Duration.ofSeconds(30);
    /** Long enough for the query API to show the last hour of windows. */
    public static final Duration RETENTION = Duration.ofHours(2);

    private final AvroSerdes avroSerdes;

    public AnalyticsTopology(AvroSerdes avroSerdes) {
        this.avroSerdes = avroSerdes;
    }

    @Bean
    public KStream<String, MerchantStats> merchantStatsStream(KStream<String, Decision> decisionStream) {
        SpecificAvroSerde<Decision> decisionSerde = avroSerdes.value();
        SpecificAvroSerde<DecisionCounts> countsSerde = avroSerdes.value();
        SpecificAvroSerde<MerchantStats> statsSerde = avroSerdes.value();

        KStream<String, MerchantStats> stats = decisionStream
                .selectKey((cardId, decision) -> decision.getMerchantId(), Named.as("rekey-by-merchant"))
                // Named explicitly, so the internal topic has a stable name
                // (risk-engine-decisions-by-merchant-repartition) rather than a generated one
                // that changes whenever the topology does.
                .repartition(Repartitioned.<String, Decision>as("decisions-by-merchant")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(decisionSerde))
                .groupByKey(Grouped.with(Serdes.String(), decisionSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW, GRACE))
                .aggregate(
                        () -> DecisionCounts.newBuilder().build(),
                        (merchantId, decision, counts) -> DecisionCounts.newBuilder()
                                .setTotal(counts.getTotal() + 1)
                                .setDeclined(counts.getDeclined()
                                        + (decision.getDecision() == DecisionType.DECLINE ? 1 : 0))
                                .build(),
                        Named.as("count-decisions-per-merchant"),
                        Materialized.<String, DecisionCounts, WindowStore<Bytes, byte[]>>as(Topics.MERCHANT_STATS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(countsSerde)
                                .withRetention(RETENTION))
                // Hold each window's result back until the window has closed - end plus grace,
                // in stream time - then emit it once. Without this, every decision would emit
                // a fresh partial count downstream. The query API still sees the live, running
                // counts, because it reads the store, not this output.
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                        .withName("final-result-per-window"))
                .toStream(Named.as("closed-windows"))
                .map((window, counts) -> KeyValue.pair(window.key(), MerchantStats.newBuilder()
                        .setMerchantId(window.key())
                        .setTotal(counts.getTotal())
                        .setDeclined(counts.getDeclined())
                        .setDeclineRate(counts.getTotal() == 0 ? 0 : (double) counts.getDeclined() / counts.getTotal())
                        .setWindowStart(window.window().startTime())
                        .setWindowEnd(window.window().endTime())
                        .build()), Named.as("to-merchant-stats"));

        stats.to(Topics.MERCHANT_STATS, Produced.with(Serdes.String(), statsSerde).withName("merchant-stats-sink"));
        return stats;
    }
}
