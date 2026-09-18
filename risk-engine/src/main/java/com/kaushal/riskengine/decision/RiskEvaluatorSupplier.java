package com.kaushal.riskengine.decision;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.DailySpend;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.EnrichedTransaction;
import com.kaushal.riskengine.avro.LastSeen;
import com.kaushal.riskengine.avro.VelocityEntry;
import com.kaushal.riskengine.config.AvroSerdes;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Set;

/**
 * Creates {@link RiskEvaluator} instances - one per task - and declares the stores they own.
 *
 * <p>Declaring stores through {@link #stores()} rather than {@code builder.addStateStore(...)}
 * keeps the processor and its state in one place: the DSL connects them automatically, and
 * nothing else in the topology can reach these stores by accident.
 *
 * <p>All three stores are changelogged (the StoreBuilder default), so a restarted or
 * reassigned task rebuilds its state from Kafka. None are cached: StoreBuilders default to no
 * caching, and this processor reads its own writes in the same call, so a cache would add
 * nothing on the read path.
 */
public class RiskEvaluatorSupplier implements ProcessorSupplier<String, EnrichedTransaction, String, Decision> {

    private final AvroSerdes avroSerdes;
    private final MeterRegistry meterRegistry;

    public RiskEvaluatorSupplier(AvroSerdes avroSerdes, MeterRegistry meterRegistry) {
        this.avroSerdes = avroSerdes;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public Processor<String, EnrichedTransaction, String, Decision> get() {
        return new RiskEvaluator(meterRegistry);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder<?> velocity = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        Topics.VELOCITY_STORE,
                        RiskRules.VELOCITY_RETENTION,
                        // Used as an append-only, time-indexed log, not as fixed windows:
                        // each attempt is put at its own timestamp and read back by range.
                        // The window size only has to be positive and within retention.
                        Duration.ofMillis(1),
                        // Keep every attempt, not just the latest per timestamp. Two
                        // attempts in the same millisecond are two attempts.
                        true),
                Serdes.String(),
                avroSerdes.<VelocityEntry>value());

        StoreBuilder<?> spend = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Topics.SPEND_STORE),
                Serdes.String(),
                avroSerdes.<DailySpend>value());

        StoreBuilder<?> geo = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Topics.GEO_STORE),
                Serdes.String(),
                avroSerdes.<LastSeen>value());

        return Set.of(velocity, spend, geo);
    }
}
