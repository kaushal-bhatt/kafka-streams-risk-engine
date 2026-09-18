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
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

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
    private final boolean inMemoryStores;

    public RiskEvaluatorSupplier(AvroSerdes avroSerdes, MeterRegistry meterRegistry) {
        this(avroSerdes, meterRegistry, false);
    }

    /**
     * @param inMemoryStores in-memory instead of RocksDB. Same store semantics, no disk. Used
     *                       only by the offline evaluation, which drives the topology through
     *                       TopologyTestDriver: that commits after every record, and each
     *                       commit flushes RocksDB to disk - a disk flush per transaction.
     */
    public RiskEvaluatorSupplier(AvroSerdes avroSerdes, MeterRegistry meterRegistry, boolean inMemoryStores) {
        this.avroSerdes = avroSerdes;
        this.meterRegistry = meterRegistry;
        this.inMemoryStores = inMemoryStores;
    }

    @Override
    public Processor<String, EnrichedTransaction, String, Decision> get() {
        return new RiskEvaluator(meterRegistry);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        // Used as an append-only, time-indexed log, not as fixed windows: each attempt is put
        // at its own timestamp and read back by range. The window size only has to be positive
        // and within retention. retainDuplicates keeps every attempt, not just the latest per
        // timestamp: two attempts in the same millisecond are two attempts.
        Duration windowSize = Duration.ofMillis(1);
        WindowBytesStoreSupplier velocityStore = inMemoryStores
                ? Stores.inMemoryWindowStore(Topics.VELOCITY_STORE, RiskRules.VELOCITY_RETENTION, windowSize, true)
                : Stores.persistentWindowStore(Topics.VELOCITY_STORE, RiskRules.VELOCITY_RETENTION, windowSize, true);

        StoreBuilder<?> velocity = Stores.windowStoreBuilder(velocityStore, Serdes.String(), avroSerdes.<VelocityEntry>value());
        StoreBuilder<?> spend = Stores.keyValueStoreBuilder(keyValue(Topics.SPEND_STORE), Serdes.String(), avroSerdes.<DailySpend>value());
        StoreBuilder<?> geo = Stores.keyValueStoreBuilder(keyValue(Topics.GEO_STORE), Serdes.String(), avroSerdes.<LastSeen>value());

        return Set.of(velocity, spend, geo);
    }

    private KeyValueBytesStoreSupplier keyValue(String name) {
        return inMemoryStores ? Stores.inMemoryKeyValueStore(name) : Stores.persistentKeyValueStore(name);
    }
}
