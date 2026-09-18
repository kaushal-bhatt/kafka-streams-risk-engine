package com.kaushal.riskengine.query;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.CardProfile;
import com.kaushal.riskengine.avro.DailySpend;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.LastSeen;
import com.kaushal.riskengine.avro.VelocityEntry;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * Opens this instance's stores for one partition via Interactive Queries.
 *
 * <p>{@code withPartition} narrows each store handle to the single partition that holds the
 * card. Without it, every {@code get} fans out across every partition this instance hosts,
 * when the key can only ever be in one of them.
 *
 * <p>With {@code allowStale}, the handle may be a <em>standby</em> copy of the partition, the
 * warm replica kept for failover, and not only the active one. Kafka Streams refuses standby
 * reads unless asked explicitly, because a standby can trail the active copy.
 */
@Component
public class LocalCardStores {

    private final StreamsBuilderFactoryBean factoryBean;

    public LocalCardStores(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    public CardStores forPartition(int partition, boolean allowStale) {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            throw new StoreNotReadyException("Kafka Streams has not started");
        }
        try {
            return new CardStores(
                    open(streams, Topics.CARD_PROFILE_STORE, QueryableStoreTypes.<String, CardProfile>keyValueStore(), partition, allowStale),
                    open(streams, Topics.SPEND_STORE, QueryableStoreTypes.<String, DailySpend>keyValueStore(), partition, allowStale),
                    open(streams, Topics.GEO_STORE, QueryableStoreTypes.<String, LastSeen>keyValueStore(), partition, allowStale),
                    open(streams, Topics.VELOCITY_STORE, QueryableStoreTypes.<String, VelocityEntry>windowStore(), partition, allowStale),
                    open(streams, Topics.LAST_DECISION_STORE, QueryableStoreTypes.<String, Decision>keyValueStore(), partition, allowStale));
        } catch (InvalidStateStoreException e) {
            // Thrown while a rebalance is migrating or restoring the partition.
            throw new StoreNotReadyException("stores for partition " + partition + " are not queryable yet", e);
        }
    }

    private static <T> T open(KafkaStreams streams, String store, QueryableStoreType<T> type,
                              int partition, boolean allowStale) {
        StoreQueryParameters<T> params = StoreQueryParameters.fromNameAndType(store, type).withPartition(partition);
        return streams.store(allowStale ? params.enableStaleStores() : params);
    }
}
