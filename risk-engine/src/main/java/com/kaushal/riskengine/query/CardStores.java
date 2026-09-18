package com.kaushal.riskengine.query;

import com.kaushal.riskengine.avro.CardProfile;
import com.kaushal.riskengine.avro.DailySpend;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.LastSeen;
import com.kaushal.riskengine.avro.VelocityEntry;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

/**
 * Read-only handles on the five stores that describe a card.
 *
 * <p>Read-only on purpose. Interactive Queries hand out the same stores the topology writes
 * to, and nothing on the query path should ever be able to write to them.
 */
public record CardStores(
        ReadOnlyKeyValueStore<String, CardProfile> profiles,
        ReadOnlyKeyValueStore<String, DailySpend> spend,
        ReadOnlyKeyValueStore<String, LastSeen> geo,
        ReadOnlyWindowStore<String, VelocityEntry> velocity,
        ReadOnlyKeyValueStore<String, Decision> lastDecisions
) {
}
