package com.kaushal.riskengine.query;

import com.kaushal.riskengine.avro.DecisionCounts;
import com.kaushal.riskengine.topology.AnalyticsTopology;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Builds a {@link MerchantStatsView} from the analytics window store. Pure, so it can be tested
 * against the store a TopologyTestDriver exposes.
 */
public final class MerchantStatsAssembler {

    /** One hour of 5-minute windows. */
    public static final int MAX_WINDOWS = 12;

    private MerchantStatsAssembler() {
    }

    /**
     * The merchant's most recent windows that the store still holds - newest first, at most an
     * hour's worth.
     *
     * <p>Deliberately not "the last hour by wall clock". The store keeps windows relative to
     * <em>stream</em> time, and during a replay stream time can be days ahead of the wall
     * clock: a wall-clock range then finds nothing at all, even though the store is full. The
     * first live run showed exactly that. Asking for everything the store holds for the key is
     * cheap, because retention bounds it to a couple of hours of windows.
     *
     * @return empty when the store holds nothing for this merchant
     */
    public static Optional<MerchantStatsView> assemble(String merchantId, int partition, String servedBy,
                                                       Instant now, ReadOnlyWindowStore<String, DecisionCounts> store) {
        List<MerchantStatsView.Window> windows = new ArrayList<>();
        try (WindowStoreIterator<DecisionCounts> it =
                     store.fetch(merchantId, Instant.EPOCH, Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, DecisionCounts> kv = it.next();
                Instant start = Instant.ofEpochMilli(kv.key);
                Instant end = start.plus(AnalyticsTopology.WINDOW);
                DecisionCounts counts = kv.value;
                windows.add(new MerchantStatsView.Window(
                        start, end, counts.getTotal(), counts.getDeclined(),
                        counts.getTotal() == 0 ? 0 : (double) counts.getDeclined() / counts.getTotal(),
                        !end.plus(AnalyticsTopology.GRACE).isAfter(now)));
            }
        }
        if (windows.isEmpty()) {
            return Optional.empty();
        }
        windows.sort(Comparator.comparing(MerchantStatsView.Window::start).reversed());
        List<MerchantStatsView.Window> newest = windows.subList(0, Math.min(MAX_WINDOWS, windows.size()));
        return Optional.of(new MerchantStatsView(merchantId, servedBy, null, partition, List.copyOf(newest)));
    }
}
