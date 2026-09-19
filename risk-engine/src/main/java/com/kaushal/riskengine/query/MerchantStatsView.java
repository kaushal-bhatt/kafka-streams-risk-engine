package com.kaushal.riskengine.query;

import java.time.Instant;
import java.util.List;

/**
 * A merchant's decision counts per 5-minute window: the most recent hour the store holds,
 * newest first.
 *
 * @param routedVia set when the request was forwarded here from the instance first asked
 */
public record MerchantStatsView(
        String merchantId,
        String servedBy,
        String routedVia,
        int partition,
        List<Window> windows
) {

    public MerchantStatsView withRoutedVia(String via) {
        return new MerchantStatsView(merchantId, servedBy, via, partition, windows);
    }

    /**
     * @param closed true once the window's end plus grace has passed: its counts are final and
     *               have been published to risk.merchant-stats.v1. An open window's counts are
     *               live and still changing - visible here, but not yet in the topic. Judged by
     *               the wall clock, which matches event time for live traffic. During an
     *               accelerated replay, event time runs ahead, so a window can already be final
     *               while this still says false.
     */
    public record Window(Instant start, Instant end, long total, long declined, double declineRate,
                         boolean closed) {
    }
}
