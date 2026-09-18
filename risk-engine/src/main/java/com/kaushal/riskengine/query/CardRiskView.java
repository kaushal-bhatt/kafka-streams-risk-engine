package com.kaushal.riskengine.query;

import java.time.Instant;
import java.util.List;

/**
 * Everything the engine knows about one card, assembled from five local state stores.
 *
 * <p>Plain records rather than the Avro classes: Avro-generated types expose their schema as
 * a getter, and Jackson would try to serialise it.
 *
 * @param servedBy  the instance whose state stores answered
 * @param routedVia the instance the caller actually asked, when that wasn't the one that
 *                  answered; null when the request landed on the answering instance directly
 * @param partition the partition that holds this card - and therefore which instance owns it
 * @param stale     true when the answer came from a standby replica because the owner was
 *                  unreachable. A standby trails the owner by whatever it hadn't replicated
 *                  yet - usually nothing, but not guaranteed.
 */
public record CardRiskView(
        String cardId,
        String servedBy,
        String routedVia,
        int partition,
        boolean stale,
        Profile profile,
        Spend spendToday,
        Location lastSeen,
        Activity recentActivity,
        LastDecision lastDecision
) {

    public CardRiskView withRoutedVia(String via) {
        return new CardRiskView(cardId, servedBy, via, partition, stale, profile, spendToday, lastSeen,
                recentActivity, lastDecision);
    }

    public CardRiskView asStale() {
        return new CardRiskView(cardId, servedBy, routedVia, partition, true, profile, spendToday, lastSeen,
                recentActivity, lastDecision);
    }

    public record Profile(String customerId, String status, long dailyLimitMinor, String currency,
                          String riskTier, String homeCountry) {
    }

    /** {@code remainingMinor} is null when the card has no profile, so no known limit. */
    public record Spend(long spentMinor, Long remainingMinor) {
    }

    public record Location(double lat, double lon, String place, Instant seenAt) {
    }

    public record Activity(long attemptsLast60s, long attemptsLast10m, Instant lastAttemptAt) {
    }

    public record LastDecision(String transactionId, String decision, int score, long amountMinor,
                               List<Reason> reasons, Instant evaluatedAt) {
    }

    public record Reason(String rule, int score, String detail) {
    }
}
