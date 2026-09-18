package com.kaushal.riskengine.query;

import com.kaushal.riskengine.avro.CardProfile;
import com.kaushal.riskengine.avro.DailySpend;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.LastSeen;
import com.kaushal.riskengine.avro.VelocityEntry;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Builds a {@link CardRiskView} from the stores. Pure: no Kafka Streams runtime, no HTTP, so it
 * can be tested against the stores a {@code TopologyTestDriver} exposes.
 */
public final class CardRiskAssembler {

    private static final long DAY_MILLIS = Duration.ofDays(1).toMillis();

    private CardRiskAssembler() {
    }

    /**
     * @return empty when no store has anything for this card - the engine has never seen it
     */
    public static Optional<CardRiskView> assemble(String cardId, int partition, String servedBy,
                                                  Instant now, CardStores stores) {
        CardProfile profile = stores.profiles().get(cardId);
        DailySpend spend = stores.spend().get(cardId);
        LastSeen lastSeen = stores.geo().get(cardId);
        Decision lastDecision = stores.lastDecisions().get(cardId);
        CardRiskView.Activity activity = activity(stores, cardId, now);

        if (profile == null && spend == null && lastSeen == null && lastDecision == null
                && activity.lastAttemptAt() == null) {
            return Optional.empty();
        }

        return Optional.of(new CardRiskView(
                cardId,
                servedBy,
                null,
                partition,
                false,
                profile == null ? null : new CardRiskView.Profile(
                        profile.getCustomerId(),
                        profile.getStatus().name(),
                        profile.getDailyLimitMinor(),
                        profile.getCurrency(),
                        profile.getRiskTier().name(),
                        profile.getHomeCountry()),
                spendToday(profile, spend, now),
                lastSeen == null ? null : new CardRiskView.Location(
                        lastSeen.getLat(), lastSeen.getLon(), lastSeen.getPlace(), lastSeen.getSeenAt()),
                activity,
                lastDecision == null ? null : new CardRiskView.LastDecision(
                        lastDecision.getTransactionId(),
                        lastDecision.getDecision().name(),
                        lastDecision.getScore(),
                        lastDecision.getAmountMinor(),
                        lastDecision.getReasons().stream()
                                .map(r -> new CardRiskView.Reason(r.getRule(), r.getScore(), r.getDetail()))
                                .toList(),
                        lastDecision.getEvaluatedAt())
        ));
    }

    private static CardRiskView.Spend spendToday(CardProfile profile, DailySpend spend, Instant now) {
        long today = Math.floorDiv(now.toEpochMilli(), DAY_MILLIS);
        // A total from an earlier day is history, not today's spend.
        long spent = (spend != null && spend.getEpochDay() == today) ? spend.getSpentMinor() : 0;
        Long remaining = profile == null ? null : Math.max(0, profile.getDailyLimitMinor() - spent);
        return new CardRiskView.Spend(spent, remaining);
    }

    private static CardRiskView.Activity activity(CardStores stores, String cardId, Instant now) {
        List<KeyValue<Long, VelocityEntry>> attempts = new ArrayList<>();
        try (WindowStoreIterator<VelocityEntry> it =
                     stores.velocity().fetch(cardId, now.minus(Duration.ofMinutes(10)), now)) {
            it.forEachRemaining(attempts::add);
        }
        long cutoff60s = now.minusSeconds(60).toEpochMilli();
        long last60s = attempts.stream().filter(kv -> kv.key >= cutoff60s).count();
        Instant lastAttempt = attempts.stream()
                .map(kv -> kv.key)
                .max(Long::compare)
                .map(Instant::ofEpochMilli)
                .orElse(null);
        return new CardRiskView.Activity(last60s, attempts.size(), lastAttempt);
    }
}
