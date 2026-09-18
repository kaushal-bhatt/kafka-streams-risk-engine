package com.kaushal.riskengine.decision;

import com.kaushal.riskengine.Geo;
import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.CardProfile;
import com.kaushal.riskengine.avro.CardStatus;
import com.kaushal.riskengine.avro.DailySpend;
import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.DecisionType;
import com.kaushal.riskengine.avro.EnrichedTransaction;
import com.kaushal.riskengine.avro.LastSeen;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.RiskTier;
import com.kaushal.riskengine.avro.RuleHit;
import com.kaushal.riskengine.avro.Transaction;
import com.kaushal.riskengine.avro.VelocityEntry;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The decision path: one enriched transaction in, one decision out.
 *
 * <h2>Why the Processor API and not the DSL</h2>
 *
 * A DSL aggregation ({@code groupByKey().windowedBy(...).count()}) <em>emits</em> its result
 * downstream as a changelog. That is the right shape for publishing a statistic, and the
 * wrong shape here: to decide about the transaction in hand, this node has to <em>read</em>
 * the card's recent history synchronously, combine several reads into one verdict, and then
 * update that history according to the verdict. Wiring that in the DSL means chains of joins
 * over partial aggregates. Here it is one {@code process()} call against three local stores:
 *
 * <ul>
 *   <li>{@code velocity-store} - window store of every attempt, retained 1 h, self-expiring</li>
 *   <li>{@code spend-store} - approved spend today, per card</li>
 *   <li>{@code geo-store} - last location per card</li>
 * </ul>
 *
 * <h2>State follows the verdict</h2>
 *
 * Every attempt is recorded for velocity, because declined attempts are exactly what a card
 * tester generates. But a DECLINED transaction does not add to daily spend - the money never
 * moved - and does not move the card's last-known location. Otherwise one fraudulent charge
 * in Sao Paulo would make the real cardholder's next purchase in Berlin look like
 * impossible travel, and the fraud would lock out the victim.
 *
 * <h2>Bounded state</h2>
 *
 * The window store expires on its own. The two key-value stores would grow forever, one entry
 * per card ever seen, so a wall-clock punctuator evicts stale entries every hour. Unbounded
 * state is the most common way a Kafka Streams application dies in production.
 */
public class RiskEvaluator implements Processor<String, EnrichedTransaction, String, Decision> {

    private static final Logger log = LoggerFactory.getLogger(RiskEvaluator.class);
    private static final long DAY_MILLIS = Duration.ofDays(1).toMillis();

    private final MeterRegistry meterRegistry;
    private final RiskPolicy policy;

    private ProcessorContext<String, Decision> context;
    private WindowStore<String, VelocityEntry> velocityStore;
    private KeyValueStore<String, DailySpend> spendStore;
    private KeyValueStore<String, LastSeen> geoStore;

    public RiskEvaluator(MeterRegistry meterRegistry, RiskPolicy policy) {
        this.meterRegistry = meterRegistry;
        this.policy = policy;
    }

    @Override
    public void init(ProcessorContext<String, Decision> context) {
        this.context = context;
        this.velocityStore = context.getStateStore(Topics.VELOCITY_STORE);
        this.spendStore = context.getStateStore(Topics.SPEND_STORE);
        this.geoStore = context.getStateStore(Topics.GEO_STORE);

        // WALL_CLOCK_TIME, not STREAM_TIME: eviction must happen even when a partition goes
        // quiet. Stream time only advances when records arrive, so a stream-time punctuator
        // on an idle partition would never fire and its state would never be cleaned up.
        context.schedule(RiskRules.EVICTION_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::evictStaleState);
    }

    @Override
    public void process(Record<String, EnrichedTransaction> record) {
        EnrichedTransaction enriched = record.value();
        if (enriched == null || enriched.getTransaction() == null) {
            return;
        }

        String cardId = record.key();
        Transaction txn = enriched.getTransaction();
        CardProfile profile = enriched.getCardProfile();
        Merchant merchant = enriched.getMerchant();
        // Event time, courtesy of TransactionTimestampExtractor.
        long eventTime = record.timestamp();
        long amount = txn.getAmountMinor();

        List<RuleHit> hits = new ArrayList<>();

        // --- Card state -----------------------------------------------------------------
        if (profile == null) {
            hits.add(Rule.UNKNOWN_CARD.hit("card is not in the reference data"));
        } else if (profile.getStatus() != CardStatus.ACTIVE) {
            hits.add(Rule.CARD_NOT_ACTIVE.hit("card status is " + profile.getStatus()));
        }

        // --- Velocity and card testing: one range scan serves both ---------------------
        // Record this attempt first, so the window counts it.
        velocityStore.put(cardId, VelocityEntry.newBuilder()
                .setTransactionId(txn.getTransactionId())
                .setAmountMinor(amount)
                .setMerchantId(txn.getMerchantId())
                .build(), eventTime);

        List<KeyValue<Long, VelocityEntry>> recent = recentAttempts(cardId, eventTime);
        checkVelocity(recent, eventTime, hits);
        checkCardTesting(recent, hits);

        // --- Daily limit ----------------------------------------------------------------
        long epochDay = Math.floorDiv(eventTime, DAY_MILLIS);
        DailySpend stored = spendStore.get(cardId);
        // A late transaction from yesterday must not reset today's running total.
        boolean lateFromEarlierDay = stored != null && stored.getEpochDay() > epochDay;
        long spentToday = (stored != null && stored.getEpochDay() == epochDay) ? stored.getSpentMinor() : 0;

        if (profile != null && spentToday + amount > profile.getDailyLimitMinor()) {
            hits.add(Rule.DAILY_LIMIT.hit("%s spent today + %s would exceed the %s limit".formatted(
                    money(spentToday, txn), money(amount, txn), money(profile.getDailyLimitMinor(), txn))));
        }

        // --- Impossible travel ----------------------------------------------------------
        LastSeen last = geoStore.get(cardId);
        checkImpossibleTravel(last, txn, merchant, eventTime, hits);

        // --- Merchant category ----------------------------------------------------------
        String category = merchant != null ? merchant.getMcc() : txn.getCategory();
        if (category != null && RiskRules.HIGH_RISK_CATEGORIES.contains(category)) {
            hits.add(Rule.MERCHANT_RISK.hit("high-risk merchant category " + category));
        }

        // --- Customer tier: escalates, never triggers ------------------------------------
        if (!hits.isEmpty() && profile != null && profile.getRiskTier() == RiskTier.HIGH) {
            hits.add(Rule.CUSTOMER_RISK_TIER.hit("customer is HIGH risk tier"));
        }

        int score = Math.min(RiskRules.MAX_SCORE, hits.stream().mapToInt(RuleHit::getScore).sum());
        DecisionType verdict = RiskRules.band(score);

        // --- State follows the verdict ---------------------------------------------------
        if (verdict != DecisionType.DECLINE) {
            if (profile != null && !lateFromEarlierDay) {
                spendStore.put(cardId, DailySpend.newBuilder()
                        .setEpochDay(epochDay)
                        .setSpentMinor(spentToday + amount)
                        .build());
            }
            // Never move the location backwards in time on an out-of-order record.
            if (last == null || eventTime >= last.getSeenAt().toEpochMilli()) {
                geoStore.put(cardId, LastSeen.newBuilder()
                        .setLat(txn.getLat())
                        .setLon(txn.getLon())
                        .setSeenAt(Instant.ofEpochMilli(eventTime))
                        .setPlace(merchant != null ? merchant.getCountry() : null)
                        .build());
            }
        }

        Decision decision = Decision.newBuilder()
                .setTransactionId(txn.getTransactionId())
                .setCardId(cardId)
                .setMerchantId(txn.getMerchantId())
                .setDecision(verdict)
                .setScore(score)
                .setReasons(hits)
                .setAmountMinor(amount)
                // Processing time, from the context so tests can control it.
                .setEvaluatedAt(Instant.ofEpochMilli(context.currentSystemTimeMs()))
                .setLabelledFraud(txn.getLabelledFraud())
                .build();

        meterRegistry.counter("risk.decisions", "decision", verdict.name()).increment();
        hits.forEach(hit -> meterRegistry.counter("risk.rule.hits", "rule", hit.getRule()).increment());

        logDecision(decision, txn);
        context.forward(record.withValue(decision));
    }

    private List<KeyValue<Long, VelocityEntry>> recentAttempts(String cardId, long eventTime) {
        // The longest lookback is card testing's. Velocity filters this down further,
        // so both rules share a single range scan of the store.
        long from = eventTime - RiskRules.CARD_TESTING_WINDOW.toMillis();
        List<KeyValue<Long, VelocityEntry>> recent = new ArrayList<>();
        try (WindowStoreIterator<VelocityEntry> it = velocityStore.fetch(cardId, from, eventTime)) {
            it.forEachRemaining(recent::add);
        }
        return recent;
    }

    private static void checkVelocity(List<KeyValue<Long, VelocityEntry>> recent, long eventTime, List<RuleHit> hits) {
        long from = eventTime - RiskRules.VELOCITY_WINDOW.toMillis();
        long attempts = recent.stream().filter(kv -> kv.key >= from).count();
        if (attempts > RiskRules.VELOCITY_MAX_ATTEMPTS) {
            hits.add(Rule.VELOCITY.hit("%d attempts in %ds (max %d)".formatted(
                    attempts, RiskRules.VELOCITY_WINDOW.toSeconds(), RiskRules.VELOCITY_MAX_ATTEMPTS)));
        }
    }

    private static void checkCardTesting(List<KeyValue<Long, VelocityEntry>> recent, List<RuleHit> hits) {
        List<VelocityEntry> small = recent.stream()
                .map(kv -> kv.value)
                .filter(v -> v.getAmountMinor() < RiskRules.CARD_TESTING_MAX_AMOUNT_MINOR)
                .toList();
        Set<String> merchants = new HashSet<>();
        small.forEach(v -> merchants.add(v.getMerchantId()));

        if (small.size() >= RiskRules.CARD_TESTING_MIN_ATTEMPTS
                && merchants.size() >= RiskRules.CARD_TESTING_MIN_MERCHANTS) {
            hits.add(Rule.CARD_TESTING.hit("%d attempts under %s across %d merchants in %d min".formatted(
                    small.size(), "EUR 2.00", merchants.size(), RiskRules.CARD_TESTING_WINDOW.toMinutes())));
        }
    }

    private void checkImpossibleTravel(LastSeen last, Transaction txn, Merchant merchant,
                                       long eventTime, List<RuleHit> hits) {
        if (last == null) {
            return;
        }
        double km = Geo.distanceKm(last.getLat(), last.getLon(), txn.getLat(), txn.getLon());
        if (km < RiskRules.GEO_MIN_DISTANCE_KM) {
            return;
        }
        // abs(): an out-of-order record is still two places at two times.
        long elapsed = Math.abs(eventTime - last.getSeenAt().toEpochMilli());
        double kmh = Geo.impliedKmh(km, elapsed);
        if (kmh > RiskRules.GEO_MAX_KMH) {
            String from = last.getPlace() != null ? last.getPlace() : "last location";
            String to = merchant != null ? merchant.getCountry() : "here";
            String detail = "%.0f km from %s to %s in %s implies %.0f km/h".formatted(
                    km, from, to, human(Duration.ofMillis(elapsed)), kmh);
            // The same impossible speed means different things at different distances: across
            // a continent it can only be two people; across a region it may be a merchant
            // registered 150 km from where the card was really used. The evaluation found the
            // regional case made up most of this rule's false declines.
            if (km >= policy.geoCertainDistanceKm()) {
                hits.add(Rule.GEO_VELOCITY.hit(detail));
            } else {
                hits.add(Rule.GEO_SHORT_HOP.hit(detail, policy.geoShortHopScore()));
            }
        }
    }

    /**
     * Wall-clock punctuator: drops locations older than the TTL and spend totals from before
     * yesterday. A full scan, which is fine at this scale; with millions of cards per task
     * you would keep a secondary index ordered by time and scan only the expired prefix.
     */
    private void evictStaleState(long wallClockMillis) {
        long geoCutoff = wallClockMillis - RiskRules.GEO_STATE_TTL.toMillis();
        List<String> staleLocations = new ArrayList<>();
        try (KeyValueIterator<String, LastSeen> it = geoStore.all()) {
            it.forEachRemaining(kv -> {
                if (kv.value.getSeenAt().toEpochMilli() < geoCutoff) {
                    staleLocations.add(kv.key);
                }
            });
        }
        // Collect, then delete: never mutate a store while iterating over it.
        staleLocations.forEach(geoStore::delete);

        long yesterday = Math.floorDiv(wallClockMillis, DAY_MILLIS) - 1;
        List<String> staleSpend = new ArrayList<>();
        try (KeyValueIterator<String, DailySpend> it = spendStore.all()) {
            it.forEachRemaining(kv -> {
                if (kv.value.getEpochDay() < yesterday) {
                    staleSpend.add(kv.key);
                }
            });
        }
        staleSpend.forEach(spendStore::delete);

        if (!staleLocations.isEmpty() || !staleSpend.isEmpty()) {
            log.info("evicted {} stale locations and {} stale spend totals", staleLocations.size(), staleSpend.size());
        }
    }

    private static void logDecision(Decision decision, Transaction txn) {
        if (decision.getDecision() == DecisionType.APPROVE && decision.getReasons().isEmpty()) {
            log.debug("APPROVE {} {}", decision.getCardId(), money(decision.getAmountMinor(), txn));
            return;
        }
        String reasons = decision.getReasons().stream()
                .map(h -> h.getRule() + " (" + h.getDetail() + ")")
                .collect(Collectors.joining("; "));
        log.info("{} {} {} score={} - {}", decision.getDecision(), decision.getCardId(),
                money(decision.getAmountMinor(), txn), decision.getScore(), reasons);
    }

    private static String money(long minor, Transaction txn) {
        return "%s %d.%02d".formatted(txn.getCurrency(), minor / 100, Math.abs(minor % 100));
    }

    private static String human(Duration d) {
        long minutes = d.toMinutes();
        long seconds = d.toSecondsPart();
        return minutes > 0 ? "%dm %ds".formatted(minutes, seconds) : "%ds".formatted(seconds);
    }
}
