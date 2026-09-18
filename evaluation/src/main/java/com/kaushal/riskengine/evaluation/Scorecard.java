package com.kaushal.riskengine.evaluation;

import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.DecisionType;
import com.kaushal.riskengine.avro.RuleHit;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tallies the engine's decisions against ground truth.
 *
 * <p>The label comes from the {@link Decision} itself ({@code labelledFraud}), which the
 * transaction carried through enrichment and the RiskEvaluator. So what's scored is exactly
 * what the engine emitted, not a separate re-computation of what it "should" have done.
 *
 * <p>Two operating points are scored, because they answer different questions:
 * <ul>
 *   <li><b>declined</b> - blocked outright. The customer-facing cost of a false positive is a
 *       failed purchase.</li>
 *   <li><b>flagged</b> - REVIEW or DECLINE. What a fraud team would see in its queue.</li>
 * </ul>
 */
public class Scorecard {

    /** One confusion matrix plus the money it represents. */
    public static final class Confusion {
        long truePositives;
        long falsePositives;
        long falseNegatives;
        long trueNegatives;
        long fraudMinorCaught;

        void add(boolean predicted, boolean fraud, long amountMinor) {
            if (predicted && fraud) {
                truePositives++;
                fraudMinorCaught += amountMinor;
            } else if (predicted) {
                falsePositives++;
            } else if (fraud) {
                falseNegatives++;
            } else {
                trueNegatives++;
            }
        }

        public long truePositives() { return truePositives; }
        public long falsePositives() { return falsePositives; }
        public long falseNegatives() { return falseNegatives; }
        public long trueNegatives() { return trueNegatives; }
        public long fraudMinorCaught() { return fraudMinorCaught; }

        public double precision() {
            return ratio(truePositives, truePositives + falsePositives);
        }

        public double recall() {
            return ratio(truePositives, truePositives + falseNegatives);
        }

        public double f1() {
            double p = precision();
            double r = recall();
            return p + r == 0 ? 0 : 2 * p * r / (p + r);
        }

        /** Share of legitimate transactions wrongly caught - the cardholder-annoyance number. */
        public double falsePositiveRate() {
            return ratio(falsePositives, falsePositives + trueNegatives);
        }
    }

    /** How one rule behaves, in isolation from the others. */
    public static final class RuleStats {
        long hits;
        long fraudHits;

        public long hits() { return hits; }
        public long fraudHits() { return fraudHits; }
    }

    /** One card's fraud episode: how many fraudulent transactions before the first catch. */
    static final class Episode {
        int fraudSeen;
        int firstCaughtAt = -1;
    }

    private long transactions;
    private long fraud;
    private long fraudMinorTotal;
    private final Map<DecisionType, Long> byDecision = new TreeMap<>();
    private final Confusion declined = new Confusion();
    private final Confusion flagged = new Confusion();
    private final Map<String, RuleStats> rules = new TreeMap<>();
    private final Map<String, Episode> fraudCards = new HashMap<>();

    public void record(Decision decision) {
        boolean isFraud = Boolean.TRUE.equals(decision.getLabelledFraud());
        DecisionType verdict = decision.getDecision();
        long amount = decision.getAmountMinor();

        transactions++;
        byDecision.merge(verdict, 1L, Long::sum);
        if (isFraud) {
            fraud++;
            fraudMinorTotal += amount;
        }

        declined.add(verdict == DecisionType.DECLINE, isFraud, amount);
        flagged.add(verdict != DecisionType.APPROVE, isFraud, amount);

        for (RuleHit hit : decision.getReasons()) {
            RuleStats stats = rules.computeIfAbsent(hit.getRule(), r -> new RuleStats());
            stats.hits++;
            if (isFraud) {
                stats.fraudHits++;
            }
        }

        if (isFraud) {
            Episode episode = fraudCards.computeIfAbsent(decision.getCardId(), c -> new Episode());
            episode.fraudSeen++;
            if (verdict != DecisionType.APPROVE && episode.firstCaughtAt < 0) {
                episode.firstCaughtAt = episode.fraudSeen;
            }
        }
    }

    public long transactions() { return transactions; }
    public long fraud() { return fraud; }
    public long fraudMinorTotal() { return fraudMinorTotal; }
    public Map<DecisionType, Long> byDecision() { return byDecision; }
    public Confusion declined() { return declined; }
    public Confusion flagged() { return flagged; }
    public Map<String, RuleStats> rules() { return rules; }

    public double baseRate() {
        return ratio(fraud, transactions);
    }

    public int fraudCards() {
        return fraudCards.size();
    }

    /** Cards on which at least one fraudulent transaction was flagged (REVIEW or DECLINE). */
    public int fraudCardsCaught() {
        return (int) fraudCards.values().stream().filter(e -> e.firstCaughtAt > 0).count();
    }

    /**
     * Median number of fraudulent transactions that got through on a card before the engine
     * first flagged one, across the cards it did catch. 0 means it caught the first one.
     */
    public double medianFraudsBeforeCatch() {
        int[] before = fraudCards.values().stream()
                .filter(e -> e.firstCaughtAt > 0)
                .mapToInt(e -> e.firstCaughtAt - 1)
                .sorted()
                .toArray();
        if (before.length == 0) {
            return Double.NaN;
        }
        int mid = before.length / 2;
        return before.length % 2 == 1 ? before[mid] : (before[mid - 1] + before[mid]) / 2.0;
    }

    static double ratio(long numerator, long denominator) {
        return denominator == 0 ? 0 : (double) numerator / denominator;
    }
}
