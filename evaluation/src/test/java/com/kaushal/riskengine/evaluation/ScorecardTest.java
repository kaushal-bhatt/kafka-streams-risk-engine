package com.kaushal.riskengine.evaluation;

import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.avro.DecisionType;
import com.kaushal.riskengine.avro.RuleHit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ScorecardTest {

    @Test
    @DisplayName("precision, recall and false-positive rate at both operating points")
    void confusionAtBothOperatingPoints() {
        Scorecard s = new Scorecard();
        s.record(decision("C1", DecisionType.DECLINE, true, 10_000, "GEO_VELOCITY"));  // TP both
        s.record(decision("C1", DecisionType.REVIEW, true, 5_000, "VELOCITY"));        // TP flagged, FN declined
        s.record(decision("C2", DecisionType.APPROVE, true, 2_000));                   // FN both
        s.record(decision("C3", DecisionType.DECLINE, false, 1_000, "DAILY_LIMIT"));   // FP both
        s.record(decision("C4", DecisionType.APPROVE, false, 1_000));                  // TN both
        s.record(decision("C4", DecisionType.APPROVE, false, 1_000));                  // TN both

        assertThat(s.transactions()).isEqualTo(6);
        assertThat(s.fraud()).isEqualTo(3);
        assertThat(s.fraudMinorTotal()).isEqualTo(17_000);

        // Declined: TP 1, FP 1, FN 2, TN 2
        assertThat(s.declined().precision()).isCloseTo(0.5, within(1e-9));
        assertThat(s.declined().recall()).isCloseTo(1.0 / 3, within(1e-9));
        assertThat(s.declined().falsePositiveRate()).isCloseTo(1.0 / 3, within(1e-9));
        assertThat(s.declined().fraudMinorCaught()).isEqualTo(10_000);

        // Flagged: TP 2, FP 1, FN 1, TN 2
        assertThat(s.flagged().precision()).isCloseTo(2.0 / 3, within(1e-9));
        assertThat(s.flagged().recall()).isCloseTo(2.0 / 3, within(1e-9));
        assertThat(s.flagged().fraudMinorCaught()).isEqualTo(15_000);
    }

    @Test
    @DisplayName("rules are scored on what they fired on, independently of the final verdict")
    void perRuleStats() {
        Scorecard s = new Scorecard();
        s.record(decision("C1", DecisionType.REVIEW, true, 100, "VELOCITY", "CARD_TESTING"));
        s.record(decision("C2", DecisionType.APPROVE, false, 100, "MERCHANT_RISK"));
        s.record(decision("C3", DecisionType.APPROVE, true, 100, "MERCHANT_RISK"));

        assertThat(s.rules().get("MERCHANT_RISK").hits()).isEqualTo(2);
        assertThat(s.rules().get("MERCHANT_RISK").fraudHits()).isEqualTo(1);
        assertThat(s.rules().get("CARD_TESTING").fraudHits()).isEqualTo(1);
    }

    @Test
    @DisplayName("card episodes: caught cards, and how many frauds slipped through first")
    void cardEpisodes() {
        Scorecard s = new Scorecard();
        // Card A: two frauds approved, caught on the third -> 2 before the catch.
        s.record(decision("A", DecisionType.APPROVE, true, 100));
        s.record(decision("A", DecisionType.APPROVE, true, 100));
        s.record(decision("A", DecisionType.REVIEW, true, 100, "VELOCITY"));
        // Card B: caught on its first fraud -> 0.
        s.record(decision("B", DecisionType.DECLINE, true, 100, "GEO_VELOCITY"));
        // Card C: never caught.
        s.record(decision("C", DecisionType.APPROVE, true, 100));
        // Card D: legitimate only - not a fraud card at all.
        s.record(decision("D", DecisionType.APPROVE, false, 100));

        assertThat(s.fraudCards()).isEqualTo(3);
        assertThat(s.fraudCardsCaught()).isEqualTo(2);
        assertThat(s.medianFraudsBeforeCatch()).isEqualTo(1.0); // median of {0, 2}
    }

    private static Decision decision(String card, DecisionType type, boolean fraud, long amount, String... rules) {
        List<RuleHit> hits = java.util.Arrays.stream(rules)
                .map(r -> RuleHit.newBuilder().setRule(r).setScore(10).setDetail("test").build())
                .toList();
        return Decision.newBuilder()
                .setTransactionId(UUID.randomUUID().toString())
                .setCardId(card)
                .setMerchantId("M")
                .setDecision(type)
                .setScore(0)
                .setReasons(hits)
                .setAmountMinor(amount)
                .setEvaluatedAt(Instant.EPOCH)
                .setLabelledFraud(fraud)
                .build();
    }
}
