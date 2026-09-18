package com.kaushal.riskengine.decision;

import com.kaushal.riskengine.avro.RuleHit;

/**
 * Every rule that can contribute to a decision, with the score it adds.
 *
 * <p>Scores are additive and capped at 100. The bands in {@link RiskRules#band(int)} turn
 * the total into APPROVE / REVIEW / DECLINE. A score of 80 on its own is deliberately a
 * decline: those rules describe something no legitimate cardholder does.
 */
public enum Rule {

    /** More authorisations in a short window than a person makes. */
    VELOCITY(45),

    /** Many tiny amounts across several merchants: a bot checking which stolen card
        numbers are live before using them for real. */
    CARD_TESTING(60),

    /** This transaction would take the card past its daily limit. */
    DAILY_LIMIT(80),

    /** Implied travel speed since the card's last location is faster than a plane. */
    GEO_VELOCITY(80),

    /** Merchant category where fraud concentrates (gambling, quasi-cash, card-not-present). */
    MERCHANT_RISK(20),

    /** The card isn't in the reference data at all. Review, don't guess. */
    UNKNOWN_CARD(50),

    /** Blocked or expired card. Always a decline. */
    CARD_NOT_ACTIVE(100),

    /** High-risk customers are escalated sooner - but only once another rule has fired.
        On its own this adds nothing, so a HIGH-tier customer's ordinary purchases approve. */
    CUSTOMER_RISK_TIER(10);

    private final int score;

    Rule(int score) {
        this.score = score;
    }

    public int score() {
        return score;
    }

    public RuleHit hit(String detail) {
        return RuleHit.newBuilder()
                .setRule(name())
                .setScore(score)
                .setDetail(detail)
                .build();
    }
}
