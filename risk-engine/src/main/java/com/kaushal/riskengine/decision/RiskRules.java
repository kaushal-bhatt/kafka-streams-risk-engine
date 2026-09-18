package com.kaushal.riskengine.decision;

import com.kaushal.riskengine.avro.DecisionType;

import java.time.Duration;
import java.util.Set;

/**
 * Thresholds for every rule, in one place.
 *
 * <p>These are starting values chosen to make the scripted scenarios unambiguous. Stage 7
 * measures them against 1.85M labelled transactions; expect them to move then.
 */
public final class RiskRules {

    private RiskRules() {
    }

    // --- Velocity ---------------------------------------------------------------------
    public static final Duration VELOCITY_WINDOW = Duration.ofSeconds(60);
    /** Fires when attempts in the window, including this one, exceed this. */
    public static final int VELOCITY_MAX_ATTEMPTS = 5;

    // --- Card testing -----------------------------------------------------------------
    public static final Duration CARD_TESTING_WINDOW = Duration.ofMinutes(10);
    /** EUR 2.00. Card testers use amounts too small for the cardholder to notice. */
    public static final long CARD_TESTING_MAX_AMOUNT_MINOR = 200;
    public static final int CARD_TESTING_MIN_ATTEMPTS = 4;
    /** Spread across merchants, so no single merchant sees the pattern. */
    public static final int CARD_TESTING_MIN_MERCHANTS = 3;

    // --- Impossible travel ------------------------------------------------------------
    /** Faster than a commercial flight. */
    public static final double GEO_MAX_KMH = 900;
    /**
     * Distances below this never count, however short the time gap. Without it, two shops
     * 5 km apart used 10 seconds apart imply 1,800 km/h - a false positive on every busy
     * high street. GPS and merchant-location noise live well inside 100 km.
     */
    public static final double GEO_MIN_DISTANCE_KM = 100;

    // --- Merchant risk ----------------------------------------------------------------
    /**
     * ISO 18245 MCCs where fraud concentrates - 7995 gambling, 6051 quasi-cash and crypto,
     * 4829 money transfer - plus the Sparkov dataset's card-not-present categories, which
     * that dataset uses in place of MCCs.
     */
    public static final Set<String> HIGH_RISK_CATEGORIES = Set.of(
            "7995", "6051", "4829",
            "shopping_net", "misc_net"
    );

    // --- Decision bands ---------------------------------------------------------------
    public static final int REVIEW_FROM = 40;
    public static final int DECLINE_FROM = 71;
    public static final int MAX_SCORE = 100;

    // --- State retention --------------------------------------------------------------
    /** Must cover the longest lookback (card testing, 10 minutes) with room to spare. */
    public static final Duration VELOCITY_RETENTION = Duration.ofHours(1);
    /** A location older than this says nothing about where the card is now. */
    public static final Duration GEO_STATE_TTL = Duration.ofDays(7);
    public static final Duration EVICTION_INTERVAL = Duration.ofHours(1);

    public static DecisionType band(int score) {
        if (score >= DECLINE_FROM) {
            return DecisionType.DECLINE;
        }
        if (score >= REVIEW_FROM) {
            return DecisionType.REVIEW;
        }
        return DecisionType.APPROVE;
    }
}
