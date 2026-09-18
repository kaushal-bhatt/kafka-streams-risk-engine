package com.kaushal.riskengine.decision;

/**
 * The tunable part of the scoring - kept separate from {@link RiskRules}' fixed thresholds so
 * that the offline evaluation can compare variants without editing code.
 *
 * <p>Only knobs that an evaluation has actually been run on belong here. Adding one is a claim
 * that it has been measured.
 *
 * @param geoCertainDistanceKm impossible travel over at least this distance is certain, and
 *                             declines ({@code GEO_VELOCITY}). Below it - but above
 *                             {@link RiskRules#GEO_MIN_DISTANCE_KM} - it is a
 *                             {@code GEO_SHORT_HOP}: location data is noisy at that scale.
 * @param geoShortHopScore     score for a {@code GEO_SHORT_HOP}
 */
public record RiskPolicy(double geoCertainDistanceKm, int geoShortHopScore) {

    /**
     * Stage 2's original behaviour: every impossible-travel hit over 100 km declines. With the
     * certain distance equal to the minimum distance, {@code GEO_SHORT_HOP} can never fire.
     * Kept so the baseline evaluation stays reproducible.
     */
    public static final RiskPolicy ORIGINAL = new RiskPolicy(RiskRules.GEO_MIN_DISTANCE_KM, 0);

    /** What the engine runs. */
    public static final RiskPolicy DEFAULT = ORIGINAL;

    public RiskPolicy {
        if (geoCertainDistanceKm < RiskRules.GEO_MIN_DISTANCE_KM) {
            throw new IllegalArgumentException("geoCertainDistanceKm must be at least "
                    + RiskRules.GEO_MIN_DISTANCE_KM + " km");
        }
        if (geoShortHopScore < 0 || geoShortHopScore > RiskRules.MAX_SCORE) {
            throw new IllegalArgumentException("geoShortHopScore must be 0.." + RiskRules.MAX_SCORE);
        }
    }

    @Override
    public String toString() {
        return "geoCertainDistanceKm=%.0f, geoShortHopScore=%d".formatted(geoCertainDistanceKm, geoShortHopScore);
    }
}
