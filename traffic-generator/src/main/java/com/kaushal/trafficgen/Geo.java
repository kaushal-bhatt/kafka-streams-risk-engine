package com.kaushal.trafficgen;

/**
 * Great-circle distance. The same formula the engine's geo-velocity rule uses, kept here
 * so the generator can assert that a scenario really does imply an impossible speed.
 */
public final class Geo {

    private static final double EARTH_RADIUS_KM = 6371.0;

    private Geo() {
    }

    public static double distanceKm(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    /** Implied travel speed in km/h between two points separated by {@code millis}. */
    public static double impliedKmh(double km, long millis) {
        if (millis <= 0) {
            return Double.POSITIVE_INFINITY;
        }
        return km / (millis / 3_600_000.0);
    }
}
