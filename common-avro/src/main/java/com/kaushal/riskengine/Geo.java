package com.kaushal.riskengine;

/**
 * Great-circle distance, shared by the engine's impossible-travel rule and the traffic
 * generator, so the speed the generator prints is the speed the engine computes.
 */
public final class Geo {

    private static final double EARTH_RADIUS_KM = 6371.0;

    private Geo() {
    }

    /** Haversine distance in kilometres. */
    public static double distanceKm(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    /** Implied travel speed in km/h. Zero elapsed time over a real distance is infinite. */
    public static double impliedKmh(double km, long millis) {
        if (millis <= 0) {
            return km > 0 ? Double.POSITIVE_INFINITY : 0;
        }
        return km / (millis / 3_600_000.0);
    }
}
