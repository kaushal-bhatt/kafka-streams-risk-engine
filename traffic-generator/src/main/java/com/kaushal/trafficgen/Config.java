package com.kaushal.trafficgen;

import java.util.HashMap;
import java.util.Map;

/**
 * Settings parsed from {@code --key=value} arguments, falling back to environment
 * variables and then to defaults that match docker-compose.
 */
public record Config(
        String bootstrapServers,
        String schemaRegistryUrl,
        long seed,
        double speed,
        String dataDir,
        int limit
) {

    public static Config parse(String[] args) {
        Map<String, String> flags = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                int eq = arg.indexOf('=');
                if (eq > 0) {
                    flags.put(arg.substring(2, eq), arg.substring(eq + 1));
                } else {
                    flags.put(arg.substring(2), "true");
                }
            }
        }

        return new Config(
                value(flags, "bootstrap-servers", "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                value(flags, "schema-registry", "SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                // Everything random in this generator is seeded, so a demo run produces the
                // same decisions every time. A recruiter watching the dashboard should see
                // the same story you saw when you recorded the GIF.
                Long.parseLong(value(flags, "seed", "TRAFFIC_SEED", "42")),
                Double.parseDouble(value(flags, "speed", "TRAFFIC_SPEED", "1")),
                value(flags, "data-dir", "DATA_DIR", "data"),
                Integer.parseInt(value(flags, "limit", "TRAFFIC_LIMIT", "0"))
        );
    }

    private static String value(Map<String, String> flags, String flag, String env, String fallback) {
        if (flags.containsKey(flag)) {
            return flags.get(flag);
        }
        String fromEnv = System.getenv(env);
        return fromEnv != null ? fromEnv : fallback;
    }

    /** Wall-clock milliseconds to wait for a gap of {@code eventMillis} in event time. */
    public long pauseFor(long eventMillis) {
        if (speed <= 0) {
            return 0;
        }
        return Math.max(0, Math.round(eventMillis / speed));
    }
}
