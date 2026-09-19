package com.kaushal.trafficgen;

import java.util.Arrays;

/**
 * Entry point.
 *
 * <pre>
 *   ./gradlew :traffic-generator:run --args="seed"
 *   ./gradlew :traffic-generator:run --args="baseline --limit=500"
 *   ./gradlew :traffic-generator:run --args="card-testing"
 *   ./gradlew :traffic-generator:run --args="impossible-travel --speed=20"
 *   ./gradlew :traffic-generator:run --args="limit-breach --speed=20"
 *   ./gradlew :traffic-generator:run --args="replay --speed=1000 --limit=50000"
 * </pre>
 */
public final class TrafficGenerator {

    private static final int CUSTOMERS = 500;
    private static final int MERCHANTS = 60;

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || "--help".equals(args[0])) {
            usage();
            return;
        }

        String scenario = args[0];
        Config config = Config.parse(Arrays.copyOfRange(args, 1, args.length));

        System.out.printf("scenario=%s bootstrap=%s registry=%s seed=%d speed=%s%n",
                scenario, config.bootstrapServers(), config.schemaRegistryUrl(),
                config.seed(), config.speed());

        ReferenceData.Seeded reference = ReferenceData.generate(config.seed(), CUSTOMERS, MERCHANTS);

        try (Publisher publisher = new Publisher(config)) {
            Scenarios scenarios = new Scenarios(config, publisher, reference);

            switch (scenario) {
                case "seed" -> scenarios.seed();
                case "baseline" -> {
                    scenarios.seed();
                    scenarios.baseline(config.limit() > 0 ? config.limit() : 500);
                }
                case "card-testing" -> {
                    scenarios.seed();
                    scenarios.cardTesting();
                }
                case "impossible-travel" -> {
                    scenarios.seed();
                    scenarios.impossibleTravel();
                }
                case "limit-breach" -> {
                    scenarios.seed();
                    scenarios.limitBreach();
                }
                case "latency" -> {
                    scenarios.seed();
                    scenarios.latency(config.limit() > 0 ? config.limit() : 200);
                }
                // The replay brings its own cards and merchants, so it does not seed the
                // synthetic reference data. Stage 5 loads Sparkov's customer and merchant
                // files into Postgres and lets Debezium carry them in.
                case "replay" -> new SparkovReplay(config, publisher).run();
                default -> {
                    System.err.println("unknown scenario: " + scenario);
                    usage();
                    System.exit(2);
                }
            }
        }
    }

    private static void usage() {
        System.out.println("""
                Usage: traffic-generator <scenario> [flags]

                Scenarios
                  seed               publish cards, customers and merchants only
                  baseline           ordinary traffic that should trip no rules
                  card-testing       20 sub-EUR-2 authorisations across 8 merchants
                  impossible-travel  Berlin, then Sao Paulo four minutes later
                  limit-breach       walk a card up to and past its daily limit
                  latency            time send -> decision readable, 20/s (--limit=N, default 200)
                  replay             replay the labelled Sparkov dataset (CSVs in data/)

                Flags
                  --bootstrap-servers=localhost:9092
                  --schema-registry=http://localhost:8081
                  --seed=42          everything random is seeded, so runs repeat exactly
                  --speed=1          event-time multiplier; 1000 replays an hour in 3.6s
                  --limit=0          stop after N records (0 = no limit)
                  --data-dir=data    where the Sparkov CSVs live
                """);
    }
}
