package com.kaushal.riskengine.evaluation;

import com.kaushal.riskengine.avro.Decision;
import com.kaushal.riskengine.decision.RiskPolicy;
import com.kaushal.trafficgen.SparkovMapping;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Runs the labelled dataset through the offline engine and scores every decision.
 *
 * <p>All files go through <em>one</em> engine, in order - train, then test - so state carries
 * across exactly as it would in production: a card's velocity window and last location don't
 * reset because the file changed. Each file is also scored separately. The rules aren't
 * trained on anything, so today the split is just two time periods. It becomes the guard
 * against overfitting the moment a threshold is tuned: tune on train, report on test.
 */
public final class Evaluation {

    public record Result(Map<String, Scorecard> byFile, Scorecard overall, long rows, Duration elapsed,
                         int cards, int merchants, RiskPolicy policy) {
    }

    private static final int PROGRESS_EVERY = 25_000;

    private final long seed;
    private final long limit;
    private final Consumer<String> progress;
    private final RiskPolicy policy;

    public Evaluation(long seed, long limit, Consumer<String> progress, RiskPolicy policy) {
        this.seed = seed;
        this.limit = limit;
        this.progress = progress;
        this.policy = policy;
    }

    public Result run(List<Path> files, Path stateDir) throws IOException {
        long started = System.nanoTime();
        try (OfflineEngine engine = new OfflineEngine(stateDir, policy)) {
            int[] reference = seedReferenceData(engine, files);

            Map<String, Scorecard> byFile = new LinkedHashMap<>();
            Scorecard overall = new Scorecard();
            long rows = 0;
            long scoringStarted = System.nanoTime();
            progress.accept("  scoring transactions (progress every %,d)...".formatted(PROGRESS_EVERY));

            outer:
            for (Path file : files) {
                Scorecard period = byFile.computeIfAbsent(label(file), f -> new Scorecard());
                try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
                     CSVParser parser = SparkovMapping.csvFormat().parse(reader)) {
                    for (CSVRecord record : parser) {
                        Decision decision = engine.decide(
                                SparkovMapping.transaction(record, SparkovMapping.eventMillis(record)));
                        period.record(decision);
                        overall.record(decision);

                        if (++rows % PROGRESS_EVERY == 0) {
                            double seconds = (System.nanoTime() - scoringStarted) / 1e9;
                            progress.accept("  %,d scored  (%,.0f/s, now in %s)".formatted(
                                    rows, rows / seconds, label(file)));
                        }
                        if (limit > 0 && rows >= limit) {
                            break outer;
                        }
                    }
                }
            }
            return new Result(byFile, overall, rows, Duration.ofNanos(System.nanoTime() - started),
                    reference[0], reference[1], policy);
        }
    }

    /** Every card, customer and merchant the transactions will reference, before any of them. */
    private int[] seedReferenceData(OfflineEngine engine, List<Path> files) throws IOException {
        Set<String> cards = new HashSet<>();
        Set<String> merchants = new HashSet<>();
        long read = 0;

        outer:
        for (Path file : files) {
            try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
                 CSVParser parser = SparkovMapping.csvFormat().parse(reader)) {
                for (CSVRecord record : parser) {
                    if (cards.add(SparkovMapping.cardId(record))) {
                        SparkovMapping.Cardholder holder = SparkovMapping.cardholder(record, seed);
                        engine.seed(holder.customer(), holder.card());
                    }
                    if (merchants.add(SparkovMapping.merchantId(record))) {
                        engine.seed(SparkovMapping.merchant(record));
                    }
                    if (limit > 0 && ++read >= limit) {
                        break outer;
                    }
                }
            }
        }
        progress.accept("  seeded %,d cards and %,d merchants".formatted(cards.size(), merchants.size()));
        return new int[] {cards.size(), merchants.size()};
    }

    static String label(Path file) {
        String name = file.getFileName().toString();
        return name.endsWith(".csv") ? name.substring(0, name.length() - 4) : name;
    }
}
