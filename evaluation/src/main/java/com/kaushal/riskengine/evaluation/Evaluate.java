package com.kaushal.riskengine.evaluation;

import com.kaushal.trafficgen.SparkovMapping;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Entry point.
 *
 * <pre>
 *   ./gradlew :evaluation:run
 *   ./gradlew :evaluation:run --args="--limit=100000"
 *   ./gradlew :evaluation:run --args="--data-dir=data --out=docs/EVALUATION.md"
 * </pre>
 */
public final class Evaluate {

    private Evaluate() {
    }

    public static void main(String[] args) throws IOException {
        Map<String, String> flags = parse(args);
        Path dataDir = Path.of(flags.getOrDefault("data-dir", "data"));
        Path out = Path.of(flags.getOrDefault("out", "docs/EVALUATION.md"));
        long limit = Long.parseLong(flags.getOrDefault("limit", "0"));
        long seed = Long.parseLong(flags.getOrDefault("seed", "42"));

        List<Path> files = SparkovMapping.locateCsvs(dataDir);
        System.out.println("evaluating " + files.stream().map(Evaluation::label).collect(Collectors.joining(", "))
                + (limit > 0 ? " (first %,d transactions)".formatted(limit) : ""));

        // Under build/ rather than the OS temp folder, which Kafka Streams warns about because
        // the OS may clear it mid-run. Deleted again at the end either way.
        Path buildDir = Files.createDirectories(Path.of("build"));
        Path stateDir = Files.createTempDirectory(buildDir, "evaluation-state-");
        try {
            Evaluation.Result result = new Evaluation(seed, limit, System.out::println).run(files, stateDir);

            String source = "[Sparkov credit card transactions](https://www.kaggle.com/datasets/kartik2112/fraud-detection) ("
                    + files.stream().map(f -> "`" + f.getFileName() + "`").collect(Collectors.joining(", ")) + ")";
            Files.createDirectories(out.toAbsolutePath().getParent());
            Files.writeString(out, ReportWriter.markdown(result, source), StandardCharsets.UTF_8);

            printSummary(result);
            System.out.println("report written to " + out);
        } finally {
            deleteRecursively(stateDir);
        }
    }

    private static void printSummary(Evaluation.Result r) {
        Scorecard all = r.overall();
        System.out.printf("%n%,d transactions, %,d fraudulent (%s), in %ds%n",
                r.rows(), all.fraud(), ReportWriter.pct(all.baseRate()), r.elapsed().toSeconds());
        System.out.printf("declined:  precision %s  recall %s  false-positive rate %s%n",
                ReportWriter.pct(all.declined().precision()), ReportWriter.pct(all.declined().recall()),
                ReportWriter.pct(all.declined().falsePositiveRate()));
        System.out.printf("flagged:   precision %s  recall %s  false-positive rate %s%n",
                ReportWriter.pct(all.flagged().precision()), ReportWriter.pct(all.flagged().recall()),
                ReportWriter.pct(all.flagged().falsePositiveRate()));
        System.out.printf("cards:     %d of %d fraud-hit cards caught%n", all.fraudCardsCaught(), all.fraudCards());
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> flags = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--") && arg.contains("=")) {
                flags.put(arg.substring(2, arg.indexOf('=')), arg.substring(arg.indexOf('=') + 1));
            }
        }
        return flags;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }
}
