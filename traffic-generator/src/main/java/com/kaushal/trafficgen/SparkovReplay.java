package com.kaushal.trafficgen;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Transaction;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Replays the Sparkov "Credit Card Transactions Fraud Detection Dataset" into the live engine.
 *
 * <p>See {@code docs/DATA.md}. Field mapping lives in {@link SparkovMapping}, shared with the
 * offline evaluation.
 *
 * <p>The replay runs in two passes:
 * <ol>
 *   <li>Derive cards, customers and merchants from the CSV and publish them to the
 *       compacted reference topics, so the enrichment join knows every Sparkov card.</li>
 *   <li>Stream the transactions, in event-time order, shifted to the present.</li>
 * </ol>
 * Stage 5 moves the first pass into Postgres, with Debezium carrying it into Kafka.
 */
public class SparkovReplay {

    /** Time for the card-customer foreign-key join to materialise before transactions start. */
    private static final long REFERENCE_SETTLE_MILLIS = 5_000;

    private final Config config;
    private final Publisher publisher;

    public SparkovReplay(Config config, Publisher publisher) {
        this.config = config;
        this.publisher = publisher;
    }

    public void run() throws IOException, InterruptedException {
        List<Path> files = SparkovMapping.locateCsvs(Path.of(config.dataDir()));
        System.out.println("replaying " + files);

        seedReferenceData(files);
        replayTransactions(files);
    }

    private void seedReferenceData(List<Path> files) throws IOException, InterruptedException {
        Set<String> cardsSeen = new HashSet<>();
        Set<String> merchantsSeen = new HashSet<>();
        long read = 0;

        outer:
        for (Path file : files) {
            try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
                 CSVParser parser = SparkovMapping.csvFormat().parse(reader)) {
                for (CSVRecord record : parser) {
                    if (cardsSeen.add(SparkovMapping.cardId(record))) {
                        SparkovMapping.Cardholder holder = SparkovMapping.cardholder(record, config.seed());
                        publisher.send(Topics.CUSTOMERS, holder.customer().getCustomerId(), holder.customer());
                        publisher.send(Topics.CARDS, holder.card().getCardId(), holder.card());
                    }
                    if (merchantsSeen.add(SparkovMapping.merchantId(record))) {
                        publisher.send(Topics.MERCHANTS, SparkovMapping.merchantId(record), SparkovMapping.merchant(record));
                    }
                    // Only seed what the replay will actually reach.
                    if (config.limit() > 0 && ++read >= config.limit()) {
                        break outer;
                    }
                }
            }
        }

        publisher.flush();
        System.out.printf("seeded %,d cards and %,d merchants from the dataset%n",
                cardsSeen.size(), merchantsSeen.size());

        // The card profile is produced by a foreign-key join, which runs through internal
        // topics. Give it a moment so the first transaction on each card finds its profile.
        Thread.sleep(REFERENCE_SETTLE_MILLIS);
    }

    private void replayTransactions(List<Path> files) throws IOException, InterruptedException {
        long sent = 0;
        long previousEventMillis = -1;
        // The dataset's timestamps are years old. Everything is shifted by one constant offset
        // so it lands in the present: retention and state store TTLs are relative to now, and
        // old event times make every window behave strangely. A constant shift preserves
        // event-time ordering and the gaps between events.
        long shiftMillis = Long.MIN_VALUE;

        outer:
        for (Path file : files) {
            try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
                 CSVParser parser = SparkovMapping.csvFormat().parse(reader)) {

                for (CSVRecord record : parser) {
                    long eventMillis = SparkovMapping.eventMillis(record);

                    if (shiftMillis == Long.MIN_VALUE) {
                        shiftMillis = Instant.now().toEpochMilli() - eventMillis;
                    }
                    if (previousEventMillis > 0 && eventMillis > previousEventMillis) {
                        Thread.sleep(config.pauseFor(eventMillis - previousEventMillis));
                    }
                    previousEventMillis = eventMillis;

                    Transaction transaction = SparkovMapping.transaction(record, eventMillis + shiftMillis);
                    publisher.send(Topics.TRANSACTIONS, transaction.getCardId(), transaction);

                    if (++sent % 10_000 == 0) {
                        publisher.flush();
                        System.out.printf("  %,d transactions%n", sent);
                    }
                    if (config.limit() > 0 && sent >= config.limit()) {
                        break outer;
                    }
                }
            }
        }

        publisher.flush();
        System.out.printf("replay complete: %,d transactions%n", sent);
    }
}
