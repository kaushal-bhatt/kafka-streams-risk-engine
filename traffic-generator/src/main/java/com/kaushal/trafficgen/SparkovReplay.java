package com.kaushal.trafficgen;

import com.kaushal.riskengine.Topics;
import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.CardStatus;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.RiskTier;
import com.kaushal.riskengine.avro.Transaction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

/**
 * Replays the Sparkov "Credit Card Transactions Fraud Detection Dataset".
 *
 * <p>See {@code docs/DATA.md}. The raw generator emits these columns (read from its source):
 *
 * <pre>
 * ssn, cc_num, first, last, gender, street, city, state, zip, lat, long, city_pop,
 * job, dob, acct_num, profile, trans_num, trans_date, trans_time, unix_time, category,
 * amt, is_fraud, merchant, merch_lat, merch_long
 * </pre>
 *
 * <p>Exports of the dataset differ slightly (the Kaggle CSVs, for example, add an unnamed
 * index column and merge the date and time), so this loader only relies on columns every
 * version has: cc_num, lat, long, trans_num, unix_time, category, amt, is_fraud, merchant,
 * merch_lat, merch_long. PII-shaped fields (ssn, dob, street, names) are never read.
 *
 * <p>The replay runs in two passes:
 * <ol>
 *   <li>Derive cards, customers and merchants from the CSV and publish them to the
 *       compacted reference topics. Without this, the enrichment join has never seen a
 *       Sparkov card number and silently drops every replayed transaction.</li>
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
        List<Path> files = locateCsvs();
        System.out.println("replaying " + files);

        seedReferenceData(files);
        replayTransactions(files);
    }

    // --- Pass 1: reference data ---------------------------------------------------------

    private void seedReferenceData(List<Path> files) throws IOException, InterruptedException {
        Set<String> cardsSeen = new HashSet<>();
        Set<String> merchantsSeen = new HashSet<>();
        long read = 0;

        outer:
        for (Path file : files) {
            try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
                 CSVParser parser = csvFormat().parse(reader)) {
                for (CSVRecord record : parser) {
                    String cardId = record.get("cc_num");
                    if (cardsSeen.add(cardId)) {
                        publishCardAndCustomer(record, cardId);
                    }

                    String merchantId = stripPrefix(record.get("merchant"));
                    if (merchantsSeen.add(merchantId)) {
                        publishMerchant(record, merchantId);
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
        // topics. Give it a moment, or the first transaction on each card arrives before
        // its profile does and is dropped by the inner join.
        Thread.sleep(REFERENCE_SETTLE_MILLIS);
    }

    private void publishCardAndCustomer(CSVRecord record, String cardId) {
        // Sparkov issues one card per customer and has no notion of limits or risk tiers.
        // Both are assigned deterministically from the card number, so repeated replays
        // give every card the same limit and the same tier.
        Random random = new Random(config.seed() ^ cardId.hashCode());
        String customerId = "SPARKOV-" + cardId;

        publisher.send(Topics.CUSTOMERS, customerId, Customer.newBuilder()
                .setCustomerId(customerId)
                .setRiskTier(RiskTier.values()[random.nextInt(RiskTier.values().length)])
                .setHomeCountry("US")
                .setHomeLat(Double.parseDouble(record.get("lat")))
                .setHomeLon(Double.parseDouble(record.get("long")))
                .build());

        publisher.send(Topics.CARDS, cardId, Card.newBuilder()
                .setCardId(cardId)
                .setCustomerId(customerId)
                .setStatus(CardStatus.ACTIVE)
                // Between 500 and 3,000 in minor units.
                .setDailyLimitMinor(50_000L + random.nextInt(26) * 10_000L)
                .setCurrency("EUR")
                .build());
    }

    private void publishMerchant(CSVRecord record, String merchantId) {
        publisher.send(Topics.MERCHANTS, merchantId, Merchant.newBuilder()
                .setMerchantId(merchantId)
                .setName(merchantId)
                // Sparkov's category labels ("grocery_pos", "misc_net") stand in for MCCs.
                .setMcc(record.get("category"))
                .setCountry("US")
                // Sparkov scatters a merchant's location per transaction; the first one
                // seen is as good as any. Each transaction carries its own coordinates.
                .setLat(Double.parseDouble(record.get("merch_lat")))
                .setLon(Double.parseDouble(record.get("merch_long")))
                .build());
    }

    // --- Pass 2: transactions -----------------------------------------------------------

    private void replayTransactions(List<Path> files) throws IOException, InterruptedException {
        long sent = 0;
        long previousEventMillis = -1;
        // The dataset runs 2019-2020. Everything is shifted by one constant offset so it
        // lands in the present: retention and state store TTLs are relative to now, and a
        // two-year-old event time makes every window behave strangely. A constant shift
        // preserves event-time ordering and the gaps between events.
        long shiftMillis = Long.MIN_VALUE;

        outer:
        for (Path file : files) {
            try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8);
                 CSVParser parser = csvFormat().parse(reader)) {

                for (CSVRecord record : parser) {
                    long eventMillis = Long.parseLong(record.get("unix_time")) * 1000L;

                    if (shiftMillis == Long.MIN_VALUE) {
                        shiftMillis = Instant.now().toEpochMilli() - eventMillis;
                    }
                    if (previousEventMillis > 0 && eventMillis > previousEventMillis) {
                        Thread.sleep(config.pauseFor(eventMillis - previousEventMillis));
                    }
                    previousEventMillis = eventMillis;

                    Transaction transaction = toTransaction(record, eventMillis + shiftMillis);
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

    private Transaction toTransaction(CSVRecord record, long eventMillis) {
        // amt is a decimal amount. It becomes a long in minor units here and never goes
        // back; money is not a float.
        long amountMinor = new BigDecimal(record.get("amt"))
                .movePointRight(2)
                .setScale(0, RoundingMode.HALF_UP)
                .longValueExact();

        return Transaction.newBuilder()
                .setTransactionId(record.get("trans_num"))
                .setCardId(record.get("cc_num"))
                .setMerchantId(stripPrefix(record.get("merchant")))
                .setAmountMinor(amountMinor)
                .setCurrency("EUR")
                .setEventTime(Instant.ofEpochMilli(eventMillis))
                // Where the transaction happened, not where the cardholder lives.
                .setLat(Double.parseDouble(record.get("merch_lat")))
                .setLon(Double.parseDouble(record.get("merch_long")))
                .setCategory(record.get("category"))
                .setLabelledFraud("1".equals(record.get("is_fraud")))
                .build();
    }

    // --- Helpers ------------------------------------------------------------------------

    private static CSVFormat csvFormat() {
        return CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                // The Kaggle export starts with an unnamed index column.
                .setAllowMissingColumnNames(true)
                .setIgnoreSurroundingSpaces(true)
                .build();
    }

    /**
     * Sparkov prefixes every merchant with "fraud_", legitimate ones included. Leaving it
     * in would make the merchant id look like a label.
     */
    private static String stripPrefix(String merchant) {
        return merchant.startsWith("fraud_") ? merchant.substring("fraud_".length()) : merchant;
    }

    /**
     * All CSVs in the data directory, training file first. The Kaggle download is split
     * into fraudTrain.csv (2019 to mid-2020) and fraudTest.csv (mid to end 2020), and
     * replaying them in that order keeps event time moving forwards.
     */
    private List<Path> locateCsvs() throws IOException {
        Path dir = Path.of(config.dataDir());
        if (!Files.isDirectory(dir)) {
            throw new IOException("""
                    No data directory at %s.
                    Run scripts/fetch-data.sh first, or see docs/DATA.md for the manual download."""
                    .formatted(dir.toAbsolutePath()));
        }
        try (var stream = Files.list(dir)) {
            List<Path> csvs = stream
                    .filter(p -> p.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".csv"))
                    .sorted(Comparator
                            .comparing((Path p) -> !p.getFileName().toString().toLowerCase(Locale.ROOT).contains("train"))
                            .thenComparing(p -> p.getFileName().toString()))
                    .toList();
            if (csvs.isEmpty()) {
                throw new IOException("No .csv files in " + dir.toAbsolutePath());
            }
            return csvs;
        }
    }
}
