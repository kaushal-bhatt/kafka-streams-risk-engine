package com.kaushal.trafficgen;

import com.kaushal.riskengine.avro.Card;
import com.kaushal.riskengine.avro.CardStatus;
import com.kaushal.riskengine.avro.Customer;
import com.kaushal.riskengine.avro.Merchant;
import com.kaushal.riskengine.avro.RiskTier;
import com.kaushal.riskengine.avro.Transaction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Sparkov CSV row to engine records, in one place.
 *
 * <p>Both the live replay ({@link SparkovReplay}) and the offline evaluation use this, so the
 * numbers in the evaluation describe exactly the data the live engine would see.
 *
 * <p>Only columns present in every export of the dataset are read: cc_num, lat, long,
 * trans_num, unix_time, category, amt, is_fraud, merchant, merch_lat, merch_long. PII-shaped
 * fields (ssn, dob, street, names) are never touched.
 */
public final class SparkovMapping {

    private SparkovMapping() {
    }

    /** A Sparkov cardholder as the engine's reference data. One card per customer. */
    public record Cardholder(Customer customer, Card card) {
    }

    public static CSVFormat csvFormat() {
        return CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                // The Kaggle export starts with an unnamed index column.
                .setAllowMissingColumnNames(true)
                .setIgnoreSurroundingSpaces(true)
                .build();
    }

    /**
     * All CSVs in the directory, training file first. The Kaggle download is split into
     * fraudTrain.csv (2019 to mid-2020) and fraudTest.csv (mid to end 2020), and replaying
     * them in that order keeps event time moving forwards.
     */
    public static List<Path> locateCsvs(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            throw new IOException("""
                    No data directory at %s.
                    Download the dataset from https://www.kaggle.com/datasets/kartik2112/fraud-detection
                    and unzip fraudTrain.csv and fraudTest.csv there (see docs/DATA.md)."""
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

    public static String cardId(CSVRecord record) {
        return record.get("cc_num");
    }

    public static String merchantId(CSVRecord record) {
        return stripPrefix(record.get("merchant"));
    }

    public static long eventMillis(CSVRecord record) {
        return Long.parseLong(record.get("unix_time")) * 1000L;
    }

    public static boolean isFraud(CSVRecord record) {
        return "1".equals(record.get("is_fraud"));
    }

    /**
     * Sparkov has no limits or risk tiers, so both are assigned deterministically from the
     * card number: the same card always gets the same limit and tier. They are synthetic, and
     * any rule that depends on them (DAILY_LIMIT, CUSTOMER_RISK_TIER) is measuring invented
     * data, not fraud. The evaluation report says so.
     */
    public static Cardholder cardholder(CSVRecord record, long seed) {
        String cardId = cardId(record);
        Random random = new Random(seed ^ cardId.hashCode());
        String customerId = "SPARKOV-" + cardId;

        Customer customer = Customer.newBuilder()
                .setCustomerId(customerId)
                .setRiskTier(RiskTier.values()[random.nextInt(RiskTier.values().length)])
                .setHomeCountry("US")
                .setHomeLat(Double.parseDouble(record.get("lat")))
                .setHomeLon(Double.parseDouble(record.get("long")))
                .build();

        Card card = Card.newBuilder()
                .setCardId(cardId)
                .setCustomerId(customerId)
                .setStatus(CardStatus.ACTIVE)
                // Between 500 and 3,000 in minor units.
                .setDailyLimitMinor(50_000L + random.nextInt(26) * 10_000L)
                .setCurrency("EUR")
                .build();

        return new Cardholder(customer, card);
    }

    public static Merchant merchant(CSVRecord record) {
        String merchantId = merchantId(record);
        return Merchant.newBuilder()
                .setMerchantId(merchantId)
                .setName(merchantId)
                // Sparkov's category labels ("grocery_pos", "misc_net") stand in for MCCs.
                .setMcc(record.get("category"))
                .setCountry("US")
                // Sparkov scatters a merchant's location per transaction; the first one seen
                // is as good as any. Each transaction carries its own coordinates.
                .setLat(Double.parseDouble(record.get("merch_lat")))
                .setLon(Double.parseDouble(record.get("merch_long")))
                .build();
    }

    public static Transaction transaction(CSVRecord record, long eventMillis) {
        // amt is a decimal amount. It becomes a long in minor units here and never goes
        // back; money is not a float.
        long amountMinor = new BigDecimal(record.get("amt"))
                .movePointRight(2)
                .setScale(0, RoundingMode.HALF_UP)
                .longValueExact();

        return Transaction.newBuilder()
                .setTransactionId(record.get("trans_num"))
                .setCardId(cardId(record))
                .setMerchantId(merchantId(record))
                .setAmountMinor(amountMinor)
                .setCurrency("EUR")
                .setEventTime(Instant.ofEpochMilli(eventMillis))
                // Where the transaction happened, not where the cardholder lives.
                .setLat(Double.parseDouble(record.get("merch_lat")))
                .setLon(Double.parseDouble(record.get("merch_long")))
                .setCategory(record.get("category"))
                .setLabelledFraud(isFraud(record))
                .build();
    }

    /**
     * Sparkov prefixes every merchant with "fraud_", legitimate ones included. Leaving it in
     * would make the merchant id look like a label.
     */
    static String stripPrefix(String merchant) {
        return merchant.startsWith("fraud_") ? merchant.substring("fraud_".length()) : merchant;
    }
}
