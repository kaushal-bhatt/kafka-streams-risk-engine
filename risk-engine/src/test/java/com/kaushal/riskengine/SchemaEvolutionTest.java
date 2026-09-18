package com.kaushal.riskengine;

import com.kaushal.riskengine.avro.Transaction;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stage 6 added {@code deviceFingerprint} to {@code Transaction} on a live topic. This proves
 * that was safe in both directions, using the real v1 schema - generated from the code as it
 * was before the change and kept in test resources - rather than a hand-written approximation.
 *
 * <ul>
 *   <li><b>Backward</b>: the new engine reads transactions written before the change.</li>
 *   <li><b>Forward</b>: an engine still running the old code reads transactions written after
 *       it. That's what makes a rolling deploy safe: producers and consumers can be upgraded
 *       in any order.</li>
 * </ul>
 */
class SchemaEvolutionTest {

    private static final Instant WHEN = Instant.parse("2026-09-18T10:00:00Z");

    private static Schema v1;
    private static final Schema V2 = Transaction.getClassSchema();

    @BeforeAll
    static void loadV1() throws IOException {
        try (InputStream in = SchemaEvolutionTest.class.getResourceAsStream("/schemas/transaction-v1.avsc")) {
            v1 = new Schema.Parser().parse(in);
        }
    }

    @Test
    @DisplayName("v1 has no deviceFingerprint and v2 does - the fixture really is the old schema")
    void fixtureIsTheOldSchema() {
        assertThat(v1.getField("deviceFingerprint")).isNull();
        assertThat(V2.getField("deviceFingerprint")).isNotNull();
    }

    @Test
    @DisplayName("Avro's own checker agrees: compatible in both directions")
    void compatibleBothWays() {
        assertThat(SchemaCompatibility.checkReaderWriterCompatibility(V2, v1).getType())
                .as("new reader, old data (BACKWARD)")
                .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
        assertThat(SchemaCompatibility.checkReaderWriterCompatibility(v1, V2).getType())
                .as("old reader, new data (FORWARD)")
                .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    @DisplayName("an old record read by the new code gets a null deviceFingerprint")
    void newCodeReadsOldRecord() throws IOException {
        GenericRecord old = new GenericData.Record(v1);
        old.put("transactionId", "T-1");
        old.put("cardId", "CARD-1");
        old.put("merchantId", "M-1");
        old.put("amountMinor", 1_234L);
        old.put("currency", "EUR");
        old.put("eventTime", WHEN.toEpochMilli());
        old.put("lat", 52.52);
        old.put("lon", 13.405);

        byte[] bytes = encode(new GenericDatumWriter<>(v1), old);
        SpecificDatumReader<Transaction> newReader =
                new SpecificDatumReader<>(v1, V2, SpecificData.getForClass(Transaction.class));
        Transaction read = newReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));

        assertThat(read.getDeviceFingerprint()).isNull();
        assertThat(read.getAmountMinor()).isEqualTo(1_234L);
        assertThat(read.getEventTime()).isEqualTo(WHEN);
    }

    @Test
    @DisplayName("a new record read by the old code: the unknown field is skipped, everything else intact")
    void oldCodeReadsNewRecord() throws IOException {
        Transaction current = Transaction.newBuilder()
                .setTransactionId("T-2")
                .setCardId("CARD-1")
                .setMerchantId("M-1")
                .setAmountMinor(5_678L)
                .setEventTime(WHEN)
                .setLat(52.52)
                .setLon(13.405)
                .setDeviceFingerprint("device-7f3a")
                .build();

        byte[] bytes = encode(new SpecificDatumWriter<>(V2, SpecificData.getForClass(Transaction.class)), current);
        GenericRecord read = new GenericDatumReader<GenericRecord>(V2, v1)
                .read(null, DecoderFactory.get().binaryDecoder(bytes, null));

        assertThat(read.hasField("deviceFingerprint")).isFalse();
        assertThat(read.get("transactionId").toString()).isEqualTo("T-2");
        assertThat(read.get("amountMinor")).isEqualTo(5_678L);
    }

    private static <T> byte[] encode(org.apache.avro.io.DatumWriter<T> writer, T datum) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}
