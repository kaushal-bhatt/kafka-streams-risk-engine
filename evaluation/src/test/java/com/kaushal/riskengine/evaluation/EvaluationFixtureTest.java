package com.kaushal.riskengine.evaluation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole evaluation path on a hand-made CSV in the Kaggle export's format - including the
 * unnamed index column it starts with - so the real 1.85M-row run holds no surprises.
 *
 * <p>Card 1111 makes one ordinary purchase, then four tiny labelled-fraud authorisations
 * across three merchants: card testing, which the engine should catch on the fourth. Card
 * 2222 is an ordinary customer.
 */
class EvaluationFixtureTest {

    private static final String HEADER =
            ",trans_date_trans_time,cc_num,merchant,category,amt,lat,long,trans_num,unix_time,merch_lat,merch_long,is_fraud";

    @TempDir
    Path dir;

    @Test
    @DisplayName("card testing in a Kaggle-format CSV is caught on the 4th attempt, and scored correctly")
    void scoresTheFixture() throws IOException {
        long t = 1_325_376_000L; // the Kaggle export's unix_time epoch sits in 2012
        List<String> rows = new ArrayList<>();
        rows.add(HEADER);
        rows.add(row(0, "1111", "fraud_Grocer", "grocery_pos", "45.00", t, "t1", 0));
        rows.add(row(1, "1111", "fraud_Kiosk A", "misc_pos", "1.10", t + 60, "t2", 1));
        rows.add(row(2, "1111", "fraud_Kiosk B", "misc_pos", "1.20", t + 120, "t3", 1));
        rows.add(row(3, "1111", "fraud_Kiosk C", "misc_pos", "1.30", t + 180, "t4", 1));
        rows.add(row(4, "1111", "fraud_Kiosk A", "misc_pos", "1.40", t + 240, "t5", 1));
        rows.add(row(5, "2222", "fraud_Grocer", "grocery_pos", "30.00", t + 300, "t6", 0));
        Path csv = dir.resolve("fraudTest.csv");
        Files.write(csv, rows, StandardCharsets.UTF_8);

        Path state = Files.createDirectory(dir.resolve("state"));
        Evaluation.Result result = new Evaluation(42, 0, line -> { }).run(List.of(csv), state);
        Scorecard s = result.overall();

        assertThat(result.rows()).isEqualTo(6);
        assertThat(result.cards()).isEqualTo(2);
        assertThat(s.fraud()).isEqualTo(4);

        // Only the 4th tiny attempt meets the card-testing threshold (4 attempts, 3 merchants).
        assertThat(s.flagged().truePositives()).isEqualTo(1);
        assertThat(s.flagged().falseNegatives()).isEqualTo(3);
        assertThat(s.flagged().falsePositives()).isZero();
        assertThat(s.rules().get("CARD_TESTING").fraudHits()).isEqualTo(1);

        // One fraud card, caught, after three fraudulent attempts got through.
        assertThat(s.fraudCards()).isEqualTo(1);
        assertThat(s.fraudCardsCaught()).isEqualTo(1);
        assertThat(s.medianFraudsBeforeCatch()).isEqualTo(3.0);

        // And the report renders from it.
        String report = ReportWriter.markdown(result, "fixture");
        assertThat(report).contains("| `CARD_TESTING` |").contains("fraudTest");
    }

    private static String row(int index, String card, String merchant, String category, String amount,
                              long unixTime, String id, int fraud) {
        // All within a few km of each other, so impossible travel stays out of it.
        return "%d,2020-06-21 12:00:00,%s,%s,%s,%s,40.0,-75.0,%s,%d,40.01,-75.01,%d"
                .formatted(index, card, merchant, category, amount, id, unixTime, fraud);
    }
}
