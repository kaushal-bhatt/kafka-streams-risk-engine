# Test data

Three tiers, each for a different job. The project needs all three — they are not alternatives.

| Tier | Source | Used for |
|---|---|---|
| 1. Scripted generator | Written in-repo, Java | The live demo, and every `TopologyTestDriver` test |
| 2. Labelled replay | Sparkov dataset | Volume, realism, and measured precision/recall |
| 3. Reference data | Sparkov customer/merchant files → Postgres | Seeding the CDC tables |

---

## Tier 1 — Own scripted generator (primary)

The `traffic-generator` module from the build plan. **This is non-negotiable and no dataset
replaces it**, for two reasons:

- **The demo needs determinism.** Someone opening `risk.wekt.in` has to see an impossible-travel
  decline within about thirty seconds. They will not wait for one to occur naturally in a
  replay. Scenarios are seeded, so the same run produces the same decisions every time.
- **Tests need exact inputs.** A `TopologyTestDriver` test for the velocity rule needs precisely
  six transactions at controlled timestamps, not a sample of real traffic.

Scenarios: `baseline`, `card-testing`, `impossible-travel`, `limit-breach`.

---

## Tier 2 — Sparkov replay (the interesting one)

**[Credit Card Transactions Fraud Detection Dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection)**
— 1,852,394 simulated transactions, 1,000 customers, 800 merchants, Jan 2019 – Dec 2020.
Generated with [Sparkov Data Generation](https://github.com/namebrandon/Sparkov_Data_Generation).

Confirmed columns, read from the generator source
([customer](https://github.com/namebrandon/Sparkov_Data_Generation/blob/master/datagen_customer.py),
[transaction](https://github.com/namebrandon/Sparkov_Data_Generation/blob/master/datagen_transaction.py)):

```
ssn, cc_num, first, last, gender, street, city, state, zip, lat, long,
city_pop, job, dob, acct_num, profile,
trans_num, trans_date, trans_time, unix_time, category, amt, is_fraud,
merchant, merch_lat, merch_long
```

### Why this one and not the famous one

The most popular fraud dataset on Kaggle is
[ULB creditcardfraud](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) — 284,807 rows,
and it is **useless for this project**. Its features are PCA-anonymised into `V1`–`V28`. There
is no card identifier, no merchant, no location, and no timestamp beyond "seconds elapsed since
the first record". You cannot compute a per-card velocity, a daily limit, or a geo-velocity
from it. It is a dataset for training a classifier, not for stream processing.

Worth a footnote in the README: choosing the less famous dataset *because the famous one cannot
express the problem* is itself a signal.

### Field mapping

| Sparkov | Engine | Note |
|---|---|---|
| `cc_num` | `cardId` | The stream partition key. Also the basis of `customerId` (`SPARKOV-<cc_num>`): Sparkov issues one card per customer, and not every export of the dataset includes `acct_num` |
| `merchant` | `merchantId` | Key into the merchants GlobalKTable |
| `category` | MCC proxy | Sparkov uses names like `grocery_pos`, `misc_net`; map to a high-risk set for R4 |
| `amt` | `amountMinor` | **Convert to minor units as a `long` at ingest.** Never carry money as a float |
| `unix_time` | `eventTime` | Feeds the custom `TimestampExtractor` |
| `merch_lat`, `merch_long` | transaction location | Drives R3 impossible travel |
| `lat`, `long` | cardholder home location | Useful as a "distance from home" signal |
| `is_fraud` | ground truth | See below |
| `ssn`, `dob`, `street` | **drop at ingest** | Do not carry PII-shaped fields through the pipeline you do not use |

### The payoff: a measured rule set

`is_fraud` is a ground-truth label. That means the README can carry a section most portfolio
projects cannot:

> Replayed against 1.85M labelled transactions, the rule set flags X% of labelled fraud at a
> Y% false-positive rate, with a per-rule breakdown.

That turns a demo into an evaluated system, and it gives you something concrete to talk about in
an interview: which rule carries the recall, which one generates the false positives, where the
score thresholds should sit.

**Be honest about the limitation in the README.** Sparkov's fraud is injected by a known
generative process, so precision and recall measure agreement with *that simulator's* fraud
model, not with real-world fraud. Saying so plainly is worth more than the headline number.

### Practical gotchas

- **Volume.** The CSVs are several hundred MB. Download via a script into a gitignored `data/`
  directory. Do not commit them, and do not reach for git-lfs.
- **Licence.** The Sparkov generator repo states no licence, so do not vendor its code. Check
  the Kaggle dataset's own stated licence before redistributing anything derived from it, and
  in the README link to the source rather than mirroring the file.
- **No Python on your machine.** If you want to generate fresh data rather than use the
  pre-generated Kaggle CSVs, run the generator in a throwaway container:
  `docker run --rm -v "$PWD/data:/out" python:3.11-slim ...`. Do not install Python locally for
  this.
- **Timestamps are from 2019–2020.** Shift the whole series by a constant offset to land in the
  present, or every window and retention setting will behave strangely. Document the offset —
  it is a constant shift, so event-time ordering is preserved.
- **Replay speed.** Two modes: `--speed=1000x` for a watchable demo, and `--asap` for the
  precision/recall run.
- **R3 will barely fire on replay.** Sparkov places merchants near the cardholder, so natural
  impossible-travel events are rare-to-absent. The geo rule has to be exercised by the tier-1
  generator. Finding this out and writing it down is better than quietly reporting that R3 has
  100% precision on zero hits.

---

### Reference data during replay

The replay seeds its own cards, customers and merchants in a first pass over the CSV, because
the enrichment join drops any transaction whose card it has never seen. Limits and risk tiers
don't exist in the dataset, so they are assigned deterministically from the card number. The
same card gets the same limit on every replay.

## Tier 3 — Reference data for CDC

Sparkov's customer file gives realistic customers and cards; its merchant list gives merchants.
Load both into Postgres with a Flyway migration, and let Debezium carry them into the compacted
topics. This means the `cards`, `customers` and `merchants` KTables are populated from
data shaped like the transaction stream, rather than from three hand-written rows.

Daily limits are not in the dataset — assign them per customer profile so the R2 limit rule has
something realistic to breach.

---

## What this changes in the build plan

- **Stage 1** gains a Sparkov loader: parse CSV, map fields, produce to
  `payments.transactions.v1`. Small, and it front-loads the field mapping.
- **Stage 7** gains the precision/recall run and its README section. This is now one of the
  strongest items in the whole plan — move it earlier if stage 6 runs long.
