# kafka-streams-risk-engine

A real-time card-payment risk engine built on **Kafka Streams**. Every authorisation is scored
against state that is already materialised in local state stores, so no database sits on the
hot path.

Built with Java 21, Spring Boot 3, Kafka Streams, Avro and Schema Registry, with Kafka running
in KRaft mode.

> **Status: work in progress.** Enrichment and the decision path are built, tested and
> verified against a live stack. Every transaction gets an APPROVE / REVIEW / DECLINE with
> human-readable reasons. See the [roadmap](#roadmap) for what is planned. Nothing below claims
> a feature that isn't in the code.

---

## The problem

A card authorisation has about 100 ms before the network times out. In that time the engine
has to answer:

- Has this card been used 12 times in the last minute? (a bot testing stolen numbers)
- Would this transaction push the card past its daily limit?
- Was this card in Berlin 4 minutes ago and is it now being used in São Paulo?
- Is this merchant in a category where fraud concentrates?

Each one is a question about **state derived from past events**. The obvious design, a message
queue plus a `SUM()` query against Postgres, breaks in three ways:

1. **Latency.** It is an aggregate query on your hottest table inside a 100 ms budget.
2. **Coupling.** Fraud checks stop working whenever the transaction database struggles.
3. **Correctness.** Counting on top of at-least-once delivery gives wrong answers, and only
   at volume.

Kafka Streams keeps those aggregates in local RocksDB stores inside the application. They are
updated continuously as events arrive and can be rebuilt from the Kafka log at any time.
A lookup is a local read, not a network call.

---

## What it does today

```
payments.transactions.v1              KStream, keyed by cardId, timestamped by eventTime
        │
        ├── leftJoin ── cardProfiles  KTable  ◄── cards ⋈ customers (foreign-key join)
        ├── leftJoin ── merchants     GlobalKTable
        │
        ▼
RiskEvaluator                         Processor API, owns three state stores
   ├─ velocity-store                  every attempt, window store, expires after 1 h
   ├─ spend-store                     approved spend today, per card
   ├─ geo-store                       last location, per card
   └─ punctuator                      evicts stale state hourly (wall-clock)
        │
        ▼
payments.decisions.v1                 APPROVE / REVIEW / DECLINE + score + reasons
```

### Enrichment: three joins, three reasons

| Join | Why this type |
|---|---|
| **cards ⋈ customers** (table–table, foreign key) | A card's customer almost never changes, but transactions arrive constantly. Joining the tables costs one lookup per card change. Joining on the stream would cost one per transaction. |
| **transactions ⟕ cardProfiles** (stream–table, left) | Both are keyed by `cardId` with the same partition count, so the join is local and needs no repartition. It's a *left* join so that an unknown card is sent to review rather than silently dropped. |
| **… ⟕ merchants** (stream–GlobalKTable) | The stream is keyed by card, not merchant. A normal table join would force a repartition on every authorisation. A GlobalKTable is copied to every instance, so it can be looked up by any key. |

### Decisions: the Processor API, on purpose

A DSL aggregation like `groupByKey().windowedBy(...).count()` *emits* its result downstream.
That's the right shape for publishing a statistic, and the wrong shape for a decision. To
decide on the transaction in hand, the engine has to *read* the card's recent history, combine
several reads into one verdict, and then update that history according to the verdict.
`RiskEvaluator` does all of that in one `process()` call against three local stores. It uses
one range scan of the window store to answer both the velocity rule and the card-testing rule.

| Rule | Fires when | Score |
|---|---|---|
| `VELOCITY` | More than 5 attempts on one card in 60 s | 45 |
| `CARD_TESTING` | 4 or more attempts under €2 across 3 or more merchants in 10 min | 60 |
| `DAILY_LIMIT` | This transaction would take the card past its daily limit | 80 |
| `GEO_VELOCITY` | Implied speed since the last location is over 900 km/h (ignored under 100 km) | 80 |
| `MERCHANT_RISK` | Gambling, quasi-cash, money-transfer or card-not-present category | 20 |
| `UNKNOWN_CARD` | The card isn't in the reference data | 50 |
| `CARD_NOT_ACTIVE` | The card is blocked or expired | 100 |
| `CUSTOMER_RISK_TIER` | The customer is HIGH risk tier. Only escalates, never triggers on its own | 10 |

Scores add up, capped at 100: **under 40 approve, 40–70 review, over 70 decline.**

**State follows the verdict.** Every attempt counts toward velocity, because a card tester's
rejected attempts are exactly the signal. But a **declined** transaction doesn't add to daily
spend, since the money never moved. It also doesn't move the card's last-known location.
Otherwise one fraudulent charge in São Paulo would make the real cardholder's next purchase
in Berlin look like impossible travel, and the fraud would lock the victim out. There's a test
for exactly that.

**State is bounded.** The window store expires on its own. The two key-value stores would
grow by one entry per card forever, so a wall-clock punctuator evicts stale entries every hour.
It runs on the wall clock, not stream time, because stream time only advances when records
arrive. An idle partition would never clean up.

Also in place:

- An Avro data model in a single IDL file, registered in Schema Registry with `BACKWARD`
  compatibility
- `TopologyTestDriver` tests that run in seconds, with no broker and no Docker
- A seeded traffic generator with four scripted fraud scenarios, plus a replay of 1.85M
  labelled transactions from a public dataset
- A health endpoint that reports the Kafka Streams state as well as the HTTP port

---

## Quick start

### Prerequisites

- **Docker** (Docker Desktop on Windows/macOS), with about 4 GB of memory available
- **JDK 17 or newer** on your `PATH` to run Gradle. The build downloads JDK 21 itself through a
  Gradle toolchain, so you don't need to install 21 separately.

Nothing else is needed. The Gradle wrapper is included.

> On Windows, use `gradlew.bat` in place of `./gradlew` in every command below.

### 1. Start the infrastructure

```bash
docker compose up -d
```

This starts Kafka, Schema Registry and kafka-ui, and runs a one-off container that creates the
topics. Wait for topic creation to finish before starting the engine:

```bash
docker compose logs -f init-topics
```

Wait for `topics ready`, then press `Ctrl+C`. That only stops the log tail. The containers keep
running.

### 2. Start the engine

```bash
./gradlew :risk-engine:bootRun
```

Check that it is healthy (the state should be `RUNNING`):

```bash
curl http://localhost:8088/actuator/health
```

### 3. Send some traffic

In a second terminal:

```bash
./gradlew :traffic-generator:run --args="impossible-travel --speed=20"
```

Every scenario publishes the reference data (cards, customers, merchants) first, so each one
works on its own.

The generator prints what to expect:

```
impossible-travel: CARD-0003 at ElektroMarkt Berlin (DE)
impossible-travel: then BR - 10252 km in 4 min, implies 153782 km/h
expect:   first APPROVE, second DECLINE (GEO_VELOCITY)
```

### 4. Look at the result

The engine logs every REVIEW and DECLINE with its reasons:

```
DECLINE CARD-0003 EUR 89.00 score=90 - GEO_VELOCITY (10253 km from DE to BR in 4m 0s implies 153793 km/h); CUSTOMER_RISK_TIER (customer is HIGH risk tier)
```

In **kafka-ui** at <http://localhost:8089>, open **Topics → payments.decisions.v1 → Messages**
to see every decision, and **payments.enriched.v1** to see the joined data that fed it.

---

## Running each piece

### Services

| Service | Address | What it is |
|---|---|---|
| Kafka | `localhost:9092` | Single broker in KRaft mode (no ZooKeeper) |
| Schema Registry | <http://localhost:8081> | Avro schemas, `BACKWARD` compatibility |
| kafka-ui | <http://localhost:8089> | Browse topics, messages, schemas and consumer groups |
| Risk engine | <http://localhost:8088> | Spring Boot + Kafka Streams. `/actuator/health` |
| Postgres | `localhost:5432` | Only used from the CDC stage onwards. Start it with `docker compose --profile cdc up -d` |

```bash
docker compose ps
```

```bash
docker compose logs -f kafka
```

### Traffic generator scenarios

```bash
./gradlew :traffic-generator:run --args="<scenario> [flags]"
```

| Scenario | What it sends | What the engine decides |
|---|---|---|
| `seed` | Only the reference data: 500 customers and cards, 60 merchants | nothing |
| `baseline` | Ordinary purchases, each card near its home (`--limit=N`, default 500) | APPROVE, with `MERCHANT_RISK` noted on casino and crypto merchants |
| `card-testing` | 20 authorisations under €2 on one card, across 8 merchants | 3 APPROVE, then REVIEW (`CARD_TESTING`), then DECLINE once `VELOCITY` joins in |
| `impossible-travel` | Home, then São Paulo four minutes later in event time | APPROVE, then DECLINE (`GEO_VELOCITY`) |
| `limit-breach` | Walks a card up to its daily limit, then one transaction past it | 4 APPROVE, then DECLINE (`DAILY_LIMIT`) |
| `replay` | Replays the labelled Sparkov dataset (see [below](#replaying-real-labelled-data)) | measured in stage 7 |

The engine remembers. If you run `limit-breach` twice on the same day, the card declines sooner
the second time because its approved spend is already near the limit. That's the engine
working as intended. Reset (see below) to replay a scenario from a clean slate.

| Flag | Default | Meaning |
|---|---|---|
| `--speed` | `1` | Makes the demo run faster in real time. At `20`, a four-minute gap takes 12 seconds. |
| `--seed` | `42` | Everything random is seeded, so the same command gives the same output |
| `--limit` | `0` | Stop after N records (`0` = no limit) |
| `--bootstrap-servers` | `localhost:9092` | |
| `--schema-registry` | `http://localhost:8081` | |

### Reading topics from the command line

kafka-ui is the easiest way to look at topics. For a terminal view, the Schema Registry
container already includes an Avro-aware console consumer:

```bash
docker exec -it risk-schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic payments.decisions.v1 --from-beginning --property schema.registry.url=http://localhost:8081
```

### Tests

```bash
./gradlew test
```

18 tests, driven through `TopologyTestDriver` with an in-memory mock Schema Registry, so they
need no running infrastructure. Each rule has its own test, including the edge cases that
matter:

- the declined amount doesn't count toward the daily limit
- a real 14-hour flight isn't impossible travel
- neighbouring shops seconds apart aren't flagged
- a fraudulent location doesn't lock out the cardholder

The eviction punctuator is tested by moving the test clock forward 8 days.

Unit tests aren't enough on their own. `TopologyTestDriver` commits after every record, which
once hid a real bug (see [Things that bit me](#things-that-bit-me)). So each stage is also
verified against the live Docker stack before it's called done.

### Replaying real, labelled data

The traffic generator can replay the
[Sparkov credit card transactions dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection):
1.85M simulated transactions, each labelled fraud or not. That label will later be used to
measure the rules' precision and recall.

```bash
./scripts/fetch-data.sh
```

```bash
./gradlew :traffic-generator:run --args="replay --speed=1000 --limit=50000"
```

The replay first derives cards, customers and merchants from the CSV and publishes them, then
streams the transactions in event-time order, shifted to the present.

The fetch script needs a Kaggle API token. It prints instructions if one isn't found, and
explains how to download the files by hand instead. You can also unzip the CSVs into `data/`
yourself. None of the other scenarios need this data. See [docs/DATA.md](docs/DATA.md) for the field mapping, and for why this dataset was
chosen over the better-known one.

### Resetting everything

```bash
docker compose down
```

Kafka's data isn't kept in a volume, so this wipes every topic **and the Schema Registry**.
Stop the engine first, then delete its local state in `state/` at the repo root:

```bash
rm -rf state
```

On Windows PowerShell: `Remove-Item -Recurse -Force state`.

**This step isn't optional.** The engine's local RocksDB state stores values tagged with Schema
Registry IDs. After a reset the new registry doesn't know those IDs, and the engine
crash-loops with `Schema N not found; error code: 40403` the first time it reads one.

### Troubleshooting

| Symptom | Cause |
|---|---|
| Engine crash-loops with `Schema N not found; error code: 40403` | Local state from before a `docker compose down` survived. Stop the engine, delete `state/` at the repo root, and start it again. |
| Engine logs `MissingSourceTopicException` | It started before `init-topics` finished. Wait for `topics ready` and restart it. |
| A known card gets `UNKNOWN_CARD` | Its transaction overtook its card profile, which is built by a foreign-key join through internal topics. This happens mainly when the engine starts with a backlog. The transaction is reviewed, not lost. The scenarios pause briefly after seeding to avoid it. |
| A purchase at home is declined for `GEO_VELOCITY` | The card's last known location is somewhere far away and recent. Old test data is the usual cause. Reset to start from a clean slate. |
| `curl localhost:8080/...` returns something that isn't this app | The engine runs on **8088**. Port 8080 is often taken by another local project. |
| `create-topics.sh: $'\r': command not found` | The script was checked out with Windows line endings. `.gitattributes` prevents this on a fresh clone. |
| `Bind for 0.0.0.0:8089 failed: port is already allocated` | Something else is using the port. Pick another: `KAFKA_UI_PORT=8189 docker compose up -d`. For the engine, set `SERVER_PORT` and `APPLICATION_SERVER=localhost:<port>` together. |
| Engine can't connect to `localhost:9092` | Docker isn't running, or Kafka hasn't finished starting. Check with `docker compose ps`. |

---

## Roadmap

- [x] **Stage 0: Skeleton.** Gradle multi-module build, Avro model, KRaft stack, topic setup
- [x] **Stage 1: Enrichment.** Three join types, tests, traffic generator, dataset replay
- [x] **Stage 2: Decision path.** A custom Processor API node with three state stores, eight
      rules, verdict-dependent state updates and a punctuator that keeps state bounded
- [ ] **Stage 3: Interactive Queries.** Serve risk profiles over HTTP straight from the state
      stores, route queries between instances, and add a live dashboard
- [ ] **Stage 4: Analytics path.** Windowed merchant decline rates with a grace period and
      `suppress(untilWindowCloses)`
- [ ] **Stage 5: CDC.** Reference data flows from Postgres through Debezium into Kafka
- [ ] **Stage 6: Hardening.** Exactly-once processing, a dead-letter queue, standby replicas
      with a failover demo, schema evolution, metrics
- [ ] **Stage 7: Evaluation.** Precision and recall of the rules measured against 1.85M
      labelled transactions

The full plan is in [docs/BUILD-PLAN.md](docs/BUILD-PLAN.md).

---

## Why Kafka Streams, and not something else

| Alternative | Why not here | Where it would win |
|---|---|---|
| Queue + Postgres | Aggregate queries on the hot path; fraud checks depend on the transaction database being healthy; no event-time windows | Low volume, where simplicity matters more than latency |
| Redis counters + cron | No event time, no replay, no exactly-once; state can't be rebuilt from the log after a bug | Simple rate limits where exact correctness doesn't matter |
| Apache Flink | Needs its own cluster to run; heavier than this problem | Much larger scale, complex event processing, teams that prefer SQL |
| ksqlDB | Fine for the analytics path, but the impossible-travel rule and the read-then-update decision logic are awkward to write in SQL | Windowed analytics with no custom state logic |
| Plain Kafka consumer | You end up rebuilding state stores, changelogs and rebalancing by hand | When you really need no state |

---

## Project structure

```
kafka-streams-risk-engine/
├── common-avro/          Avro IDL (risk.avdl) → generated Java classes, plus shared topic names
├── risk-engine/          Spring Boot + Kafka Streams application
│   └── src/main/java/com/kaushal/riskengine/
│       ├── config/       Streams configuration, Avro serdes, properties
│       ├── topology/     Enrichment and decision topologies, event-time extractor
│       ├── decision/     RiskEvaluator processor, its stores, the rules and thresholds
│       └── health/       Health check that reports the Kafka Streams state
├── traffic-generator/    Seeded fraud scenarios and the dataset replay
├── docker/               Topic creation script
├── scripts/              Dataset download
└── docs/
    ├── DESIGN.md         Architecture and the reasoning behind each decision
    ├── BUILD-PLAN.md     Staged plan and progress
    ├── DATA.md           Test data: the scripted scenarios and the labelled dataset
    └── NOTES.md          Things that broke, and why
```

---

## Things that bit me

A few problems from building this, written up in [docs/NOTES.md](docs/NOTES.md):

- **Record caching made new cards invisible for up to 30 seconds, and every unit test
  passed.** KTable stores are cached by default and only flush downstream on commit (every
  30 s). A new card sat in the cache, its profile never reached the foreign-key join, and the
  inner join dropped its transactions. No errors, no dropped-record count, nothing in the
  logs. `TopologyTestDriver` commits after every record, so it can't show this. Found by
  ruling out partitioning, deserialization and join logic one at a time, then timing probes
  against a fresh card. Fixed by disabling caching on the reference stores, and verified with
  an A/B run against the old build.
- **The usual Avro Gradle plugin was archived in 2023**, and was last tested against Gradle 7.6.
  This project runs on Gradle 9, so it calls `avro-tools` directly through two cacheable build
  tasks instead.
- **avro-tools writes files in the platform's default charset.** On Windows that is
  windows-1252, so a single em dash in a schema comment broke the next build step with
  `Invalid UTF-8 start byte 0x97`. The build now forces UTF-8 everywhere.

---

## License

[MIT](LICENSE)
