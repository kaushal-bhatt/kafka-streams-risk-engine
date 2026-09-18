# kafka-streams-risk-engine

A real-time card-payment risk engine built on **Kafka Streams**. Every authorisation is scored
against state that is already materialised in local state stores, so no database sits on the
hot path.

Built with Java 21, Spring Boot 3, Kafka Streams, Avro and Schema Registry, with Kafka running
in KRaft mode.

> **Status: work in progress.** The enrichment stage is built and tested; the risk rules come
> next. See the [roadmap](#roadmap) for what is done and what is planned. Nothing below claims
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

The **enrichment stage** is complete. Each incoming transaction is joined with its card
profile and merchant using three different join types, each picked for a specific reason:

```
payments.transactions.v1             KStream, keyed by cardId
        │
        ├── join ───── cardProfiles  KTable  ◄── cards ⋈ customers (foreign-key join)
        │
        ├── leftJoin ─ merchants     GlobalKTable
        │
        ▼
payments.enriched.v1
```

| Join | Why this type |
|---|---|
| **cards ⋈ customers** (table–table, foreign key) | A card's customer almost never changes, but transactions arrive constantly. Joining the tables costs one lookup per card change. Joining on the stream would cost one per transaction. |
| **transactions ⋈ cardProfiles** (stream–table) | Both are keyed by `cardId` with the same partition count, so the join is local and needs no repartition. |
| **… ⋈ merchants** (stream–GlobalKTable) | The stream is keyed by card, not merchant. A normal table join would force a repartition on every authorisation. A GlobalKTable is copied to every instance, so it can be looked up by any key. |

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
curl http://localhost:8080/actuator/health
```

### 3. Send some traffic

In a second terminal:

```bash
./gradlew :traffic-generator:run --args="impossible-travel --speed=20"
```

Every scenario publishes the reference data (cards, customers, merchants) first, so each one
works on its own.

### 4. Look at the result

Open **kafka-ui** at <http://localhost:8090>, go to **Topics → payments.enriched.v1 →
Messages**. Each record now carries the transaction together with the card's daily limit, the
customer's risk tier and the merchant's category code.

---

## Running each piece

### Services

| Service | Address | What it is |
|---|---|---|
| Kafka | `localhost:9092` | Single broker in KRaft mode (no ZooKeeper) |
| Schema Registry | <http://localhost:8081> | Avro schemas, `BACKWARD` compatibility |
| kafka-ui | <http://localhost:8090> | Browse topics, messages, schemas and consumer groups |
| Risk engine | <http://localhost:8080> | Spring Boot + Kafka Streams. `/actuator/health` |
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

| Scenario | What it sends |
|---|---|
| `seed` | Only the reference data: 500 customers and cards, 60 merchants |
| `baseline` | Ordinary traffic that should not trigger any rule (`--limit=N`, default 500) |
| `card-testing` | 20 authorisations under €2 on one card, spread across 8 merchants |
| `impossible-travel` | Berlin, then São Paulo four minutes later in event time |
| `limit-breach` | Walks a card up to its daily limit, then one transaction past it |
| `replay` | Replays the labelled Sparkov dataset (see [below](#replaying-real-labelled-data)) |

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
docker exec -it risk-schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic payments.enriched.v1 --from-beginning --property schema.registry.url=http://localhost:8081
```

### Tests

```bash
./gradlew test
```

The topology tests use `TopologyTestDriver` and an in-memory mock Schema Registry, so they need
no running infrastructure.

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

Kafka's data isn't kept in a volume, so this wipes every topic. Also delete the engine's local
state, otherwise it will not match the fresh Kafka:

```bash
rm -rf risk-engine/state
```

### Troubleshooting

| Symptom | Cause |
|---|---|
| Engine logs `MissingSourceTopicException` | It started before `init-topics` finished. Wait for `topics ready` and restart it. |
| Some or all transactions missing from `payments.enriched.v1` | A transaction that arrives before its card profile has been built is dropped by the inner join (a known gap, fixed in stage 2). Run the scenario again; the reference data is already in place the second time. |
| `create-topics.sh: $'\r': command not found` | The script was checked out with Windows line endings. `.gitattributes` prevents this on a fresh clone. |
| Engine can't connect to `localhost:9092` | Docker isn't running, or Kafka hasn't finished starting. Check with `docker compose ps`. |

---

## Roadmap

- [x] **Stage 0: Skeleton.** Gradle multi-module build, Avro model, KRaft stack, topic setup
- [x] **Stage 1: Enrichment.** Three join types, tests, traffic generator, dataset replay
- [ ] **Stage 2: Decision path.** A custom Processor API node with its own state stores for
      velocity, daily limits and impossible travel, plus a punctuator that keeps state bounded
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
│       ├── topology/     The stream processing topology
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

- **The usual Avro Gradle plugin was archived in 2023**, and was last tested against Gradle 7.6.
  This project runs on Gradle 9, so it calls `avro-tools` directly through two cacheable build
  tasks instead.
- **avro-tools writes files in the platform's default charset.** On Windows that is
  windows-1252, so a single em dash in a schema comment broke the next build step with
  `Invalid UTF-8 start byte 0x97`. The build now forces UTF-8 everywhere.
- **Partition counts decide whether a join works.** A stream–table join only matches records
  when both topics have the same key *and* the same number of partitions. Kafka Streams gives
  no warning when they differ; the join simply produces nothing. That's why topics are
  created explicitly with a single shared partition count, not auto-created.

---

## License

[MIT](LICENSE)
