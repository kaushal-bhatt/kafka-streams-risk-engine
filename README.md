# kafka-streams-risk-engine

A real-time card-payment risk engine built on **Kafka Streams**. Every authorisation is scored
against state that is already materialised in local state stores, so no database sits on the
hot path.

Built with Java 21, Spring Boot 3, Kafka Streams, Avro and Schema Registry, with Kafka running
in KRaft mode.

> **Status: work in progress.** Enrichment, the decision path, Interactive Queries and production
> hardening are built, tested and verified against a live two-instance stack:
>
> - Every transaction gets an APPROVE / REVIEW / DECLINE with human-readable reasons.
> - Any card's full risk picture can be queried over HTTP straight from the engine's state.
> - Processing is exactly-once.
> - Reads survive an instance being killed.
>
> See the [roadmap](#roadmap) for what is planned. Nothing below claims a feature that isn't in
> the code, and every number below was measured.

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
        ├──────────────► last-decision-store   KTable, latest decision per card
        ▼
payments.decisions.v1                 APPROVE / REVIEW / DECLINE + score + reasons

GET /risk/cards/{id}  ◄── Interactive Queries over all five stores, routed to the owning instance
GET /                 ◄── live dashboard, fed by server-sent events from payments.decisions.v1
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

### Queries: the state stores are the database

A message queue can't tell you anything about a card. This engine can, because its state
stores already hold everything it knows:

```bash
curl http://localhost:8088/risk/cards/CARD-0003
```

```json
{
  "cardId": "CARD-0003",
  "servedBy": "localhost:8097",
  "routedVia": "localhost:8098",
  "partition": 1,
  "profile": { "status": "ACTIVE", "riskTier": "HIGH", "dailyLimitMinor": 50000, "homeCountry": "DE", "...": "..." },
  "spendToday": { "spentMinor": 3450, "remainingMinor": 46550 },
  "lastSeen": { "place": "DE", "seenAt": "2026-09-18T10:37:42.613Z", "...": "..." },
  "recentActivity": { "attemptsLast60s": 1, "attemptsLast10m": 2 },
  "lastDecision": {
    "decision": "DECLINE", "score": 90, "amountMinor": 8900,
    "reasons": [ { "rule": "GEO_VELOCITY", "detail": "10252 km from DE to BR in 4m 0s implies 153782 km/h" } ]
  }
}
```

That answer is assembled from five local stores, with no database anywhere. Notice what it
says: €34.50 spent, not €123.50, and last seen in DE, not BR. The €89 São Paulo charge was
declined, so the engine never recorded it as spend or as a location.

**Each card's state lives on exactly one instance.** That's the one Kafka assigned the card's
partition to. With two instances, the naive approach answers half the cards and returns 404
for the rest. This one routes instead. It hashes the card id exactly as the producer did
(`queryMetadataForKey`), and if another instance owns that partition it forwards the request
there. The response says so: `servedBy` is the owner, and `routedVia` is the instance you
actually asked. One lookup routes all five stores, because they're all keyed by card id in the
same sub-topology.

If the owner is unreachable, the query falls back to a **standby replica**: a warm copy of
the partition that another instance keeps for exactly this situation. The answer comes back
immediately, marked `"stale": true`. If no copy can answer at all, the response is `503` with
`Retry-After`, never a false 404. See [Failover](#failover-reads-survive-a-dead-instance) for
the measured difference.

### The dashboard

Open <http://localhost:8088> for live decisions as they happen, colour-coded, with counters,
the rules firing most often, which instance owns which partitions, and a box to query any
card. Clicking a card in the feed shows the Interactive Queries answer, including whether it
was routed to another instance.

The feed uses a plain Kafka consumer assigned every partition of the decisions topic, not a
`peek()` inside the topology. A `peek()` would only see the partitions its own instance owns,
so with two instances each dashboard would show half the traffic. It reads with
`isolation.level=read_committed`, so it never shows a decision from an aborted transaction.

---

## Production hardening

Every number in this section was measured on the live Docker stack, with the tools in this repo.

### Failover: reads survive a dead instance

Two instances were running, each active for three partitions and holding a warm **standby**
copy of the other three (`num.standby.replicas=1`). The instance owning `CARD-0003` was
**hard-killed**, with no graceful shutdown, and the survivor was queried every second:

| | Without standbys | **With standbys** |
|---|---|---|
| First query after the kill | `503`, repeated for ~35 s | **`200` after 0.4 s**, from the standby copy, `"stale": true` |
| Data | identical, once available | **identical** (same spend, same last decision) |
| Back to `"stale": false` | — | +43.6 s |

What standbys do **not** do is make Kafka notice the failure faster. Detection is the consumer
session timeout: about 45 seconds by default, and it cost about 44 seconds either way. What
changes is what happens during that window. Without a standby, nothing on the survivor can
answer for the dead instance's cards. With one, the survivor already holds their state and
answers straight away. It labels the answer `stale` rather than passing it off as fresh,
because a standby can trail the owner's last writes. Once Kafka moves the partition, the
standby becomes the active copy with nothing to restore, and the flag clears by itself.

The detection window itself can be cut by lowering `session.timeout.ms`. The trade-off is that
a long GC pause then looks like a dead instance, and triggers a needless rebalance.

### Exactly-once, and what it actually costs

`processing.guarantee=exactly_once_v2`. For each input transaction, the input offset, every
state-store change (velocity, spend, location) and the output decision commit as **one** Kafka
transaction. A crash mid-way can't produce a decision twice, or count one authorisation twice
toward velocity.

The expected cost is latency, since output only becomes readable when the transaction
commits. So it was measured instead of assumed. The `latency` scenario sends 400 transactions
at 20 per second, and times each one from send to its decision being readable by a
`read_committed` consumer. Two runs per mode:

| | p50 | p95 | p99 |
|---|---|---|---|
| `at_least_once` | 94 / 69 ms | 108 / 106 ms | 115 / 108 ms |
| `exactly_once_v2` | 71 / 61 ms | **81 / 76 ms** | 136 / 79 ms |

**At this load, exactly-once costs nothing measurable, and its p95 is lower.** The reason:
Kafka Streams defaults its producer to `linger.ms=100`, so under at-least-once, output waits up
to 100 ms to be batched, which is exactly where that p95 sits. Under exactly-once, the engine
commits every 100 ms, and each commit flushes the producer. Either way the latency comes from a
batching setting, not from the guarantee.

What this doesn't measure is **throughput at high volume**. That's where exactly-once really
costs something: transaction markers and per-commit overhead. It's a separate benchmark, not
claimed here.

```bash
./gradlew :traffic-generator:run --args="latency --limit=400"
```

Run it once with the engine started normally and once with
`--risk.processing-guarantee=at_least_once` to reproduce the comparison.

### Dead-letter queue

A record that isn't valid Avro doesn't stall its partition, and isn't silently skipped either.
It's written to `payments.transactions.dlq.v1` **byte for byte**, with headers recording where
it came from and why it failed. This is from a live test that put raw JSON on the transactions
topic:

```
dlq.original.topic:payments.transactions.v1, dlq.original.partition:0, dlq.original.offset:305,
dlq.exception.class:org.apache.kafka.common.errors.SerializationException,
dlq.exception.message:Unknown magic byte!, dlq.task.id:0_0      CARD-0042      {"definitely": "not avro"}
```

The engine stayed `RUNNING`, and the next transaction on the same partition was processed
normally.

Two deliberate choices:

- **The DLQ write is synchronous.** If the dead-letter topic itself is down, the engine stops
  rather than skip the record. Dropping a transaction without a trace is the one outcome this
  exists to prevent.
- **The DLQ producer is outside the Streams transaction.** A reprocessed task can therefore
  dead-letter the same record twice. For a DLQ that's the right trade: a duplicate costs
  nothing, and a missing record hides a problem.

### Schema evolution on a live topic

Stage 6 added an optional `deviceFingerprint` field to `Transaction` while the topic was in
use. Schema Registry confirmed compatibility before anything was deployed:

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": $(jq -Rs . < common-avro/build/generated/avro/schema/Transaction.avsc)}" \
  http://localhost:8081/compatibility/subjects/payments.transactions.v1-value/versions/latest
# {"is_compatible":true}
```

Then came the real test. An engine still running the **old** code, compiled before the field
existed, was sent transactions in the **new** format. It consumed both, produced both
decisions, and dropped nothing. Because the new field is optional with a null default, the
change works in both directions, so producers and consumers can be upgraded in any order.
`SchemaEvolutionTest` proves both directions against the real v1 schema, kept in test
resources.

### Metrics

`/actuator/prometheus` exposes the engine's own counters alongside every Kafka Streams metric:

```
risk_decisions_total{decision="APPROVE"} 4.0
risk_decisions_total{decision="DECLINE"} 1.0
risk_rule_hits_total{rule="DAILY_LIMIT"} 1.0
risk_dlq_records_total{source="payments.transactions.v1"} 1.0
kafka_stream_task_dropped_records_total{task_id="0_0",...} 1.0     <- Kafka's own count agrees with the DLQ
```

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

Open the dashboard at **<http://localhost:8088>** and click `CARD-0003` in the feed.

The engine also logs every REVIEW and DECLINE with its reasons:

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
| Risk engine | <http://localhost:8088> | Spring Boot + Kafka Streams. Dashboard at `/`, API under `/risk`, health at `/actuator/health` |
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
| `latency` | 20 transactions per second (`--limit=N`, default 200), each timed until its decision is readable | prints p50 / p95 / p99 latency |
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

### The query API

| Endpoint | Returns |
|---|---|
| `GET /risk/cards/{cardId}` | The card's profile, spend today and remaining limit, last location, recent attempts, and last decision with reasons. `"stale": true` if the owner was unreachable and a standby copy answered. `404` if the engine has never seen the card; `503` + `Retry-After` if no copy can answer. |
| `GET /actuator/prometheus` | Decision, rule-hit and DLQ counters, plus every Kafka Streams metric. |
| `GET /risk/instances` | Every instance in the group, and which partitions each one owns (active and standby). |
| `GET /risk/decisions/stream` | Server-sent events: a `snapshot` on connect, then one `decision` per decision. This is what the dashboard uses. |

### Running two instances

This is where Interactive Queries become visible. With the first engine running on 8088,
start a second one in another terminal. It needs its own port, advertised address and state
directory:

```bash
./gradlew :risk-engine:bootRun --args="--server.port=8087 --risk.application-server=localhost:8087 --risk.state-dir=./state-2"
```

Kafka rebalances. Each instance ends up active for three of the six partitions, and holds a
standby copy of the other three:

```bash
curl http://localhost:8088/risk/instances
```

Now ask **both** instances about the same card. Each gives the same answer. One serves it
from its own state, and the other routes to it (`routedVia` is set):

```bash
curl http://localhost:8088/risk/cards/CARD-0003
```

```bash
curl http://localhost:8087/risk/cards/CARD-0003
```

Then kill the instance that owns `CARD-0003` and keep querying the other one. It answers
immediately from its standby copy with `"stale": true`, and after about 45 seconds, once Kafka
has moved the partition, with `"stale": false`. The dashboard marks stale answers in amber.

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

38 tests, none of which need running infrastructure. The topology tests drive
`TopologyTestDriver` with an in-memory mock Schema Registry. The query tests read the same
stores back out, and check the routing logic against a mocked second instance:

- a card owned locally is answered with no HTTP call
- a card owned elsewhere is forwarded
- a forwarded request is never forwarded twice
- an unreachable owner falls back to a standby copy, marked stale
- with no copy anywhere, the answer is 503, not 404

The dead-letter test puts real garbage bytes on the transactions topic. It checks that they
land in the DLQ with their provenance, that the next record still gets a decision, and that
the engine stops rather than drop a record when the DLQ itself fails. The schema-evolution
test proves compatibility in both directions against the real v1 schema.

Each rule has its own test, including the edge cases that matter:

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
| `/risk/cards/...` returns 503 | Partitions are moving between instances: at startup, or after an instance joined or left. Retry after a second, as the `Retry-After` header says. It clears once the group settles. |
| The second instance fails to start with a state-directory lock error | Both instances are using the same `state` folder. Give the second one its own with `--risk.state-dir=./state-2`. |
| `/risk/cards/...` answers with `"stale": true` | The card's owner is unreachable, so a standby copy answered. This clears by itself once Kafka moves the partition, after about 45 s. |
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
- [x] **Stage 3: Interactive Queries.** Risk profiles served over HTTP straight from five state
      stores, routed to the owning instance, and a live dashboard
- [ ] **Stage 4: Analytics path.** Windowed merchant decline rates with a grace period and
      `suppress(untilWindowCloses)`
- [ ] **Stage 5: CDC.** Reference data flows from Postgres through Debezium into Kafka
- [x] **Stage 6: Hardening.** Exactly-once processing (latency measured), a dead-letter queue,
      standby replicas with measured failover, a live schema evolution, Prometheus metrics.
      A Grafana dashboard is not built yet.
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
│       ├── query/        Interactive Queries: store access, cross-instance routing, the API
│       ├── dashboard/    Live decision feed (server-sent events); the page is in resources/static
│       ├── errors/       Dead-letter queue: the deserialization handler and its publisher
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
