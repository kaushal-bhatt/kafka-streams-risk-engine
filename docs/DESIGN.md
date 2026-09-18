# Design — kafka-streams-risk-engine

A real-time card-payment risk and limits engine built on Kafka Streams.

Status: stages 0 and 1 are built (see [BUILD-PLAN.md](BUILD-PLAN.md)). This document describes
the full target design; the README describes only what exists today.

---

## 1. The problem

A card authorisation has roughly 100 ms end to end before the network times out and the
transaction is dropped. Inside that budget you have to answer:

- Has this card been used 12 times in the last minute? (bot testing stolen card numbers)
- Would this transaction push the card past its daily limit?
- Was this card physically in Berlin 4 minutes ago and is now being used in São Paulo?
- Is this merchant category one where fraud concentrates?

Every one of those questions is about **state derived from a stream of past events**. The
obvious implementation — publish transactions to a queue, have a consumer run
`SELECT SUM(amount) FROM transactions WHERE card_id = ? AND created_at > now() - interval '24 hours'`
— fails on all three axes that matter:

- **Latency.** That query is a scan on your hottest table, inside a 100 ms budget, at
  authorisation rate.
- **Coupling.** The fraud logic now cannot run unless the primary transaction database is
  healthy. Risk becomes an availability dependency of payments.
- **Correctness.** A queue gives at-least-once delivery and no ordering guarantee across
  partitions. Counting anything on top of that is wrong in a way that only shows up at volume.

Kafka Streams answers this differently: the aggregate is **already computed and sitting in a
local RocksDB store on the same JVM**, continuously updated as events arrive, and rebuildable
from the log at any time. The lookup is a local disk read, not a network round trip to a
database.

That is the claim this project exists to demonstrate.

---

## 2. What this shows that a "consume and print" project does not

| Capability | Where it appears |
|---|---|
| Custom Processor API node with owned state stores | `RiskEvaluator` — the decision path |
| Punctuator for bounded state | `geo-store` eviction, WALL_CLOCK_TIME |
| Three distinct join types, each justified | KTable⋈KTable FK, KStream⋈KTable, KStream⋈GlobalKTable |
| Deliberate rekey and repartition | merchant analytics branch |
| Event time vs processing time | custom `TimestampExtractor`, grace periods |
| `suppress(untilWindowCloses)` | merchant decline-rate windows |
| Exactly-once semantics v2 | decision production |
| Interactive Queries **with cross-instance routing** | `/risk/cards/{id}` |
| Standby replicas and stale-store reads | failover demo |
| Dead-letter queue for poison pills | custom deserialization exception handler |
| Schema Registry with a real backward-compatible evolution | Stage 6 |
| `TopologyTestDriver` unit tests | every rule |

---

## 3. Topics

All Avro, all registered in Schema Registry, compatibility mode `BACKWARD`.

| Topic | Key | Cleanup | Source | Notes |
|---|---|---|---|---|
| `payments.transactions.v1` | `cardId` | delete, 7d | traffic generator | Keyed by card so per-card ordering holds — the correct production key, not an artefact of the demo |
| `payments.cards.v1` | `cardId` | **compact** | Debezium ← Postgres | `{cardId, customerId, status, dailyLimitMinor, currency}` |
| `payments.customers.v1` | `customerId` | **compact** | Debezium ← Postgres | `{customerId, riskTier, homeCountry}` |
| `payments.merchants.v1` | `merchantId` | **compact** | Debezium ← Postgres | `{merchantId, name, mcc, country}` — small, slow-changing |
| `payments.decisions.v1` | `cardId` | delete, 30d | this app (EOS) | `{transactionId, cardId, decision, score, reasons[], evaluatedAt}` |
| `risk.merchant-stats.v1` | `merchantId` | **compact** | this app | windowed decline rate, suppressed until window close |
| `payments.transactions.dlq.v1` | original bytes | delete, 30d | this app | poison pills, failure metadata in headers |

**Decision:** cards, customers and merchants arrive via **CDC from Postgres**, not from a
hand-written producer. Postgres stays the system of record for reference data; Kafka is how
that data reaches the edge where it is needed. That is the honest architecture, and it
demonstrates the CDC pattern without inventing a use for it.

---

## 4. Topology

### 4.1 Reference data — enrich the table, not the stream

```
cards (KTable, keyed cardId)
   ⋈ FK join on card.customerId
customers (KTable, keyed customerId)
   ↓
cardProfiles (KTable<cardId, CardProfile>)   ← materialised, one entry per card
```

**Why this rather than joining the customer onto each transaction:** a card's customer changes
almost never; transactions arrive constantly. Joining on the table side pays the cost once per
*change* instead of once per *transaction*. On the stream side it would be a foreign-key
lookup per authorisation.

Small point, but it reliably separates people who have run Kafka Streams from people who have
read the docs.

### 4.2 Merchants — GlobalKTable

```
merchants → GlobalKTable<merchantId, Merchant>
```

**Why global and not partitioned:** a `KStream ⋈ KTable` join requires co-partitioning — the
stream must be keyed by the table's key. This stream is keyed by `cardId`, and rekeying every
transaction to `merchantId` just to look up an MCC code would force a repartition topic onto
the hot path, adding a broker round trip to every authorisation.

A `GlobalKTable` is fully replicated to every instance, so it can be joined by **any** key
extractor with no repartition. The trade-offs are memory and the fact that global tables are
not time-synchronised with the stream. Both are acceptable here: the merchant table is small
and near-static.

Documenting *why* each join type was chosen is the point. Any of the three would "work".

### 4.3 The decision path — Processor API

```
transactions (KStream<cardId, Transaction>)
   ⋈ cardProfiles (KTable)             → co-partitioned, no repartition
   ⋈ merchants (GlobalKTable, by merchantId)
   ↓
RiskEvaluator (custom Processor)
   ├─ WindowStore<String, Long>       velocity-store   retention 1h
   ├─ KeyValueStore<String, Spend>    spend-store      {windowStartMs, sumMinor}
   ├─ KeyValueStore<String, LastSeen> geo-store        {lat, lon, epochMs}
   └─ punctuator(WALL_CLOCK, 1h)      evict geo entries older than 7d
   ↓
decisions (KStream<cardId, Decision>) → payments.decisions.v1  [EOS v2]
   └─ materialised into KeyValueStore<cardId, CardRiskProfile> → Interactive Queries
```

**Why the Processor API here and not the DSL.** This is the central design argument of the
project, and it belongs in the README too.

A DSL windowed aggregation (`groupByKey().windowedBy(...).count()`) *emits* a changelog
downstream. That is the right shape when you want to publish a running statistic. It is the
wrong shape when you need to **read** the current value synchronously, in the middle of
deciding about the transaction you are holding, and then combine five such reads into one
verdict. Wiring that with the DSL means either a chain of joins over partial aggregates, or
emitting intermediate records that no consumer wants.

The Processor API lets `RiskEvaluator` own its stores, read-modify-write them in a single
`process()` call, and emit exactly one record: the decision. One input, one output, all state
local.

So the project uses **both APIs, for the jobs each is actually good at** — see 4.4.

### 4.4 The analytics path — DSL, windowing, suppression

```
decisions
   .selectKey((k, v) -> v.merchantId)
   .repartition(...)                        ← deliberate, and on the cold path
   .groupByKey()
   .windowedBy(TimeWindows.ofSizeAndGrace(5m, 30s))
   .aggregate(declineRate)
   .suppress(untilWindowCloses(unbounded()))
   .toStream() → risk.merchant-stats.v1
```

Here the DSL is exactly right: the output *is* a stream of windowed statistics, late data
should be absorbed by a grace period, and downstream wants one final record per window rather
than every intermediate update. `suppress(untilWindowCloses)` gives that.

The rekey to `merchantId` forces a repartition topic. That is fine — this path is not in the
authorisation budget. Putting the repartition where it does no harm, having just explained in
4.2 why it would have been harmful, is the contrast worth drawing.

### 4.5 Rules

| Rule | State used | Fires when |
|---|---|---|
| R1 velocity | `velocity-store` (hopping 60 s / 10 s) | > 5 authorisations on one card in 60 s |
| R2 daily limit | `spend-store` | `sum + amount > card.dailyLimitMinor` |
| R3 impossible travel | `geo-store` | haversine(last, current) / Δt > 900 km/h |
| R4 merchant risk | GlobalKTable lookup | MCC in high-risk set |
| R5 card testing | `velocity-store` + amount predicate | ≥ 4 authorisations under €2 across ≥ 3 merchants in 10 min |

Each rule returns a weighted score and a human-readable reason. Bands: `< 40 APPROVE`,
`40–70 REVIEW`, `> 70 DECLINE`. The `reasons[]` array goes into the decision record — a
declined transaction should always be explainable, which in the EU is a regulatory
requirement rather than a nicety.

**Bounded state.** `velocity-store` is a window store with 1 h retention, so it self-expires.
`geo-store` and `spend-store` are plain key-value stores and would grow without limit — hence
the punctuator. Unbounded state stores are the most common way a Kafka Streams app dies in
production, and having a deliberate answer to that is a hiring signal by itself.

---

## 5. Interactive Queries

```
GET /risk/cards/{cardId}           → CardRiskProfile
GET /risk/merchants/{id}/stats     → windowed decline rate
GET /risk/instances                → topology metadata, which instance owns what
```

The naive implementation reads the local store and 404s when the key lives on another
instance. The real one:

1. `streams.queryMetadataForKey(storeName, key, keySerde)` returns the `HostInfo` of the
   active instance for that key's partition.
2. If it is us, read the local store.
3. If not, HTTP-proxy to that host — which is why `application.server` must be configured with
   each instance's advertised address.

That routing layer is the part most tutorials skip, and it is the part that turns "Kafka
Streams has state stores" into "Kafka Streams is serving production reads".

**Standby replicas.** With `num.standby.replicas=1`, a second instance keeps a warm copy of
every store. During a rebalance the active store is unavailable; the API can either wait or
serve a slightly stale read via `StoreQueryParameters.enableStaleStores()`. The demo exposes
both behaviours behind a query parameter, so the trade-off is visible rather than asserted.

**The failover demo.** Two instances up, kill one, keep polling `/risk/cards/{id}`. Without
standbys the store rebuilds from the changelog and the endpoint is down for the restore
duration. With standbys it is available almost immediately. That side-by-side is the most
convincing thing in the repo, and it is what the README GIF should show.

---

## 6. Correctness concerns

### Event time

Transactions carry `eventTime` (when the terminal saw it), which is not when Kafka received
it. A custom `TimestampExtractor` reads it, so windows are computed on event time. Fallback
for a missing or absurd timestamp: log it, use record time, and count it in a metric.

### Late data

The decision path is inherently "now" — a transaction arriving 5 minutes late is still scored
against whatever state exists at processing time, because there is no useful alternative when
the answer is needed synchronously. The analytics path uses a 30 s grace period and corrects
itself.

Being explicit that the two paths handle lateness *differently, on purpose* is worth a README
paragraph. It is the kind of nuance that only comes up once you have had to reason about it.

### Exactly-once

`processing.guarantee=exactly_once_v2` on the decision path. Worth documenting in the repo:

- What it actually guarantees: the input offset commit, the state store update, and the write
  to `payments.decisions.v1` commit as one transaction. It does **not** make an external HTTP
  call idempotent.
- The cost: EOS drops `commit.interval.ms` to 100 ms, so latency is bounded by transaction
  commit rather than by processing, and throughput falls.
- `transaction.timeout.ms` must be under the broker's `transaction.max.timeout.ms` or the app
  fails at startup — a good footnote, because it is a real thing that bites people.

### Failure handling

- `DeserializationExceptionHandler` → route the raw bytes plus failure metadata to
  `payments.transactions.dlq.v1`, return `CONTINUE`. A malformed record must not stall the
  partition.
- `ProductionExceptionHandler` → fail on anything that is not a serialization error.
- `StreamsUncaughtExceptionHandler` → `REPLACE_THREAD`, so one bad thread does not take the
  instance down.

---

## 7. Testing

- **`TopologyTestDriver` per rule.** No broker, no Docker, millisecond tests. Drive wall-clock
  forward with `advanceWallClockTime()` to trigger and assert the geo-store punctuator. This is
  the single most credible signal in the repo that the author has written Kafka Streams rather
  than read about it — almost no portfolio project has these.
- **One Testcontainers end-to-end test** covering Kafka and Schema Registry, asserting that the
  scripted card-testing burst produces a `DECLINE` with the expected reason codes.
- **A schema-evolution test**: serialize with v2 of the transaction schema, deserialize with
  v1, assert it still reads.

---

## 8. Making it legible to a non-engineer

Everything above is invisible in a GitHub repo unless something moves. So:

Test data is covered in full in [DATA.md](DATA.md) — a scripted generator for the demo and the
tests, plus a replay of a labelled public dataset for volume and measured precision/recall.

**Traffic generator** (separate Gradle module) with scripted, seeded scenarios, so the demo is
reproducible rather than random:

1. `baseline` — Poisson arrivals across 500 cards, all approved
2. `card-testing` — one card, 20 authorisations under €2 across 8 merchants in 3 minutes
3. `impossible-travel` — Berlin, then São Paulo 4 minutes later
4. `limit-breach` — walks a card up to and past its daily limit

**Dashboard** — one static HTML page served by Spring Boot, fed by SSE from the decisions
topic. Live transaction feed, decisions colouring green/amber/red, a counter per rule, and a
panel showing which instance owns which partition. No build step, no React — it has to stay
cheap to host.

**README order** (recruiters read top-down and stop early):

1. One-sentence description, then the GIF of the dashboard with a decline firing
2. Link to the live demo
3. The problem — why a queue plus a database does not solve this
4. Topology diagram
5. "Why Kafka Streams and not X" (section 9)
6. `docker compose up` and how to trigger each scenario
7. Everything else

---

## 9. Why Kafka Streams and not X

This table goes in the README verbatim. Senior reviewers read it first, and an honest
trade-off table is worth more than a feature list.

| Alternative | Why not here | Where it would win |
|---|---|---|
| Queue + Postgres | Aggregate queries on the hot path; risk becomes an availability dependency of payments; no event-time windowing | Low volume, where simplicity beats latency |
| Redis counters + cron | No event time, no replay, no exactly-once; state cannot be rebuilt from the log after a bug | Simple rate limits with no correctness requirement |
| Apache Flink | Needs a cluster to operate; heavier than the problem | Much larger scale, complex CEP, SQL-first teams |
| ksqlDB | Fine for the analytics path; the geo-velocity rule and the read-modify-write decision logic do not express cleanly in SQL | Windowed analytics needing no custom state logic |
| Consumer API by hand | You end up reimplementing state stores, changelogs and rebalancing, badly | When you genuinely need no state |

The point of the table is to show the choice was made, not defaulted into.

---

## 10. Deployment

Local: `docker compose up` brings up Kafka in **KRaft mode** (no ZooKeeper — worth calling out,
it dates the project correctly), Schema Registry, Postgres, Debezium Connect, two app instances
behind nginx, and the traffic generator.

Hosted at `risk.wekt.in`: the full stack is roughly 2.5–3 GB of RAM. **Check the Hetzner box has
headroom before committing to this.** If it does not, the fallback is to host only the dashboard
replaying a captured decision log as static JSON, labelled clearly as a replay, with the live
stack running locally. A replay that is honestly labelled beats a hosted demo that OOMs during
an interview.

---

## 11. Open decisions

- **Repo name** — `kafka-streams-risk-engine` chosen for recruiter keyword match, matching the
  `kafka-` prefix of the existing Wikimedia repo. Alternative: `streaming-risk-engine`, cleaner
  but less searchable.
- **Java 21 vs 25** — 21 here, for LTS and CI-runner availability. The Wikimedia repo is on 25.
- **Hetzner RAM** — see section 10. Blocks the hosting decision only, not the build.
