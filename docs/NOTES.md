# Notes — things that broke and why

Keep this as you build. The three best entries become the README's "Things that bit me"
section, which is exactly what interviewers ask about.

Format: what happened, what the symptom looked like, what it actually was.

---

## Record caching held new cards back for up to 30 seconds

**The best bug in the project so far, because every unit test passed.**

Symptom: on the first real run, the impossible-travel scenario sent two transactions, and
**zero** came out on `payments.enriched.v1`. No errors, no exceptions, `dropped-records-total`
at 0, stream thread healthy, consumer lag 0. The `TopologyTestDriver` tests for the exact same
topology passed.

How it was narrowed down, one hypothesis at a time:

1. **Co-partitioning?** No. The card, its profile in the store changelog and both transactions
   were all on partition 1.
2. **Deserialization failure silently skipped?** No. `dropped-records-total` was 0, and
   `thread.process.total` was 2,002: 500 cards + 500 customers + 500 FK registrations + 500 FK
   responses + 2 transactions. Everything was processed.
3. **Is the join broken in the live app?** No. A transaction for a card whose profile had
   existed for minutes was enriched correctly.
4. **So it's timing, but the second transaction arrived 13 s after its card.** A brand-new
   card with transactions at +4, +6, +8, +12 and +17 s: **none** enriched. The same card
   after 30 s: enriched.

Cause: KTable stores are **cached by default**, and the cache only flushes downstream **on
commit**, which is every 30 s under at-least-once (`commit.interval.ms`). A new card waited in
the `cards-store` cache and wasn't forwarded into the foreign-key join until the next commit.
Until then its `CardProfile` didn't exist, and the inner stream-table join dropped every
authorisation on that card.

`TopologyTestDriver` commits after every record, so it can never show this.

Fix: `.withCachingDisabled()` on the reference stores (`cards-store`, `customers-store`,
`card-profile-store`). Caching exists to collapse rapid updates to the same key into one
downstream record. Reference data barely changes, so it bought nothing and cost up to 30 s of
staleness on the latency-critical path.

Verified with an A/B run: a brand-new card with transactions 4 s later. Old build: 0 of 3
enriched. Fixed build: 3 of 3.

The lesson: **default caching trades latency for throughput, and a unit test harness that
commits on every record hides that trade completely.** Any Kafka Streams path where table
freshness matters needs this decided on purpose.

## Stage 7: two hours to evaluate, then four minutes

The first full evaluation ran at ~250 transactions/s, which is over two hours for 1.85M, and
looked frozen because it only reported progress every 100k rows. Two guesses failed before
measurement found the cause:

1. **Guess: RocksDB flushing on every commit.** Switched every store to in-memory. No change.
2. **Guess: exactly-once would skip the per-record checkpoint.** It didn't: the test driver
   enforces a checkpoint on every commit regardless. ~200/s.
3. **Measured:** 12 thread dumps of the running JVM (`jcmd <pid> Thread.print`). 11 of the 12
   were inside `OffsetCheckpoint.write -> FileDescriptor.sync`, with `RocksDBStore.onCommit` on
   the stack. So one store was *still* on RocksDB. The state directory showed exactly one:
   `card-customer-fk-subscription-store`, the foreign-key join's internal store.

`javap -c` on Kafka Streams 3.7.1's `KTableImpl.doJoinOnForeignKey` showed why the in-memory
setting didn't reach it: it calls `Stores.persistentTimestampedKeyValueStore(...)` directly.
In 3.8.1 the same method uses `SubscriptionStoreFactory`, which calls
`dslStoreSuppliers().keyValueStore(...)` and so honours the setting.

Fix: the evaluation module alone runs Kafka Streams 3.8.1. The engine stays on 3.7.1 until a
deliberate upgrade. Result: ~8,000/s, the full dataset in 3m50s. The same 50,000 transactions
on the old and new setups produced reports identical line for line (63 lines diffed), so the
speed-up changed nothing but the speed.

Lesson: after the first wrong guess, measure. Two fixes based on reasoning cost two runs; one
set of thread dumps found the answer.

## Stage 7: what the evaluation showed about the rules

See the README's Evaluation section for the numbers. The one to remember is that
`GEO_VELOCITY` caused 71% of false declines on this data. Sparkov jitters each merchant's
location up to ~100 km per transaction, and the rule has no model of location noise beyond a
100 km floor. The rule behaves exactly as specified. What this shows is that "impossible
travel" needs to scale with distance: 10,000 km in minutes is certain, 150 km in minutes is
often just noise. That, and the unused REVIEW band, is the next tuning experiment: train on
`fraudTrain`, report `fraudTest` only.

## Stage 6: standby replicas don't speed up failover. They keep reads up during it.

The stage 3 note below originally said standbys would remove "the restore part" of the ~35 s
outage. Measuring showed that was the wrong mental model. Restoring a few hundred records from
the changelog took milliseconds. Almost the entire outage was **failure detection**: Kafka
waiting out the consumer session timeout before it will move a dead member's partitions.
Standby replicas don't change that at all. With standbys it still took 43.6 s for the
partition to move.

What standbys change is who can answer *during* that window. The query routing now tries the
owner and, if it's unreachable, falls back to an instance holding a standby copy, using
`enableStaleStores()`:

```
before kill   asked :8097 -> servedBy=localhost:8098 stale=false
+0.4s   200   servedBy=localhost:8097 stale=true  spent=3450
+43.6s  200   servedBy=localhost:8097 stale=false spent=3450
```

So "standbys give you fast failover" is half true. They give fast failover *of reads*, and no
restore time on takeover. Processing of the dead instance's partitions still waits for
detection. To cut that, lower `session.timeout.ms`, and accept that a long GC pause will then
look like a death.

## Stage 6: exactly-once was not slower

Expected: EOS adds latency, because output is only visible after the transaction commits.
Measured, 400 transactions at 20/s, two runs each, send until readable by a
`read_committed` consumer:

| | p50 | p95 | p99 |
|---|---|---|---|
| at_least_once | 94 / 69 ms | 108 / 106 ms | 115 / 108 ms |
| exactly_once_v2 | 71 / 61 ms | 81 / 76 ms | 136 / 79 ms |

The at-least-once p95 sitting at ~106 ms gave it away. Kafka Streams defaults its producer to
`linger.ms=100`, so at-least-once output waits up to 100 ms to be batched. Under EOS, a commit
every 100 ms flushes the producer, so output goes out at the commit, often sooner. At this
load, both modes' latency comes from batching settings, not from the guarantee. The first EOS
run's p99 of 136 ms didn't reproduce, so it was a tail outlier, not a property. Throughput at
high volume, where EOS genuinely costs something, wasn't measured, and isn't claimed.

## Stage 6: one more PowerShell 5.1 trap

Posting a schema to the registry's compatibility endpoint failed with
`Cannot deserialize value of type String from Object value`. `Get-Content -Raw` in Windows
PowerShell 5.1 returns a string with hidden `PSPath`/`PSDrive` note properties attached, and
`ConvertTo-Json` serialises the string as an object containing them. Reading the file with
`[IO.File]::ReadAllText(...)` gives a plain string. That makes four Windows encoding or
serialisation traps in this project, all silent until something downstream refuses the result.

## Stage 3: failover without standbys takes about 35 seconds

Measured, not guessed. Two instances were running, each owning three partitions. One was
hard-killed, and the other was asked every 2 s about a card the dead one owned:

```
+0s   503  owner localhost:8097 is unreachable
+35s  200  servedBy=localhost:8098  (same data: spentMinor=3450)
```

The 35 s is Kafka noticing the member is gone (the consumer session timeout), plus the
survivor restoring those partitions' stores from their changelog topics. No data was lost,
and the answer was never wrong, only unavailable. That was the goal of returning `503` +
`Retry-After` rather than a 404, which would have wrongly said "this card doesn't exist".

This is the baseline for stage 6. With `num.standby.replicas=1`, the survivor already holds a
warm copy of the other instance's stores, so the restore part disappears. With
`enableStaleStores()` it can even answer during the rebalance. (The stage 6 measurement above
showed the restore part was negligible anyway. The 35 s was detection, and the real win was
answering during it.)

Side note from the same session: the dashboard's card box relied on implicit form submission
for the Enter key, which the embedded test browser didn't trigger. The fix was an explicit
`keydown` handler with `preventDefault()`, so a real browser doesn't submit twice.

## Local state outlived the cluster it belonged to

Symptom: after `docker compose down` and `up`, the engine crash-looped on the very first
transaction:

```
SerializationException: Error retrieving Avro value schema for id 5
Caused by: RestClientException: Schema 5 not found; error code: 40403
  ... KTableSourceValueGetter.get -> KStreamKTableJoinProcessor.doJoin
```

Cause: two things lined up.

1. **The reset missed the state directory.** `state.dir` is the relative path `./state`.
   IntelliJ runs the app from the repo root and `gradlew bootRun` ran it from `risk-engine/`,
   so the state lived in different places depending on how the engine was started. The reset
   instructions deleted `risk-engine/state`, while the state was actually in `./state`.
2. **Local state isn't self-contained.** Every Avro value in RocksDB starts with a Schema
   Registry ID. `docker compose down` wiped the registry, so the fresh one had never issued
   ID 5. The engine trusted its local checkpoint, read a stored card profile during the join,
   asked the new registry for schema 5, and failed. `REPLACE_THREAD` then restarted the thread
   straight into the same failure.

Fix: `bootRun` now runs from the repo root too, so `./state` is in one place whichever way the
engine is started. The README reset steps say where it is and why deleting it isn't optional.

The general lesson: **a Kafka Streams application's local state is only valid alongside the
exact cluster and registry that produced it.** Restore it from the changelog, or delete it.
Never carry it across a rebuilt environment.

## Stage 2 trade-offs, found while verifying against the live stack

These aren't bugs. They're consequences of deliberate design choices, written down so they
don't come as a surprise.

**A cold start with a backlog reviews some known cards as `UNKNOWN_CARD`.** When the engine
starts against topics that already hold cards and transactions, it can process a transaction
before the foreign-key join has built that card's profile. The join has to round-trip through
two internal topics that start empty. Since stage 2 made the join a left join, those
transactions are reviewed rather than lost. That is the right failure mode for a risk engine.
A production system would bootstrap reference data before opening the transaction stream.

**A false-positive `GEO_VELOCITY` decline repeats until enough time has passed.** Declined
transactions deliberately don't move the card's last-known location, so that fraud abroad
can't lock the cardholder out at home. The flip side is that if the stored location is wrong,
for example stale test data, purchases at the real location keep being declined until the
implied speed drops below 900 km/h. Dublin to Berlin (1,300 km) clears after about 90
minutes. In the live verification a card had "Dublin 19 minutes ago" left over from old test
data, and its first Berlin purchase was declined. That was correct given the history, and
confusing until the reason detail was read. Every reason carries distances, places and times
precisely so that this kind of thing can be diagnosed from the decision record alone.

**Verification needs a card with no history.** The first attempt to verify impossible travel
used a card that the baseline scenario had used in Paris two minutes earlier. The engine
correctly flagged Paris to Berlin, which wasn't the test intended. Check that a card is unused
before treating it as clean.

## PowerShell adds a byte-order mark when piping into `docker exec`

Symptom: while debugging the above, hand-made probe transactions for `CARD-0003` landed on
partition 3 instead of partition 1 and never joined.

Cause: piping a string from Windows PowerShell 5.1 into `docker exec -i` prepends a UTF-8 BOM
(`EF BB BF`). The key became `﻿CARD-0003`, hashed to a different partition, and matched
nothing. It looked exactly like a join bug. Found by dumping the raw key bytes with `od -c`.

Fix: pipe from Git Bash (`printf ... | docker exec -i ...`), which passes bytes through as-is.

## Avro code generation without the Gradle plugin

Symptom: nothing yet — this one was avoided rather than hit.

The usual choice, `com.github.davidmc24.gradle.plugin.avro`, was archived in October 2023 and
was last tested against Gradle 7.6. This project is on Gradle 9.6. Rather than find out the
hard way, `common-avro/build.gradle.kts` drives `avro-tools` directly through two `JavaExec`
tasks: IDL → `.avpr` → generated Java. Both are cacheable and declare proper inputs/outputs.

Using a single Avro **IDL** file rather than a directory of `.avsc` files also solves
cross-record references — `CardProfile` embeds `CardStatus`, `EnrichedTransaction` embeds
`Transaction` — without duplicating type definitions across files.

## avro-tools writes files in the platform charset

Symptom:

```
org.apache.avro.SchemaParseException: com.fasterxml.jackson.core.JsonParseException:
Invalid UTF-8 start byte 0x97
```

Cause: avro-tools wrote `risk.avpr` using the JVM's default charset. On Windows that is
windows-1252, so the em dash in a doc comment was written as the single byte `0x97`. The very
next step read the file back as UTF-8 and rejected it.

Fix: `defaultCharacterEncoding = "UTF-8"` on both `JavaExec` tasks, plus
`options.encoding = "UTF-8"` on every `JavaCompile` in the root build. A build that depends on
the platform default charset is a build that works on your laptop and fails in CI.

## PowerShell 5.1 mangles UTF-8 source files

Symptom: `error: illegal character: '﻿'` on line 1, and em dashes turned into `â€"`.

Cause: `Get-Content` in Windows PowerShell 5.1 reads a UTF-8 file without a BOM as ANSI, and
`Set-Content -Encoding utf8` writes one back **with** a BOM. Round-tripping a Java file through
that pair corrupts every non-ASCII character and then prepends a byte javac refuses to parse.

Fix: don't use `Get-Content`/`Set-Content` to edit source files, and keep Java comments ASCII.
Worth knowing for any scripted refactor on Windows.

## Gradle 9 removed the implicit JUnit launcher

Symptom: `Failed to load JUnit Platform. Please ensure that all JUnit Platform dependencies
are available on the test's runtime classpath, including the JUnit Platform launcher.` — with
everything compiling fine.

Fix: `testRuntimeOnly("org.junit.platform:junit-platform-launcher")` in each module that has
tests. Gradle used to add this for you.

## Gradle 9 requires included project directories to exist

Symptom: `Configuring project ':risk-engine' without an existing directory is not allowed.`

`settings.gradle.kts` can no longer `include()` a module whose directory has not been created
yet. Obvious in hindsight, but it turns "scaffold the settings file first" into an error.
