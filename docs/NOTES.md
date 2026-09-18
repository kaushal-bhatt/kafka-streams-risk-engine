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
