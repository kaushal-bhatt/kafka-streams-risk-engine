# Notes — things that broke and why

Keep this as you build. The three best entries become the README's "Things that bit me"
section, which is exactly what interviewers ask about.

Format: what happened, what the symptom looked like, what it actually was.

---

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
