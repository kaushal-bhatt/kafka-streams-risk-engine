# kafka-streams-risk-engine

[![CI](https://github.com/kaushal-bhatt/kafka-streams-risk-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/kaushal-bhatt/kafka-streams-risk-engine/actions/workflows/ci.yml)
![Java 21](https://img.shields.io/badge/Java-21-blue)
![Kafka Streams](https://img.shields.io/badge/Kafka%20Streams-3.7-black)

A real-time card fraud checker built with **Kafka Streams**. Every payment is checked in
milliseconds against data the app already holds in memory, with no database lookup.

**[See how it works →](https://kaushal-bhatt.github.io/kafka-streams-risk-engine/)**

## What it does

- Checks every card payment and returns **APPROVE**, **REVIEW** or **DECLINE**, with the reason.
- Catches: too many payments too fast, card testing with tiny amounts, daily limit breaches,
  and impossible travel (Berlin, then São Paulo 4 minutes later).
- Answers "what do we know about this card?" over HTTP, straight from the app's own state.
- Tracks every shop's decline rate in 5-minute windows. A sudden spike can mean a hacked card
  machine.
- Shows every decision live on a dashboard.

## Results

Tested on **1.85 million** real-looking payments with known fraud labels:

- **89%** of cards hit by fraud were caught
- **54%** of the fraud money was stopped
- After tuning, **64% fewer** good customers were wrongly blocked, measured on data the tuning
  never saw

## Built to stay up

- **Exactly-once:** a crash never counts a payment twice.
- **Failover:** if one server dies, another answers in under half a second from its backup copy.
- **Bad messages** are set aside in a dead-letter topic, never lost.
- **45 tests**, all run on every push.

## Tech

Java 21 · Spring Boot 3 · Kafka Streams · Avro · Schema Registry · Kafka (KRaft) · Docker

## Run it

Needs Docker and JDK 17+. On Windows, use `gradlew.bat`.

```bash
docker compose up -d
```

```bash
./gradlew :risk-engine:bootRun
```

In a second terminal:

```bash
./gradlew :traffic-generator:run --args="impossible-travel --speed=20"
```

Open **http://localhost:8088** to watch the decisions arrive.

## Project layout

```
risk-engine/         the Kafka Streams app: joins, fraud rules, queries, dashboard
traffic-generator/   sends test payments and fraud scenarios
evaluation/          scores the rules against the 1.85M labelled payments
common-avro/         the data model (Avro)
```

## License

[MIT](LICENSE)
