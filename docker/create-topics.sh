#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="kafka:29092"

# Every topic on the join path shares this partition count. A KStream-KTable join
# requires co-partitioning: same key AND same partition count. Kafka Streams does not
# warn when they differ, it just never matches anything.
PARTITIONS=6

create() {
  local name=$1
  local cleanup=$2
  shift 2
  echo "creating ${name} (${cleanup})"
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --config "cleanup.policy=${cleanup}" \
    "$@"
}

echo "waiting for kafka at ${BOOTSTRAP}"
until kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; do
  sleep 2
done

# Event streams — retained by time.
create payments.transactions.v1 delete --config retention.ms=604800000   # 7 days
create payments.enriched.v1     delete --config retention.ms=86400000    # 1 day, stage 1 only
create payments.decisions.v1    delete --config retention.ms=2592000000  # 30 days
create payments.transactions.dlq.v1 delete --config retention.ms=2592000000

# Reference data — compacted, so the latest value per key is kept forever and a
# restarting KTable can rebuild its full state from the topic.
create payments.cards.v1     compact
create payments.customers.v1 compact
create payments.merchants.v1 compact
create risk.merchant-stats.v1 compact

echo
kafka-topics --bootstrap-server "$BOOTSTRAP" --list
echo "topics ready"
