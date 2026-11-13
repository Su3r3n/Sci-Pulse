#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

until "$KAFKA_TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  echo "Waiting for Kafka at $BOOTSTRAP..."
  sleep 2
done

DAY_MS=86400000
RETENTION_7D=$((7 * DAY_MS))
RETENTION_30D=$((30 * DAY_MS))
RETENTION_1D=$((1 * DAY_MS))

create_delete_topic() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  "$KAFKA_TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config cleanup.policy=delete \
    --config "retention.ms=${retention_ms}"
}

create_compact_topic() {
  local topic="$1"
  local partitions="$2"

  "$KAFKA_TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config cleanup.policy=compact
}

# Raw paper & code topics (§2.2)
create_delete_topic "raw.papers.openalex" 3 "$RETENTION_7D"
create_delete_topic "raw.papers.arxiv"    3 "$RETENTION_7D"
create_delete_topic "raw.papers.inspire"  3 "$RETENTION_7D"
create_delete_topic "raw.code.github"     3 "$RETENTION_7D"
create_delete_topic "raw.code.events"     3 "$RETENTION_7D"
create_delete_topic "raw.code.zenodo"     1 "$RETENTION_7D"

# CDC topics (§2.2 — compacted)
create_compact_topic "cdc.registry.public.tracked_projects"     1
create_compact_topic "cdc.registry.public.project_repos"        1
create_compact_topic "cdc.registry.public.institutions"         1
create_compact_topic "cdc.registry.public.project_institutions" 1

# DLQ topics (§2.2 — 30 days)
create_delete_topic "dlq.papers" 1 "$RETENTION_30D"
create_delete_topic "dlq.code"   1 "$RETENTION_30D"

# Synthetic load topic (§2.2 — 6 partitions, 1 day)
create_delete_topic "load.synthetic" 6 "$RETENTION_1D"

echo "All 13 Kafka topics verified/created."
