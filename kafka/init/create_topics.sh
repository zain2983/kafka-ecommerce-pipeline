#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"

echo "Creating Kafka topics on ${BOOTSTRAP_SERVER}..."

/opt/kafka/bin/kafka-topics.sh --create \
  --if-not-exists \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic ecommerce-events \
  --partitions 3 \
  --replication-factor 1

# DLQ and retry topics (design.md sections 7/12/13, Phase 9). Low,
# operator-driven volume compared to the main topic, so 1 partition each
# is plenty - there's no throughput reason to split them further, and a
# single partition keeps their ordering simple to reason about.
/opt/kafka/bin/kafka-topics.sh --create \
  --if-not-exists \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic ecommerce-events-dlq \
  --partitions 1 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create \
  --if-not-exists \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic ecommerce-events-retry \
  --partitions 1 \
  --replication-factor 1

echo "Done. Current topics:"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "${BOOTSTRAP_SERVER}"
