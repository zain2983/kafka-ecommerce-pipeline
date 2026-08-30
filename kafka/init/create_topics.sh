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

echo "Done. Current topics:"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "${BOOTSTRAP_SERVER}"
