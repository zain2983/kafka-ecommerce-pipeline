#!/usr/bin/env bash
#
# Starts the live pipeline: makes sure the Docker infra is up, then
# runs the real producer and ingestion consumer as background
# processes, streaming both logs to this terminal until you Ctrl+C -
# at which point both get a clean SIGTERM (same graceful-shutdown path
# they already have for being run by hand: flush pending offsets/
# messages, print final stats, exit) rather than being killed outright.
#
# Does NOT touch docker compose on exit - Kafka/Postgres/Grafana/etc.
# keep running afterward, since you'll usually want Grafana still up to
# look at what just happened.
#
# Requires ./scripts/setup.sh to have been run at least once.
#
# Usage:
#   ./scripts/run.sh                       # default rate (see producer/app/config.py)
#   EVENTS_PER_SECOND=50 ./scripts/run.sh  # faster traffic, e.g. for watching Grafana move

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ ! -d .venv ]; then
    echo "No .venv found - run ./scripts/setup.sh first." >&2
    exit 1
fi
# shellcheck disable=SC1091
source .venv/bin/activate

echo "==> Ensuring Docker infra is up"
docker compose up -d

wait_healthy() {
    local container="$1"
    local timeout="${2:-90}"
    local waited=0
    while [ "$waited" -lt "$timeout" ]; do
        status="$(docker inspect "$container" --format '{{.State.Health.Status}}' 2>/dev/null || echo "unknown")"
        [ "$status" = "healthy" ] && return 0
        sleep 3
        waited=$((waited + 3))
    done
    echo "${container} did not become healthy within ${timeout}s" >&2
    return 1
}
wait_healthy ecommerce-kafka
wait_healthy ecommerce-postgres

echo "==> Ensuring Kafka topics exist (idempotent)"
docker cp kafka/init/create_topics.sh ecommerce-kafka:/tmp/create_topics.sh
docker exec ecommerce-kafka bash /tmp/create_topics.sh >/dev/null

mkdir -p logs
CONSUMER_LOG="logs/ingestion.log"
PRODUCER_LOG="logs/producer.log"

echo "==> Starting ingestion consumer (logs: ${CONSUMER_LOG})"
(cd ingestion && exec python3 -m app.main) >"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!

echo "==> Starting producer (logs: ${PRODUCER_LOG})"
(cd producer && exec python3 -m app.main) >"$PRODUCER_LOG" 2>&1 &
PRODUCER_PID=$!

cleanup() {
    echo
    echo "==> Stopping producer and consumer (graceful shutdown)..."
    kill -TERM "$PRODUCER_PID" 2>/dev/null || true
    kill -TERM "$CONSUMER_PID" 2>/dev/null || true
    wait "$PRODUCER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
    echo "==> Stopped. Docker services (Kafka, Postgres, Grafana, ...) are still running -"
    echo "    'docker compose down' if you want to stop those too."
}
trap cleanup EXIT INT TERM

echo
echo "==> Running. Grafana: http://localhost:3000 (admin / admin)"
echo "    Ctrl+C to stop the producer and consumer."
echo

tail -n +1 -f "$CONSUMER_LOG" "$PRODUCER_LOG"
