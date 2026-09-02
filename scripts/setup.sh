#!/usr/bin/env bash
#
# One-time (safe to re-run) setup: everything needed before this
# project's services can run at all. design.md section 20 - "a new
# developer should be able to clone the repository, install minimal
# prerequisites, and start the platform without manually installing
# Kafka or PostgreSQL."
#
# What it does, in order:
#   1. Checks docker and python3 are on PATH.
#   2. Creates .venv if it doesn't exist, installs all three
#      requirements.txt files into it (producer/ingestion/dbt).
#   3. docker compose up -d, then waits for kafka and postgres to
#      report healthy (not just "container started" - see
#      docker-compose.yml's healthcheck blocks and Phase 11's
#      startup-race fixes for why this distinction matters).
#   4. Creates the Kafka topics (idempotent - safe even if they
#      already exist).
#   5. Runs an initial `dbt run` so analytics.sales_by_interval exists as a
#      table before anything queries it (Grafana's dashboard included).
#   6. Runs tests/grafana/verify_stack.py - a container reporting
#      "healthy" or "Up" doesn't mean the whole system actually works
#      end to end (Phase 11 found a real bug this exact way: Grafana's
#      Postgres datasource looked fine in every surface check except
#      actually running a query through it). This is the same
#      read-only check used to verify Phase 11's monitoring stack,
#      re-run here so setup.sh's "done" message means something.
#
# Usage:
#   ./scripts/setup.sh
#
# After this, use ./scripts/run.sh to start the actual services.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Checking prerequisites"
command -v docker >/dev/null 2>&1 || { echo "docker is required but not found on PATH"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "python3 is required but not found on PATH"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "'docker compose' (v2) is required"; exit 1; }

echo "==> Setting up Python virtual environment (.venv)"
if [ ! -d .venv ]; then
    python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

echo "==> Installing Python dependencies"
pip install --quiet --upgrade pip
pip install --quiet -r producer/requirements.txt
pip install --quiet -r ingestion/requirements.txt
pip install --quiet -r dbt/requirements.txt

echo "==> Starting Docker services (kafka, postgres, kafka-exporter, prometheus, grafana)"
docker compose up -d

wait_healthy() {
    local container="$1"
    local timeout="${2:-90}"
    local waited=0
    echo "    waiting for ${container} to become healthy..."
    while [ "$waited" -lt "$timeout" ]; do
        status="$(docker inspect "$container" --format '{{.State.Health.Status}}' 2>/dev/null || echo "unknown")"
        if [ "$status" = "healthy" ]; then
            echo "    ${container} is healthy"
            return 0
        fi
        sleep 3
        waited=$((waited + 3))
    done
    echo "    ${container} did not become healthy within ${timeout}s (status: ${status})" >&2
    return 1
}

wait_healthy ecommerce-kafka
wait_healthy ecommerce-postgres

echo "==> Creating Kafka topics (idempotent)"
docker cp kafka/init/create_topics.sh ecommerce-kafka:/tmp/create_topics.sh
docker exec ecommerce-kafka bash /tmp/create_topics.sh

echo "==> Building initial dbt models"
(cd dbt && DBT_PROFILES_DIR=. dbt run) || echo "    (dbt run failed - fine on a totally empty raw.events; re-run manually once real events exist)"

echo "==> Verifying the monitoring stack is actually wired up correctly"
echo "    (waiting a few seconds for Prometheus's first scrape...)"
sleep 12
if python3 tests/grafana/verify_stack.py; then
    STACK_OK=1
else
    STACK_OK=0
fi

echo
if [ "$STACK_OK" -eq 1 ]; then
    echo "==> Setup complete - verified end to end."
else
    echo "==> Setup finished, but verify_stack.py found problems (see above)."
    echo "    Services are running but something isn't fully wired up - re-run"
    echo "    'python3 tests/grafana/verify_stack.py' after investigating."
fi
echo "    Grafana:    http://localhost:3000  (login: admin / admin)"
echo "    Prometheus: http://localhost:9090"
echo
echo "    Next: ./scripts/run.sh to start the producer + ingestion consumer"
