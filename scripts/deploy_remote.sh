#!/usr/bin/env bash
#
# Runs ON THE VM, invoked by a restricted SSH key that GitHub Actions
# (.github/workflows/deploy.yml) holds as a secret. That key's entry in
# ~deploy/.ssh/authorized_keys carries a `command=` restriction pointing
# at this exact file, so every login on that key runs this script and
# nothing else, regardless of what command the client actually sends -
# this file's contents are the entire blast radius of that key leaking.
#
# What it does:
#   1. Fast-forwards the VM's checkout to origin/main (hard reset -
#      this directory is a deploy target, not somewhere to hand-edit).
#   2. Brings up infra (kafka/postgres/monitoring) first, waits for
#      health, and creates topics BEFORE touching producer/ingestion/
#      retry - see the note below on why this ordering matters on this
#      VM specifically.
#   3. Only then rebuilds/restarts producer/ingestion/retry.
#
# .env is untracked (gitignored) so `git reset --hard` never touches it.
#
# Ordering note (found on this deploy flow's first real run): running
# `docker compose up -d --build` for every service in one shot recreates
# producer/ingestion/retry immediately, and the freshly-started producer
# starts generating events right away - on this VM's 2 vCPUs, that's
# enough CPU contention to starve the `kafka-topics.sh --create` admin
# call that ran right after, timing it out. Doing topics first, while
# only kafka/postgres/monitoring are active, avoids the race instead of
# papering over it with a retry loop.

set -euo pipefail

REPO_DIR="/home/deploy/DE-Demo-Project"
cd "$REPO_DIR"

echo "==> Fetching latest main"
git fetch origin main
git reset --hard origin/main

echo "==> Bringing up infra (kafka, postgres, monitoring)"
docker compose up -d --build kafka postgres kafka-exporter node-exporter prometheus grafana

wait_healthy() {
    local container="$1"
    local timeout="${2:-90}"
    local waited=0
    while [ "$waited" -lt "$timeout" ]; do
        status="$(docker inspect "$container" --format '{{.State.Health.Status}}' 2>/dev/null || echo unknown)"
        if [ "$status" = "healthy" ]; then
            return 0
        fi
        sleep 3
        waited=$((waited + 3))
    done
    echo "${container} did not become healthy within ${timeout}s (status: ${status})" >&2
    return 1
}
wait_healthy ecommerce-kafka
wait_healthy ecommerce-postgres

echo "==> Ensuring Kafka topics exist (idempotent)"
docker cp kafka/init/create_topics.sh ecommerce-kafka:/tmp/create_topics.sh
# Internal listener (localhost:29092), not the external/advertised one
# (localhost:9092 -> KAFKA_ADVERTISED_HOST, this VM's public IP) - see
# docs/DEPLOYMENT.md's "Bugs already fixed" #3. Hitting the advertised
# listener from a process running *inside* the kafka container itself
# is a hairpin-NAT-style connection that can hang/time out.
docker exec -e KAFKA_BOOTSTRAP_SERVER=localhost:29092 ecommerce-kafka bash /tmp/create_topics.sh >/dev/null

echo "==> Rebuilding and restarting the pipeline (producer, ingestion, retry)"
docker compose up -d --build producer ingestion retry

echo "==> Deploy complete: $(git rev-parse --short HEAD)"
