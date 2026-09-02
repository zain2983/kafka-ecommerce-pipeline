#!/usr/bin/env python3
"""
Phase 12 failure-recovery test: the monitoring stack itself, not the
data pipeline. Every other failure-recovery test in this project kills
something in the pipeline's critical path (Postgres, the consumer,
Kafka itself); this one kills kafka-exporter - purely observability
tooling, nothing in the ingestion path depends on it - and confirms:

  1. Killing it (SIGKILL) doesn't affect the pipeline at all: a real
     ingestion consumer keeps ingesting normally while it's dead.
     Monitoring is a passenger, not a dependency.
  2. Prometheus notices the target went down (health=down) instead of
     silently keeping stale data forever.
  3. `restart: unless-stopped` (docker-compose.yml) is actually
     configured on the container - confirmed by reading the running
     container's own HostConfig, not just by eyeballing the YAML.
     (Originally `on-failure` when this test was written in Phase 12;
     changed repo-wide to `unless-stopped` so every service - not just
     kafka-exporter - comes back after a full VM reboot, not just a
     process crash. See docs/DEPLOYMENT.md's "Bugs already fixed" #2.)
  4. Once it's back (see note below on how "back" happens here) and
     Prometheus resumes scraping it, Grafana's dashboard queries work
     again - reusing verify_stack.py's own checks rather than
     duplicating them.

IMPORTANT - what this does NOT (and structurally cannot) prove: that
the restart policy fires automatically for THIS kind of kill.
Docker's restart policies deliberately do not apply when a container is
stopped via an explicit API call (docker kill / docker stop / an
in-container `kill` sent through docker exec all count) - only when the
containerized process dies completely on its own. That's by design: if
someone explicitly told a container to stop, Docker assumes that was
intentional and won't fight it by restarting. This was confirmed
empirically while writing this test - the policy IS present and DOES
work (it's what silently fixed kafka-exporter's real startup-race crash
back in Phase 11), but there's no reliable, non-destructive way to
force a container to crash "by itself" on demand from outside it. So
step 4 explicitly brings kafka-exporter back (`docker compose up -d`)
rather than waiting on a restart that this kill method will never
trigger - representing what an operator (or a genuine crash + the real
policy) would do, without falsely claiming to have exercised the policy
end-to-end.

Requires the full stack running:
    docker compose up -d
    source .venv/bin/activate

Usage:
    python tests/grafana/test_monitoring_resilience.py

Note: this kills the shared ecommerce-kafka-exporter container and
brings it back itself before exiting, even on failure.
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Producer

KAFKA_EXPORTER_CONTAINER = "ecommerce-kafka-exporter"
PROMETHEUS_URL = "http://localhost:9090"
RECOVERY_TIMEOUT_SECONDS = 60

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce-events")
INGESTION_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "ingestion")

PG_CONFIG = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
    user=os.environ.get("POSTGRES_USER", "ecommerce"),
    password=os.environ.get("POSTGRES_PASSWORD", "ecommerce"),
)

RUN_ID = uuid.uuid4().hex[:8]


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def docker(*args, timeout=30):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


def get_json(url):
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read())


def kafka_exporter_target_health():
    """Returns 'up', 'down', or None if the target isn't known to Prometheus yet."""
    data = get_json(f"{PROMETHEUS_URL}/api/v1/targets")
    for t in data["data"]["activeTargets"]:
        if "kafka-exporter" in t.get("scrapeUrl", ""):
            return t["health"]
    return None


def container_running(name):
    result = docker("inspect", name, "--format", "{{.State.Running}}")
    return result.stdout.strip() == "true"


def restart_policy(name):
    result = docker("inspect", name, "--format", "{{.HostConfig.RestartPolicy.Name}}")
    return result.stdout.strip()


def event_id():
    return f"evt_test_monitorfail_{RUN_ID}"


def produce_test_event():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    event = {
        "event_id": event_id(),
        "event_type": "PRODUCT_VIEW",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id": f"user_test_monitorfail_{RUN_ID}",
        "product_id": "prod_test",
    }
    producer.produce(
        KAFKA_TOPIC, key=event["user_id"].encode("utf-8"), value=json.dumps(event).encode("utf-8")
    )
    producer.flush(10)


def event_in_db():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw.events WHERE event_id = %s", (event_id(),))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count > 0


def main():
    failures = []

    expect(
        container_running(KAFKA_EXPORTER_CONTAINER),
        "kafka-exporter is running before the test starts",
        failures,
    )

    consumer_proc = None
    try:
        print(f"Killing {KAFKA_EXPORTER_CONTAINER} (SIGKILL)...")
        docker("kill", KAFKA_EXPORTER_CONTAINER)
        time.sleep(2)

        # --- 1. The pipeline itself keeps working, uninterrupted ---
        # Real proof, not an architectural claim: start the real
        # ingestion consumer, produce one event, and confirm it lands
        # in Postgres - all while kafka-exporter is dead. Nothing in
        # the ingestion path imports or configures anything
        # kafka-exporter-related, so this should never even notice.
        consumer_proc = subprocess.Popen(
            [sys.executable, "-m", "app.main"],
            cwd=INGESTION_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        print(f"Started ingestion consumer (pid={consumer_proc.pid}) while kafka-exporter is down")
        time.sleep(4)  # let it join the group
        produce_test_event()

        deadline = time.monotonic() + 20
        landed = False
        while time.monotonic() < deadline:
            if event_in_db():
                landed = True
                break
            time.sleep(1)
        expect(
            landed,
            "the pipeline keeps ingesting normally while kafka-exporter is dead "
            "(test event landed in raw.events)",
            failures,
        )

        consumer_proc.terminate()
        try:
            consumer_proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            consumer_proc.kill()
            consumer_proc.communicate()
        consumer_proc = None

        # --- 2. Prometheus notices ---
        deadline = time.monotonic() + 30
        saw_down = False
        while time.monotonic() < deadline:
            health = kafka_exporter_target_health()
            if health == "down":
                saw_down = True
                break
            time.sleep(2)
        expect(saw_down, "Prometheus marks the kafka-exporter target as down", failures)

        # --- 3. restart: unless-stopped is actually configured (static
        # check - see the module docstring for why this kill method
        # can't exercise the policy live) ---
        policy = restart_policy(KAFKA_EXPORTER_CONTAINER)
        expect(
            policy == "unless-stopped",
            f"kafka-exporter's restart policy is 'unless-stopped' (found: '{policy}')",
            failures,
        )
    finally:
        if consumer_proc is not None and consumer_proc.poll() is None:
            consumer_proc.terminate()
            try:
                consumer_proc.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                consumer_proc.kill()
                consumer_proc.communicate()

        # Bring kafka-exporter back regardless of what happened above -
        # standing in for what an operator (or a genuine crash + the
        # real policy) would do. Always runs, even on failure/exception.
        print("Bringing kafka-exporter back (docker compose up -d)...")
        docker("compose", "up", "-d", "kafka-exporter")

    deadline = time.monotonic() + RECOVERY_TIMEOUT_SECONDS
    came_back = False
    while time.monotonic() < deadline:
        if container_running(KAFKA_EXPORTER_CONTAINER):
            came_back = True
            break
        time.sleep(2)
    expect(came_back, f"kafka-exporter is running again within {RECOVERY_TIMEOUT_SECONDS}s", failures)

    # --- 4. Prometheus resumes scraping it successfully ---
    if came_back:
        deadline = time.monotonic() + 30
        recovered = False
        while time.monotonic() < deadline:
            if kafka_exporter_target_health() == "up":
                recovered = True
                break
            time.sleep(2)
        expect(recovered, "Prometheus scrape target health returns to 'up'", failures)

        # Reuse Phase 11's own stack-verification tool rather than
        # re-implementing its checks: confirms Grafana's datasources,
        # dashboard, and every panel query all still work post-recovery.
        verify_script = os.path.join(os.path.dirname(__file__), "verify_stack.py")
        result = subprocess.run([sys.executable, verify_script], capture_output=True, text=True)
        expect(
            result.returncode == 0,
            "tests/grafana/verify_stack.py passes cleanly after recovery",
            failures,
        )
        if result.returncode != 0:
            print(result.stdout)

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
