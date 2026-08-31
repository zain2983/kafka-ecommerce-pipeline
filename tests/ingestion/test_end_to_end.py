#!/usr/bin/env python3
"""
Full pipeline test for Phase 5: produce known events straight to Kafka,
run the real ingestion consumer as a subprocess against them, and verify
what landed in PostgreSQL matches what should have happened.

Deliberately self-contained (only confluent_kafka + psycopg2 - no
imports from producer/app or ingestion/app): those are two different
packages that both happen to be named "app" internally, so importing
both into one process would collide on the module name "app". Talking
to Kafka/Postgres directly, and running the real consumer as a separate
subprocess, sidesteps that entirely.

What it checks:
  - a normal valid event gets inserted
  - re-sending the exact same event (same event_id) does NOT create a
    second row (idempotency, design.md section 11)
  - an invalid event (bad quantity) is never inserted at all
  - the consumer group's lag reaches 0 (everything got committed)

Requires the full stack running:
    docker compose up -d
    source .venv/bin/activate

Usage:
    python tests/ingestion/test_end_to_end.py
"""

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Producer

RUN_ID = uuid.uuid4().hex[:8]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce-events")
CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "ingestion-service")
INGESTION_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "ingestion")

PG_CONFIG = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
    user=os.environ.get("POSTGRES_USER", "ecommerce"),
    password=os.environ.get("POSTGRES_PASSWORD", "ecommerce"),
)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def eid(n):
    return f"evt_test_e2e_{RUN_ID}_{n}"


VALID_1 = {
    "event_id": eid(1),
    "event_type": "PRODUCT_VIEW",
    "timestamp": now_iso(),
    "user_id": f"user_test_{RUN_ID}",
    "product_id": "prod_test",
}
VALID_2 = {
    "event_id": eid(2),
    "event_type": "USER_SIGNUP",
    "timestamp": now_iso(),
    "user_id": f"user_test_{RUN_ID}",
}
INVALID_1 = {
    "event_id": eid(3),
    "event_type": "ADD_TO_CART",
    "timestamp": now_iso(),
    "user_id": f"user_test_{RUN_ID}",
    "product_id": "prod_test",
    "quantity": "NOT_A_NUMBER",
}

# VALID_1 is sent twice on purpose - exercises idempotent dedup end-to-end.
EVENTS_TO_SEND = [VALID_1, VALID_2, VALID_1, INVALID_1]
EXPECTED_ROWS = 2  # VALID_1 (once, not twice) + VALID_2. INVALID_1 never lands.


def produce_events():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for event in EVENTS_TO_SEND:
        producer.produce(
            KAFKA_TOPIC,
            key=event.get("user_id", "").encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
    producer.flush(10)
    print(f"Produced {len(EVENTS_TO_SEND)} test events (run_id={RUN_ID})")


def get_total_lag():
    """Sum of LAG across all partitions for CONSUMER_GROUP, or None if
    the group/command output couldn't be parsed (e.g. topic not created yet)."""
    result = subprocess.run(
        [
            "docker",
            "exec",
            "ecommerce-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            CONSUMER_GROUP,
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    lag_total = 0
    found_rows = False
    for line in result.stdout.splitlines():
        parts = line.split()
        # Row shape: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        if len(parts) >= 6 and parts[0] == CONSUMER_GROUP:
            try:
                lag_total += int(parts[5])
                found_rows = True
            except ValueError:
                pass
    return lag_total if found_rows else None


def run_ingestion_and_wait_for_drain(timeout=30):
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"],
        cwd=INGESTION_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(f"Started ingestion consumer (pid={proc.pid}), waiting for lag to drain...")

    deadline = time.monotonic() + timeout
    drained = False
    while time.monotonic() < deadline:
        lag = get_total_lag()
        if lag is not None and lag == 0:
            drained = True
            break
        time.sleep(1)

    proc.terminate()
    try:
        output, _ = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        output, _ = proc.communicate()

    return drained, output


def check_database():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT event_id FROM raw.events WHERE event_id LIKE %s ORDER BY event_id",
        (f"evt_test_e2e_{RUN_ID}_%",),
    )
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def main():
    failures = []

    produce_events()

    drained, ingestion_output = run_ingestion_and_wait_for_drain()
    expect(drained, "consumer group lag reached 0 within timeout", failures)

    rows = check_database()
    print(f"Rows found for this test run: {rows}")

    expect(eid(1) in rows, "VALID_1's event_id is present in raw.events", failures)
    expect(eid(2) in rows, "VALID_2's event_id is present in raw.events", failures)
    expect(eid(3) not in rows, "INVALID_1's event_id is NOT present in raw.events", failures)
    expect(
        len(rows) == EXPECTED_ROWS,
        f"exactly {EXPECTED_ROWS} rows landed for this run (got {len(rows)}) - confirms "
        f"the duplicate VALID_1 send did not create a second row",
        failures,
    )

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        print("\n--- ingestion consumer output (tail) ---")
        print("\n".join(ingestion_output.splitlines()[-30:]))
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
