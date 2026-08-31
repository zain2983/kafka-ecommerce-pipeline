#!/usr/bin/env python3
"""
Phase 8 stress test: hammer the pipeline with duplicates far beyond the
single deliberately-repeated event test_end_to_end.py sends, and prove
zero duplicate rows land in Postgres regardless of how the duplicates
are interleaved in time.

What it does:
  1. Builds NUM_UNIQUE events, each repeated DUPLICATES_PER_EVENT times
     (same event_id, sent as separate Kafka messages - simulating
     redelivery, not a single retried send), then shuffles the entire
     list so duplicates are scattered rather than sent back-to-back -
     closer to how real redelivery would actually interleave with other
     traffic than "the same message twice in a row".
  2. Runs the real ingestion consumer with DEDUP_CACHE_SIZE set
     deliberately small (well below NUM_UNIQUE) via an env var override,
     forcing the in-memory dedup cache to evict entries mid-run. This is
     the point of this test versus test_dedup_cache.py's unit-level
     eviction checks: it proves that when the fast-path cache misses a
     duplicate because of eviction, the slow-path Postgres constraint
     (design.md section 11) is still what actually stops it becoming a
     second row - the two mechanisms in main.py/dedup_cache.py described
     in main.py's module docstring (Phase 8) working together for real,
     not just each in isolation.
  3. Confirms: every unique event_id landed exactly once, and the total
     row count for this run equals NUM_UNIQUE - not NUM_UNIQUE *
     DUPLICATES_PER_EVENT.

Requires the full stack running:
    docker compose up -d
    source .venv/bin/activate

Usage:
    python tests/ingestion/test_dedup_stress.py
"""

import json
import os
import random
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

NUM_UNIQUE = 40
DUPLICATES_PER_EVENT = 5  # -> 200 messages total, 160 of them duplicates
DEDUP_CACHE_SIZE_OVERRIDE = "5"  # deliberately smaller than NUM_UNIQUE


def eid(n):
    return f"evt_test_dedupstress_{RUN_ID}_{n}"


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_messages():
    messages = []
    for n in range(NUM_UNIQUE):
        event = {
            "event_id": eid(n),
            "event_type": "PRODUCT_VIEW",
            "timestamp": now_iso(),
            "user_id": f"user_test_dedupstress_{RUN_ID}_{n % 6}",
            "product_id": f"prod_test_{n}",
        }
        messages.extend([event] * DUPLICATES_PER_EVENT)
    random.Random(RUN_ID).shuffle(messages)  # scatter duplicates, not adjacent
    return messages


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def produce_messages(messages):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for event in messages:
        producer.produce(
            KAFKA_TOPIC,
            key=event["user_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
    producer.flush(10)
    print(
        f"Produced {len(messages)} messages ({NUM_UNIQUE} unique event_ids x "
        f"{DUPLICATES_PER_EVENT} copies each, shuffled) (run_id={RUN_ID})"
    )


def get_total_lag():
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
        if len(parts) >= 6 and parts[0] == CONSUMER_GROUP:
            try:
                lag_total += int(parts[5])
                found_rows = True
            except ValueError:
                pass
    return lag_total if found_rows else None


def run_ingestion_and_wait_for_drain(timeout=30):
    env = dict(os.environ, DEDUP_CACHE_SIZE=DEDUP_CACHE_SIZE_OVERRIDE)
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"],
        cwd=INGESTION_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(
        f"Started ingestion consumer (pid={proc.pid}, DEDUP_CACHE_SIZE="
        f"{DEDUP_CACHE_SIZE_OVERRIDE}), waiting for lag to drain..."
    )

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


def rows_for_run():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT event_id FROM raw.events WHERE event_id LIKE %s",
        (f"evt_test_dedupstress_{RUN_ID}_%",),
    )
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def main():
    failures = []
    messages = build_messages()

    produce_messages(messages)

    drained, output = run_ingestion_and_wait_for_drain()
    expect(drained, "consumer group lag reached 0 within timeout", failures)

    rows = rows_for_run()
    expected_ids = {eid(n) for n in range(NUM_UNIQUE)}
    found_ids = set(rows)

    expect(
        found_ids == expected_ids,
        f"all {NUM_UNIQUE} unique event_ids are present "
        f"(found {len(found_ids)}, missing {len(expected_ids - found_ids)})",
        failures,
    )
    expect(
        len(rows) == NUM_UNIQUE,
        f"exactly {NUM_UNIQUE} rows exist for this run (found {len(rows)}) - "
        f"proves none of the {len(messages) - NUM_UNIQUE} duplicate messages became "
        f"a second row, cache-caught or DB-caught",
        failures,
    )

    # Not a correctness assertion, just confirming the test actually
    # exercised both code paths described in main.py's module docstring
    # rather than one of them going untouched by luck of the timing.
    saw_cache_dup = "cache hit" in output
    saw_db_dup = "DB caught" in output
    print(
        f"(info) fast-path cache catches observed: {saw_cache_dup}, "
        f"DB-constraint catches observed: {saw_db_dup}"
    )

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        print("\n--- ingestion consumer output (tail) ---")
        print("\n".join(output.splitlines()[-40:]))
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
