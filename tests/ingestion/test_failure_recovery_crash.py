#!/usr/bin/env python3
"""
Phase 7 failure-recovery test: prove at-least-once + idempotency
together survive a real, abrupt process crash - not a graceful
shutdown, not a caught exception, but SIGKILL. This is a different
failure mode than test_failure_recovery_postgres.py: that one tests
"the dependency this process needs is down"; this one tests "the
process itself dies mid-stream with no chance to clean up."

What it does, end to end:
  1. Produce a batch of known test events to Kafka, all with a unique
     run-scoped event_id.
  2. Start the real ingestion consumer, let it run for a short, fixed
     window (enough to process at least some of the batch, thanks to
     the default 20-message/2-second commit batching from design.md
     section 10.1), then SIGKILL it - no signal handler runs, nothing
     gets a chance to flush.
  3. Whatever subset of events got written before the kill must all be
     present, each exactly once - a crash mid-batch must never produce
     a half-written or duplicated row, since every Postgres write is
     its own transaction (design.md section 11).
  4. Start a fresh consumer instance. Since the kill could have landed
     between "wrote to Postgres" and "offset committed" for its most
     recent 0-19 messages (the pending, not-yet-flushed batch), this
     instance may re-read and re-process some events that were already
     written - that's expected, at-least-once delivery working exactly
     as designed. The ON CONFLICT DO NOTHING insert must make that a
     no-op rather than an error or a duplicate row.
  5. Wait for lag to drain to 0, then confirm ALL events from the batch
     are present in Postgres, and exactly once each.

Note on timing: a SIGKILLed process cannot send Kafka a graceful
LeaveGroupRequest (there's no chance to run any cleanup code at all) -
so the broker only learns the old member is gone once its session
timeout expires (~45s by default) and only then rebalances the
now-abandoned partitions onto the fresh instance. That wait is Kafka
correctly doing its job, not a bug: it's the same mechanism that makes
a genuine mid-processing crash recoverable in production, and it's why
the drain timeout below is generous.

This test does not try to control exactly how many events get processed
before the kill - that's inherently timing-dependent and not the point.
The point is that regardless of how many made it through, nothing is
ever lost and nothing is ever duplicated once the second run finishes.

Requires the full stack running (and PostgreSQL to be healthy - this
test does not touch postgres, only kills the consumer process):
    docker compose up -d
    source .venv/bin/activate

Usage:
    python tests/ingestion/test_failure_recovery_crash.py
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

NUM_EVENTS = 150  # comfortably more than COMMIT_BATCH_SIZE (default 20)
# Rebalancing alone (joining the group, getting partitions assigned)
# typically takes several seconds - RUN_BEFORE_KILL_SECONDS needs to
# clear that AND leave time to actually process some messages, or the
# kill just lands before the consumer has done anything at all.
RUN_BEFORE_KILL_SECONDS = 8.0


def eid(n):
    return f"evt_test_crash_{RUN_ID}_{n}"


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_events():
    return [
        {
            "event_id": eid(n),
            "event_type": "PRODUCT_VIEW",
            "timestamp": now_iso(),
            "user_id": f"user_test_crash_{RUN_ID}_{n % 5}",  # spread across a few keys/partitions
            "product_id": f"prod_test_{n}",
        }
        for n in range(NUM_EVENTS)
    ]


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def produce_events(events):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for event in events:
        producer.produce(
            KAFKA_TOPIC,
            key=event["user_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
    producer.flush(10)
    print(f"Produced {len(events)} test events (run_id={RUN_ID})")


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


def run_and_kill():
    """Start the real consumer, let it run briefly, SIGKILL it (no
    graceful shutdown), and return its output."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"],
        cwd=INGESTION_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(f"Started ingestion consumer (pid={proc.pid}), letting it run for "
          f"{RUN_BEFORE_KILL_SECONDS}s before SIGKILL...")
    time.sleep(RUN_BEFORE_KILL_SECONDS)
    proc.kill()  # SIGKILL - no signal handler, no cleanup, no final flush
    output, _ = proc.communicate(timeout=10)
    print(f"Killed pid={proc.pid}")
    return output


def run_to_drain(timeout=90):
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"],
        cwd=INGESTION_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(f"Started fresh ingestion consumer (pid={proc.pid}) to finish the batch...")

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
    return output, drained


def rows_for_run():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT event_id FROM raw.events WHERE event_id LIKE %s",
        (f"evt_test_crash_{RUN_ID}_%",),
    )
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def main():
    failures = []
    events = build_events()

    produce_events(events)

    output_1 = run_and_kill()
    rows_before_restart = rows_for_run()
    print(
        f"{len(rows_before_restart)}/{NUM_EVENTS} event(s) had landed in Postgres "
        f"before the crash"
    )
    expect(
        len(set(rows_before_restart)) == len(rows_before_restart),
        "no duplicate rows exist even from the pre-crash partial run",
        failures,
    )

    output_2, drained = run_to_drain()
    expect(drained, "consumer group lag reached 0 after restarting post-crash", failures)

    rows_after = rows_for_run()
    expected_ids = {eid(n) for n in range(NUM_EVENTS)}
    found_ids = set(rows_after)

    expect(
        found_ids == expected_ids,
        f"all {NUM_EVENTS} events are present after recovery "
        f"(found {len(found_ids)}, missing {len(expected_ids - found_ids)})",
        failures,
    )
    expect(
        len(rows_after) == NUM_EVENTS,
        f"exactly {NUM_EVENTS} rows exist for this run (found {len(rows_after)}) - "
        f"confirms re-processing overlap after the crash did not create duplicates",
        failures,
    )

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        print("\n--- consumer output before the crash (tail) ---")
        print("\n".join(output_1.splitlines()[-20:]))
        print("\n--- consumer output after restart (tail) ---")
        print("\n".join(output_2.splitlines()[-20:]))
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
