#!/usr/bin/env python3
"""
Phase 7 failure-recovery test: prove the design.md section 10/13/25
"transient PostgreSQL outage" flow actually holds under a real outage,
not just in the retry-loop code reading correctly.

What it does, end to end:
  1. Produce one known test event to Kafka.
  2. Stop the postgres container BEFORE starting the consumer, so every
     DB write attempt is guaranteed to fail.
  3. Start the real ingestion consumer. It should retry
     DB_RETRY_ATTEMPTS times (main.py), give up, flush any offsets it
     had already confirmed (none, here), and exit on its own WITHOUT
     committing this message's offset.
  4. Confirm: the process actually exited (not hung), the event was
     never written to Postgres, and the consumer group's lag is still
     1 (i.e. nothing was committed - the message is still there to
     re-read).
  5. Restart postgres, wait for it to accept connections again.
  6. Start the consumer again. Since nothing was committed, it must
     re-read the exact same message from the same offset and succeed
     this time.
  7. Confirm: the event now exists in Postgres, exactly once, and lag
     has drained to 0.

This is the automated version of "kill postgres mid-stream and see what
happens" - the same scenario design.md section 25 describes for
transient failures, but actually exercised instead of just documented.

Requires the full stack running:
    docker compose up -d
    source .venv/bin/activate

Usage:
    python tests/ingestion/test_failure_recovery_postgres.py

Note: this stops/restarts the shared ecommerce-postgres container, so
don't run it while relying on postgres for something else at the same
time. It always restarts postgres before exiting (even on failure), so
the container is left running afterward either way.
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
POSTGRES_CONTAINER = "ecommerce-postgres"

PG_CONFIG = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
    user=os.environ.get("POSTGRES_USER", "ecommerce"),
    password=os.environ.get("POSTGRES_PASSWORD", "ecommerce"),
)

EVENT_ID = f"evt_test_pgfail_{RUN_ID}_1"
TEST_EVENT = {
    "event_id": EVENT_ID,
    "event_type": "PURCHASE",
    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "user_id": f"user_test_pgfail_{RUN_ID}",
    "product_id": "prod_test",
    "quantity": 1,
    "unit_price": 9.99,
}


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def produce_test_event():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    producer.produce(
        KAFKA_TOPIC,
        key=TEST_EVENT["user_id"].encode("utf-8"),
        value=json.dumps(TEST_EVENT).encode("utf-8"),
    )
    producer.flush(10)
    print(f"Produced 1 test event (run_id={RUN_ID}, event_id={EVENT_ID})")


def docker(*args, timeout=30):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


def stop_postgres():
    print(f"Stopping {POSTGRES_CONTAINER}...")
    docker("stop", POSTGRES_CONTAINER)


def start_postgres_and_wait(timeout=30):
    print(f"Starting {POSTGRES_CONTAINER}...")
    docker("start", POSTGRES_CONTAINER)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = docker("exec", POSTGRES_CONTAINER, "pg_isready", "-U", PG_CONFIG["user"])
        if result.returncode == 0:
            print("postgres is accepting connections again")
            return True
        time.sleep(1)
    return False


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


def run_ingestion(wait_for_exit_seconds=None, wait_for_lag_zero_seconds=None):
    """
    Start the real ingestion consumer as a subprocess.

    If wait_for_exit_seconds is set, wait for the process to exit ON ITS
    OWN (the give-up-after-retries path) and return (output, exited).
    If wait_for_lag_zero_seconds is set instead, poll consumer-group lag
    until it hits 0, then terminate the process gracefully and return
    (output, drained).
    """
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"],
        cwd=INGESTION_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(f"Started ingestion consumer (pid={proc.pid})")

    if wait_for_exit_seconds is not None:
        try:
            proc.wait(timeout=wait_for_exit_seconds)
            exited = True
        except subprocess.TimeoutExpired:
            proc.kill()
            exited = False
        output, _ = proc.communicate()
        return output, exited

    deadline = time.monotonic() + wait_for_lag_zero_seconds
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


def event_in_db():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw.events WHERE event_id = %s", (EVENT_ID,))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def main():
    failures = []

    produce_test_event()

    stop_postgres()
    # Give the container a moment to actually stop accepting connections
    # before we start the consumer against it.
    time.sleep(2)

    # DB_RETRY_ATTEMPTS=5, DB_RETRY_BACKOFF_SECONDS=2 in main.py -> the
    # give-up path takes on the order of ~10s. 60s is a generous ceiling.
    output_1, exited_on_its_own = run_ingestion(wait_for_exit_seconds=60)
    expect(
        exited_on_its_own,
        "consumer gave up and exited on its own (did not hang) with postgres down",
        failures,
    )
    expect(
        "Could not connect to Postgres after" in output_1,
        "consumer logged the startup 'could not connect after N attempts' message",
        failures,
    )

    lag_while_down = get_total_lag()
    expect(
        lag_while_down is not None and lag_while_down >= 1,
        f"offset was NOT committed while postgres was down (lag={lag_while_down}, expected >= 1)",
        failures,
    )

    postgres_up = start_postgres_and_wait()
    expect(postgres_up, "postgres became reachable again after restart", failures)

    if postgres_up:
        output_2, drained = run_ingestion(wait_for_lag_zero_seconds=30)
        expect(drained, "consumer group lag reached 0 after postgres recovered", failures)

        count = event_in_db()
        expect(
            count == 1,
            f"the event was written exactly once after recovery (found {count} row(s)) - "
            f"proves the message was safely re-processed, not lost and not duplicated",
            failures,
        )
    else:
        output_2 = ""

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        print("\n--- consumer output while postgres was down (tail) ---")
        print("\n".join(output_1.splitlines()[-20:]))
        print("\n--- consumer output after postgres recovered (tail) ---")
        print("\n".join(output_2.splitlines()[-20:]))
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    try:
        main()
    finally:
        # Always leave postgres running, even if an assertion above
        # failed partway through - this is a shared container other
        # tests/tools depend on.
        start_postgres_and_wait()
