#!/usr/bin/env python3
"""
Phase 12 failure-recovery test: the broker itself going down, not one of
its clients. Every earlier failure-recovery test kills Postgres or the
ingestion consumer process - none of them kills Kafka, the one piece of
infrastructure BOTH the producer and the consumer depend on.

What it does, end to end:
  1. Starts the real producer and the real ingestion consumer as
     subprocesses, both against the live topic.
  2. Lets them run briefly to establish a steady flow.
  3. Stops the ecommerce-kafka container.
  4. Confirms BOTH processes are still alive after the outage (no crash)
     - librdkafka retries broker connections internally and neither
     Python process ever sees or has to handle an exception for "the
     broker is down"; a naive implementation could still crash on this
     if something in main.py's loop didn't expect poll() to keep
     returning None indefinitely.
  5. Restarts ecommerce-kafka and waits for its healthcheck.
  6. Confirms full recovery two ways: the producer's sent/delivered
     counts converge (nothing permanently lost - messages produced
     during the outage were queued by librdkafka, not dropped, since
     the outage is well under message.timeout.ms), and a handful of
     freshly-produced, uniquely-tagged test events land in raw.events
     within a generous window - proving the consumer is genuinely back
     to normal, not just alive.

     Deliberately NOT checked: Kafka-reported consumer-group lag
     hitting exactly 0. That metric reflects this project's own
     BATCHED offset commits (design.md section 10.1) rather than actual
     processing progress - "lag > 0" can be true for up to
     COMMIT_INTERVAL_SECONDS after every message is already safely in
     Postgres, which made it a flaky, misleading signal for "has this
     recovered" during testing. Checking Postgres directly for known
     rows is what the other failure-recovery tests do too, and doesn't
     have that problem.

This mirrors test_failure_recovery_postgres.py's shape but for a
different dependency - see that file for the Postgres-outage case, and
design.md section 25 for the general failure-flow model both follow.

Requires the full stack running:
    docker compose up -d
    source .venv/bin/activate

Usage:
    python tests/ingestion/test_failure_recovery_kafka.py

Note: this stops/restarts the shared ecommerce-kafka container, so
don't run it while relying on Kafka for something else at the same
time. It always restarts Kafka before exiting, even on failure.
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

PRODUCER_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "producer")
INGESTION_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "ingestion")
KAFKA_CONTAINER = "ecommerce-kafka"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce-events")

PG_CONFIG = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
    user=os.environ.get("POSTGRES_USER", "ecommerce"),
    password=os.environ.get("POSTGRES_PASSWORD", "ecommerce"),
)

RUN_ID = uuid.uuid4().hex[:8]
OUTAGE_SECONDS = 15
RUN_BEFORE_OUTAGE_SECONDS = 5
NUM_RECOVERY_PROBE_EVENTS = 5
# A consumer that was already joined to the group before the outage
# doesn't notice its coordinator is gone until its own session timeout
# elapses (~45s by default, client-side - see kafka_consumer.py) and
# only THEN rejoins/rebalances - and empirically that 45s clock starts
# counting from whenever the coordinator connection was last healthy,
# which can be well before Kafka's own healthcheck reports "healthy"
# again (a restarting broker can accept admin/metadata calls before its
# group-coordinator subsystem is fully stable for pre-existing
# sessions).
#
# What this test cares about is CORRECTNESS of eventual recovery, not
# its speed - there's no requirement anywhere in this project that
# recovery from a broker outage happen within any particular time
# budget, only that it happens (design.md section 25). Chasing an exact
# "tight enough but not flaky" timeout across several runs (150s, then
# 240s, then 300s - each time recovery had genuinely already succeeded,
# just a little later than the deadline) was solving the wrong problem.
# This is deliberately generous instead: long enough that hitting it is
# itself informative (something is actually stuck, not just slow).
RECOVERY_TIMEOUT_SECONDS = 600


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def docker(*args, timeout=30):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


def stop_kafka():
    print(f"Stopping {KAFKA_CONTAINER}...")
    docker("stop", KAFKA_CONTAINER)


def start_kafka_and_wait(timeout=60):
    print(f"Starting {KAFKA_CONTAINER}...")
    docker("start", KAFKA_CONTAINER)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = docker("inspect", KAFKA_CONTAINER, "--format", "{{.State.Health.Status}}")
        if result.stdout.strip() == "healthy":
            # The healthcheck itself (kafka-topics.sh --list) only
            # proves the admin/metadata API is answering - empirically,
            # a just-restarted broker's group-coordinator subsystem
            # isn't necessarily stable enough yet for a PRE-EXISTING
            # consumer session to reconnect at that exact moment, which
            # was making already-joined consumers churn through repeated
            # connection resets before finally recovering. A short fixed
            # buffer here avoids racing that internal stabilization.
            print("Kafka is healthy again, waiting a bit longer for the coordinator to settle...")
            time.sleep(15)
            return True
        time.sleep(2)
    return False


def probe_event_id(n):
    return f"evt_test_kafkafail_{RUN_ID}_{n}"


def produce_recovery_probe_events():
    """
    A handful of known, uniquely-tagged events produced AFTER Kafka is
    confirmed healthy again - proof the consumer is genuinely processing
    normally again, independent of however long the historical backlog
    from before the outage takes to fully settle.
    """
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for n in range(NUM_RECOVERY_PROBE_EVENTS):
        event = {
            "event_id": probe_event_id(n),
            "event_type": "PRODUCT_VIEW",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user_id": f"user_test_kafkafail_{RUN_ID}",
            "product_id": "prod_test",
        }
        producer.produce(
            KAFKA_TOPIC,
            key=event["user_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
    producer.flush(10)
    print(f"Produced {NUM_RECOVERY_PROBE_EVENTS} recovery-probe events (run_id={RUN_ID})")


def count_probe_events_in_db():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM raw.events WHERE event_id LIKE %s",
        (f"evt_test_kafkafail_{RUN_ID}_%",),
    )
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def parse_producer_counts(output: str):
    """Pulls the last 'sent=N delivered=N failed=N' line from producer output."""
    sent = delivered = failed = None
    for line in output.splitlines():
        if "sent=" in line and "delivered=" in line:
            try:
                parts = {p.split("=")[0]: int(p.split("=")[1]) for p in line.split() if "=" in p}
                sent, delivered, failed = parts.get("sent"), parts.get("delivered"), parts.get("failed")
            except (ValueError, IndexError):
                pass
    return sent, delivered, failed


def main():
    failures = []
    # Line-buffer this script's own stdout so its narration ("Stopping
    # ecommerce-kafka...", heartbeats, etc.) is visible in real time to
    # anyone tailing its output, not just at the end - same reasoning
    # as the subprocess logging setup below.
    sys.stdout.reconfigure(line_buffering=True)

    # Subprocess output goes straight to a file (not a PIPE only read at
    # the end via communicate()) so it can be tailed live while this
    # runs - a previous run of this test appeared to hang with "zero
    # output" for 13+ minutes, which turned out to be unresolvable
    # after the fact: PIPE output is invisible until communicate() is
    # called, so there was no way to tell "genuinely produced nothing"
    # apart from "produced plenty, we just can't see it yet." This also
    # sets PYTHONUNBUFFERED so the subprocess's own stdio doesn't add a
    # second layer of buffering on top.
    consumer_log_path = "/tmp/test_failure_recovery_kafka_consumer.log"
    producer_log_path = "/tmp/test_failure_recovery_kafka_producer.log"
    subprocess_env = dict(os.environ, PYTHONUNBUFFERED="1")

    consumer_log = open(consumer_log_path, "w")
    consumer_proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"],
        cwd=INGESTION_DIR,
        env=subprocess_env,
        stdout=consumer_log,
        stderr=subprocess.STDOUT,
    )
    print(f"Started ingestion consumer (pid={consumer_proc.pid}, log: {consumer_log_path})")

    producer_env = dict(subprocess_env, EVENTS_PER_SECOND="15")
    producer_log = open(producer_log_path, "w")
    producer_proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"],
        cwd=PRODUCER_DIR,
        env=producer_env,
        stdout=producer_log,
        stderr=subprocess.STDOUT,
    )
    print(f"Started producer (pid={producer_proc.pid}, log: {producer_log_path})")

    def _stop(proc, timeout=15):
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()

    def _read_log(path):
        try:
            with open(path) as f:
                return f.read()
        except OSError:
            return ""

    producer_output = None
    try:
        time.sleep(RUN_BEFORE_OUTAGE_SECONDS)

        stop_kafka()
        time.sleep(OUTAGE_SECONDS)

        expect(
            consumer_proc.poll() is None,
            f"ingestion consumer is still alive after a {OUTAGE_SECONDS}s Kafka outage (no crash)",
            failures,
        )
        expect(
            producer_proc.poll() is None,
            f"producer is still alive after a {OUTAGE_SECONDS}s Kafka outage (no crash)",
            failures,
        )

        kafka_up = start_kafka_and_wait()
        expect(kafka_up, "Kafka became healthy again after restart", failures)

        _stop(producer_proc)
        producer_output = _read_log(producer_log_path)

        if kafka_up:
            produce_recovery_probe_events()
            deadline = time.monotonic() + RECOVERY_TIMEOUT_SECONDS
            recovered = False
            last_heartbeat = time.monotonic()
            while time.monotonic() < deadline:
                if count_probe_events_in_db() == NUM_RECOVERY_PROBE_EVENTS:
                    recovered = True
                    break
                if time.monotonic() - last_heartbeat >= 15:
                    remaining = int(deadline - time.monotonic())
                    consumer_alive = consumer_proc.poll() is None
                    print(
                        f"  ... still waiting ({remaining}s left), "
                        f"consumer alive={consumer_alive}, "
                        f"found={count_probe_events_in_db()}/{NUM_RECOVERY_PROBE_EVENTS}",
                        flush=True,
                    )
                    last_heartbeat = time.monotonic()
                time.sleep(2)
            expect(
                recovered,
                f"all {NUM_RECOVERY_PROBE_EVENTS} recovery-probe events landed in raw.events "
                f"within {RECOVERY_TIMEOUT_SECONDS}s of Kafka recovering (found "
                f"{count_probe_events_in_db()})",
                failures,
            )

    finally:
        if producer_output is None:
            _stop(producer_proc)
            producer_output = _read_log(producer_log_path)
        _stop(consumer_proc)
        consumer_output = _read_log(consumer_log_path)

        # Always leave Kafka running, even if an assertion above failed
        # partway through - this is a shared container other tests/tools
        # depend on.
        start_kafka_and_wait()

    sent, delivered, failed = parse_producer_counts(producer_output)
    print(f"Producer final counts: sent={sent} delivered={delivered} failed={failed}")
    expect(
        sent is not None and delivered == sent and (failed or 0) == 0,
        f"producer delivered every message it sent, none permanently failed "
        f"(sent={sent}, delivered={delivered}, failed={failed})",
        failures,
    )

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        print("\n--- consumer output (tail) ---")
        print("\n".join(consumer_output.splitlines()[-20:]))
        print("\n--- producer output (tail) ---")
        print("\n".join(producer_output.splitlines()[-20:]))
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
