#!/usr/bin/env python3
"""
Phase 9 full-pipeline test: DLQ routing, replay, and recovery.

What it does, end to end:
  1. Produces one unparseable message and one validation-failing message
     (bad quantity) straight to ecommerce-events.
  2. Runs the real ingestion consumer (main.py). Confirms:
       - neither event ever lands in raw.events
       - both show up as records on ecommerce-events-dlq, with the
         right reason/original offset/retry_count=0
  3. Replays both DLQ records to ecommerce-events-retry unchanged (same
     bad payloads - simulating "try again without fixing anything").
     Runs the real retry consumer (retry_main.py). Confirms:
       - both still fail (nothing about the bad data changed)
       - both are back on the DLQ with retry_count=1 - proving a failed
         retry attempt re-enters the DLQ rather than vanishing or
         blocking the retry topic
  4. Publishes a THIRD record straight to ecommerce-events-retry with a
     corrected payload (simulating an operator fixing the bad quantity
     before replaying) and runs retry_main.py again. Confirms the fixed
     event lands in raw.events exactly once - the DLQ -> fix -> retry ->
     recovered path working for real, not just each half in isolation.

Requires the full stack running, including the DLQ/retry topics:
    docker compose up -d
    docker exec ecommerce-kafka bash -c '...create_topics.sh...'  (see README)
    source .venv/bin/activate

Usage:
    python tests/ingestion/test_dlq_flow.py
"""

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, Producer, TopicPartition

RUN_ID = uuid.uuid4().hex[:8]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce-events")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "ecommerce-events-dlq")
RETRY_TOPIC = os.environ.get("KAFKA_RETRY_TOPIC", "ecommerce-events-retry")
MAIN_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "ingestion-service")
RETRY_GROUP = os.environ.get("KAFKA_RETRY_CONSUMER_GROUP", "retry-service")
INGESTION_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "ingestion")

PG_CONFIG = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
    user=os.environ.get("POSTGRES_USER", "ecommerce"),
    password=os.environ.get("POSTGRES_PASSWORD", "ecommerce"),
)

USER_ID = f"user_test_dlq_{RUN_ID}"
BAD_EVENT_ID = f"evt_test_dlq_{RUN_ID}_1"
BAD_EVENT = {
    "event_id": BAD_EVENT_ID,
    "event_type": "ADD_TO_CART",
    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "user_id": USER_ID,
    "product_id": "prod_test",
    "quantity": "NOT_A_NUMBER",
}
UNPARSEABLE_PAYLOAD = f'{{"not-valid-json-{RUN_ID}'


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def produce_bad_events():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    producer.produce(KAFKA_TOPIC, key=USER_ID.encode(), value=json.dumps(BAD_EVENT).encode())
    producer.produce(KAFKA_TOPIC, key=USER_ID.encode(), value=UNPARSEABLE_PAYLOAD.encode())
    producer.flush(10)
    print(f"Produced 1 invalid + 1 unparseable event to {KAFKA_TOPIC} (run_id={RUN_ID})")


def get_total_lag(group):
    result = subprocess.run(
        [
            "docker", "exec", "ecommerce-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "localhost:9092",
            "--describe", "--group", group,
        ],
        capture_output=True, text=True, timeout=15,
    )
    lag_total = 0
    found_rows = False
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] == group:
            try:
                lag_total += int(parts[5])
                found_rows = True
            except ValueError:
                pass
    return lag_total if found_rows else None


def run_and_wait_for_drain(module: str, group: str, timeout=30):
    proc = subprocess.Popen(
        [sys.executable, "-m", module],
        cwd=INGESTION_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(f"Started {module} (pid={proc.pid}), waiting for group={group} lag to drain...")

    deadline = time.monotonic() + timeout
    drained = False
    while time.monotonic() < deadline:
        lag = get_total_lag(group)
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


def read_dlq_records_for_run():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"dlq-test-reader-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(topic=DLQ_TOPIC, timeout=10)
    partitions = []
    for partition_id in metadata.topics[DLQ_TOPIC].partitions:
        low, _ = consumer.get_watermark_offsets(
            TopicPartition(DLQ_TOPIC, partition_id), timeout=10, cached=False
        )
        partitions.append(TopicPartition(DLQ_TOPIC, partition_id, low))
    consumer.assign(partitions)

    records = []
    last_msg_time = time.monotonic()
    while time.monotonic() - last_msg_time < 5:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        last_msg_time = time.monotonic()
        try:
            record = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if record.get("original_topic") == KAFKA_TOPIC and RUN_ID in record.get("raw_payload", ""):
            records.append(record)
    consumer.close()
    return records


def event_in_db(event_id):
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw.events WHERE event_id = %s", (event_id,))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def replay_to_retry_topic(records):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for record in records:
        producer.produce(RETRY_TOPIC, key=USER_ID.encode(), value=json.dumps(record).encode())
    producer.flush(10)


def main():
    failures = []

    produce_bad_events()

    drained, output_1 = run_and_wait_for_drain("app.main", MAIN_GROUP)
    expect(drained, "main consumer's lag reached 0 (both bad events were handled)", failures)

    expect(event_in_db(BAD_EVENT_ID) == 0, "the invalid event never landed in raw.events", failures)

    dlq_records = read_dlq_records_for_run()
    expect(
        len(dlq_records) == 2,
        f"exactly 2 DLQ records were created for this run (found {len(dlq_records)})",
        failures,
    )
    reasons = {r.get("reason") for r in dlq_records}
    expect(
        reasons == {"VALIDATION_FAILED", "UNPARSEABLE"},
        f"DLQ records cover both rejection reasons (found {reasons})",
        failures,
    )
    expect(
        all(r.get("retry_count") == 0 for r in dlq_records),
        "both DLQ records start at retry_count=0",
        failures,
    )

    if len(dlq_records) != 2:
        print("\nCannot continue to the replay stage without both DLQ records - stopping here.")
        print(f"{len(failures)} test(s) FAILED")
        sys.exit(1)

    # --- Replay unchanged: both should fail again and bounce back to the DLQ ---
    replay_to_retry_topic(dlq_records)
    drained_2, output_2 = run_and_wait_for_drain("app.retry_main", RETRY_GROUP)
    expect(drained_2, "retry consumer's lag reached 0 after replaying unchanged records", failures)

    expect(
        event_in_db(BAD_EVENT_ID) == 0,
        "the invalid event STILL never landed in raw.events after an unfixed replay",
        failures,
    )

    dlq_records_after_replay = read_dlq_records_for_run()
    requeued = [r for r in dlq_records_after_replay if r.get("retry_count") == 1]
    expect(
        len(requeued) == 2,
        f"both records reappeared on the DLQ with retry_count=1 after failing again "
        f"(found {len(requeued)})",
        failures,
    )

    # --- Replay with a fix: the invalid event should now succeed ---
    fixed_event = dict(BAD_EVENT)
    fixed_event["quantity"] = 3
    fixed_record = {
        "dlq_id": f"test-fixed-{RUN_ID}",
        "reason": "VALIDATION_FAILED",
        "errors": [],
        "original_topic": KAFKA_TOPIC,
        "original_partition": -1,
        "original_offset": -1,
        "raw_payload": json.dumps(fixed_event),
        "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "retry_count": 1,
    }
    replay_to_retry_topic([fixed_record])
    drained_3, output_3 = run_and_wait_for_drain("app.retry_main", RETRY_GROUP)
    expect(drained_3, "retry consumer's lag reached 0 after replaying the FIXED record", failures)

    expect(
        event_in_db(BAD_EVENT_ID) == 1,
        "the fixed event landed in raw.events exactly once after recovery",
        failures,
    )

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        print("\n--- main.py output (tail) ---")
        print("\n".join(output_1.splitlines()[-20:]))
        print("\n--- retry_main.py output, unfixed replay (tail) ---")
        print("\n".join(output_2.splitlines()[-20:]))
        print("\n--- retry_main.py output, fixed replay (tail) ---")
        print("\n".join(output_3.splitlines()[-20:]))
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
