#!/usr/bin/env python3
"""
Operational tool (Phase 9): republish messages sitting in
ecommerce-events-dlq so they get a real second attempt.

`inspect_kafka_topic.py --topic ecommerce-events-dlq --show` already
covers *looking* at the DLQ (it's topic-agnostic). This tool is for
*acting* on what you find there - manually, one run at a time. It never
runs automatically and there is no "auto-replay" mode on a schedule;
replaying is always a deliberate operator decision, which is also why
this republishes to ecommerce-events-retry rather than straight back to
ecommerce-events-dlq's original topic: retry_main.py (a separate,
distinctly-named consumer group) is what actually re-attempts it, so a
replay is never silently indistinguishable from ordinary first-time
traffic in the metrics/logs.

Usage:
    # See what's in the DLQ without touching anything
    python tests/kafka/replay_dlq.py --dry-run

    # Replay everything currently in the DLQ
    python tests/kafka/replay_dlq.py

    # Replay only one record by its dlq_id (see inspect_kafka_topic.py
    # --topic ecommerce-events-dlq --show to find one)
    python tests/kafka/replay_dlq.py --dlq-id cdc90262177f460a88b9da19a7287823

    # Only replay records still below RETRY_MAX_ATTEMPTS retry_count
    # (matches retry_main.py's default; pass --max-retry-count to match
    # a different RETRY_MAX_ATTEMPTS if you've overridden it)
    python tests/kafka/replay_dlq.py --skip-exhausted

This is a read-then-produce tool, not a consumer with an offset to
manage: it reads the DLQ topic from the beginning with a throwaway
group id every run (same pattern as inspect_kafka_topic.py) and simply
republishes whatever matches your filters - it does NOT delete or mark
anything in the DLQ, so re-running with the same filters will republish
the same records again. That's intentional (Kafka topics are logs, not
queues you dequeue from) - dedup on the way back in is retry_main.py and
the DB constraint's job, not this tool's.
"""

import argparse
import json
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--dlq-topic", default="ecommerce-events-dlq")
    p.add_argument("--retry-topic", default="ecommerce-events-retry")
    p.add_argument("--dlq-id", default=None, help="Only replay this one dlq_id")
    p.add_argument(
        "--skip-exhausted",
        action="store_true",
        help="Skip records whose retry_count is already >= --max-retry-count",
    )
    p.add_argument("--max-retry-count", type=int, default=3, help="See --skip-exhausted")
    p.add_argument(
        "--dry-run", action="store_true", help="List what would be replayed, but don't produce anything"
    )
    p.add_argument("--idle-timeout", type=float, default=10.0)
    return p.parse_args()


def read_dlq_records(bootstrap_servers: str, topic: str, idle_timeout: float):
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"dlq-replay-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(topic=topic, timeout=10)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        print(f"Topic '{topic}' not found on the cluster.")
        consumer.close()
        sys.exit(1)

    partitions = []
    for partition_id in metadata.topics[topic].partitions:
        low, _ = consumer.get_watermark_offsets(
            TopicPartition(topic, partition_id), timeout=10, cached=False
        )
        partitions.append(TopicPartition(topic, partition_id, low))
    consumer.assign(partitions)

    records = []
    last_msg_time = time.monotonic()
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if time.monotonic() - last_msg_time > idle_timeout:
                break
            continue
        if msg.error():
            continue
        last_msg_time = time.monotonic()
        try:
            record = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            print(f"(skipping unparseable DLQ record at offset {msg.offset()})")
            continue
        record["_dlq_key"] = msg.key().decode("utf-8") if msg.key() else None
        records.append(record)

    consumer.close()
    return records


def main():
    args = parse_args()

    print(f"Reading {args.dlq_topic}...")
    records = read_dlq_records(args.bootstrap_servers, args.dlq_topic, args.idle_timeout)
    print(f"Found {len(records)} record(s) in the DLQ.\n")

    selected = []
    for record in records:
        if args.dlq_id and record.get("dlq_id") != args.dlq_id:
            continue
        if args.skip_exhausted and record.get("retry_count", 0) >= args.max_retry_count:
            continue
        selected.append(record)

    if not selected:
        print("No records match the given filters. Nothing to replay.")
        return

    for record in selected:
        print(
            f"  dlq_id={record.get('dlq_id')}  reason={record.get('reason')}  "
            f"retry_count={record.get('retry_count')}  "
            f"original=[{record.get('original_topic')} p{record.get('original_partition')} "
            f"o{record.get('original_offset')}]"
        )

    if args.dry_run:
        print(f"\n(dry run - {len(selected)} record(s) would be replayed to {args.retry_topic})")
        return

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    for record in selected:
        producer.produce(
            args.retry_topic,
            key=record["_dlq_key"].encode("utf-8") if record.get("_dlq_key") else None,
            value=json.dumps({k: v for k, v in record.items() if k != "_dlq_key"}).encode("utf-8"),
        )
    producer.flush(10)
    print(f"\nReplayed {len(selected)} record(s) to {args.retry_topic}.")


if __name__ == "__main__":
    main()
