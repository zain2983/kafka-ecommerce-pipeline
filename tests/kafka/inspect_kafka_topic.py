#!/usr/bin/env python3
"""
Manual inspection tool for a Kafka topic (defaults to ecommerce-events).

Always prints a per-partition summary: which broker leads each partition,
and how many messages currently exist in that partition (high watermark -
low watermark). Optionally also prints the actual messages.

This uses a randomly generated, throwaway consumer group id every run, so
running it never affects the offsets of the real ingestion consumer
(Phase 4+) - it's a pure observer, like tailing a log file.

Usage:
    # Summary only: partition counts, leader broker, message totals
    python tests/kafka/inspect_kafka_topic.py

    # Also print the actual messages, oldest first
    python tests/kafka/inspect_kafka_topic.py --show --from-beginning --max-messages 20

    # Only look at one partition
    python tests/kafka/inspect_kafka_topic.py --show --from-beginning --partition 1 --max-messages 10

    # Watch for new messages as they arrive (like `tail -f`)
    python tests/kafka/inspect_kafka_topic.py --show --max-messages 1000 --idle-timeout 30
"""

import argparse
import json
import sys
import time
import uuid

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="ecommerce-events")
    p.add_argument(
        "--show", action="store_true", help="Also print individual messages, not just the summary"
    )
    p.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from the earliest offset instead of only new messages",
    )
    p.add_argument("--partition", type=int, default=None, help="Restrict to a single partition")
    p.add_argument(
        "--max-messages", type=int, default=20, help="Stop after printing this many messages"
    )
    p.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help=(
            "Stop after this many seconds with no new messages. Note: when "
            "reading all partitions (no --partition given), the consumer "
            "group must first rebalance and get partitions assigned, which "
            "can itself take a couple of seconds - keep this comfortably "
            "above that."
        ),
    )
    return p.parse_args()


def print_summary(admin: AdminClient, consumer: Consumer, topic: str, partition_filter):
    metadata = admin.list_topics(topic=topic, timeout=10)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        print(f"Topic '{topic}' not found on the cluster.")
        sys.exit(1)

    topic_meta = metadata.topics[topic]
    print(f"Topic: {topic}")
    print(f"Partition count: {len(topic_meta.partitions)}\n")

    header = f"{'Partition':>9}  {'Leader Broker':>13}  {'Low Offset':>10}  {'High Offset':>11}  {'Messages':>9}"
    print(header)
    print("-" * len(header))

    total = 0
    for partition_id, partition_meta in sorted(topic_meta.partitions.items()):
        if partition_filter is not None and partition_id != partition_filter:
            continue
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition_id), timeout=10, cached=False
        )
        count = high - low
        total += count
        print(
            f"{partition_id:>9}  {partition_meta.leader:>13}  {low:>10}  {high:>11}  {count:>9}"
        )

    print(f"\nTotal messages currently in topic: {total}")
    print(
        "(All partitions show the same leader broker id here because this is a "
        "single-broker cluster - with multiple brokers, leadership would be "
        "spread across them.)"
    )


def show_messages(
    consumer: Consumer,
    topic: str,
    partition_filter,
    from_beginning: bool,
    max_messages: int,
    idle_timeout: float,
):
    if partition_filter is not None:
        # Passing the desired offset directly into assign() (rather than
        # assign() then seek()) avoids a librdkafka state error - seek()
        # requires the partition to already be an active fetch target.
        if from_beginning:
            low, _ = consumer.get_watermark_offsets(
                TopicPartition(topic, partition_filter), timeout=10, cached=False
            )
            consumer.assign([TopicPartition(topic, partition_filter, low)])
        else:
            consumer.assign([TopicPartition(topic, partition_filter)])
    else:
        consumer.subscribe([topic])

    print(f"\n--- Messages (max {max_messages}, idle timeout {idle_timeout}s) ---\n")

    seen = 0
    last_msg_time = time.monotonic()
    while seen < max_messages:
        msg = consumer.poll(1.0)
        if msg is None:
            if time.monotonic() - last_msg_time > idle_timeout:
                print(f"(no new messages for {idle_timeout}s, stopping)")
                break
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        last_msg_time = time.monotonic()
        seen += 1

        key = msg.key().decode("utf-8") if msg.key() else None
        raw_value = msg.value().decode("utf-8", errors="replace") if msg.value() else None
        try:
            value_str = json.dumps(json.loads(raw_value))
        except (TypeError, ValueError):
            value_str = raw_value

        print(f"[partition {msg.partition()} | offset {msg.offset()}] key={key}  value={value_str}")

    print(f"\nPrinted {seen} message(s).")


def main():
    args = parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})

    group_id = f"inspector-{uuid.uuid4().hex[:8]}"
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )

    print_summary(admin, consumer, args.topic, args.partition)

    if args.show:
        show_messages(
            consumer,
            args.topic,
            args.partition,
            args.from_beginning,
            args.max_messages,
            args.idle_timeout,
        )

    consumer.close()


if __name__ == "__main__":
    main()
