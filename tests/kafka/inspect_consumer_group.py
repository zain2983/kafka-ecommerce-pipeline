#!/usr/bin/env python3
"""
Manual inspection tool for a Kafka consumer group (defaults to
ingestion-service, the real ingestion consumer's group.id).

Prints two things:

1. Group membership: every member currently in the group (identified by
   the client.id set in kafka_consumer.py's KafkaConsumerConfig.instance_id),
   which partitions each one owns, and the group's overall state
   (STABLE, REBALANCING, EMPTY, ...). This is what makes Kafka's
   partition-to-consumer assignment visible (design.md section 26) -
   run two `python -m app.main` processes with the same
   KAFKA_CONSUMER_GROUP at once and you'll see the 3 ecommerce-events
   partitions split across them here.

2. Per-partition lag: committed offset vs. high watermark for each
   partition. Lag = how many messages exist in the topic that this
   group hasn't yet committed past - i.e. its backlog. A healthy,
   caught-up consumer should show lag near 0 once traffic stops.

This is a read-only observer (AdminClient calls only) - it never joins
the group or consumes anything, so running it has zero effect on the
real consumer's assignment or offsets.

Usage:
    python tests/kafka/inspect_consumer_group.py
    python tests/kafka/inspect_consumer_group.py --group ingestion-service
    python tests/kafka/inspect_consumer_group.py --topic ecommerce-events

    # Keep re-checking every 2s - handy for watching a rebalance happen
    # live while you start/stop consumer instances in other terminals
    python tests/kafka/inspect_consumer_group.py --watch 2
"""

import argparse
import sys
import time

from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka._model import ConsumerGroupTopicPartitions


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--group", default="ingestion-service")
    p.add_argument("--topic", default="ecommerce-events")
    p.add_argument(
        "--watch",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Re-run the inspection on a loop every SECONDS, until Ctrl+C",
    )
    return p.parse_args()


def inspect_once(admin: AdminClient, group: str, topic: str, bootstrap_servers: str):
    # --- 1. Membership + assignment ---
    describe_futures = admin.describe_consumer_groups([group], request_timeout=10)
    try:
        description = describe_futures[group].result()
    except Exception as e:
        print(f"Could not describe group '{group}': {e}")
        return

    print(f"Group: {group}")
    print(f"State: {description.state}")
    print(f"Partition assignor: {description.partition_assignor}")

    if not description.members:
        print("No active members (group is empty - no consumer currently running).\n")
    else:
        print(f"\n{len(description.members)} member(s):\n")
        for member in description.members:
            partitions = []
            if member.assignment and member.assignment.topic_partitions:
                partitions = sorted(
                    tp.partition
                    for tp in member.assignment.topic_partitions
                    if tp.topic == topic
                )
            print(f"  client.id={member.client_id}  host={member.host}")
            print(f"    owns partitions: {partitions if partitions else '(none assigned)'}")

    # --- 2. Per-partition lag ---
    metadata = admin.list_topics(topic=topic, timeout=10)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        print(f"\nTopic '{topic}' not found on the cluster - skipping lag.")
        return
    all_partitions = sorted(metadata.topics[topic].partitions.keys())

    offsets_future = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group, [TopicPartition(topic, p) for p in all_partitions])]
    )
    try:
        committed = offsets_future[group].result()
    except Exception as e:
        print(f"\nCould not fetch committed offsets for group '{group}': {e}")
        return

    committed_by_partition = {
        tp.partition: tp.offset for tp in committed.topic_partitions if tp.topic == topic
    }

    # A throwaway, non-joining probe consumer purely to read high
    # watermarks (get_watermark_offsets is a Consumer method, not an
    # AdminClient one) - it never subscribes or joins any group.
    from confluent_kafka import Consumer

    probe = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "lag-inspector-probe",
            "enable.auto.commit": False,
        }
    )

    print(f"\n{'Partition':>9}  {'Committed':>9}  {'High Watermark':>15}  {'Lag':>6}")
    print("-" * 48)
    total_lag = 0
    for p in all_partitions:
        _, high = probe.get_watermark_offsets(TopicPartition(topic, p), timeout=10, cached=False)
        raw_committed = committed_by_partition.get(p)
        # -1001 (OFFSET_INVALID) is what the broker returns for a
        # partition this group has never committed an offset on yet.
        committed_offset = raw_committed if raw_committed is not None and raw_committed >= 0 else 0
        lag = max(high - committed_offset, 0)
        total_lag += lag
        committed_display = committed_offset if raw_committed is not None and raw_committed >= 0 else "(none)"
        print(f"{p:>9}  {committed_display!s:>9}  {high:>15}  {lag:>6}")

    probe.close()
    print(f"\nTotal lag across all partitions: {total_lag}")


def main():
    args = parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})

    if args.watch is None:
        inspect_once(admin, args.group, args.topic, args.bootstrap_servers)
        return

    try:
        while True:
            print("\033c", end="")  # clear terminal between refreshes
            inspect_once(admin, args.group, args.topic, args.bootstrap_servers)
            print(f"\n(refreshing every {args.watch}s, Ctrl+C to stop)")
            time.sleep(args.watch)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
