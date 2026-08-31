"""
Wraps confluent-kafka's Consumer for the ingestion service.
"""

import json
import logging
import os
import socket
import uuid
from dataclasses import dataclass, field

from confluent_kafka import Consumer, TopicPartition

logger = logging.getLogger(__name__)


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class KafkaConsumerConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: _str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(default_factory=lambda: _str("KAFKA_TOPIC", "ecommerce-events"))
    group_id: str = field(default_factory=lambda: _str("KAFKA_CONSUMER_GROUP", "ingestion-service"))
    # A human-readable label for THIS process, distinct from Kafka's
    # internal member id (a broker-generated UUID we never see directly).
    # Its only purpose is to make multi-consumer logs/PS output readable
    # when running several instances of this same group_id side by side
    # (design.md section 26 - consumer groups, partition rebalancing) -
    # it plays no part in how Kafka assigns partitions or tracks offsets.
    instance_id: str = field(
        default_factory=lambda: _str(
            "KAFKA_CONSUMER_INSTANCE_ID", f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
        )
    )


class EventConsumer:
    def __init__(self, config: KafkaConsumerConfig):
        self.topic = config.topic
        self.instance_id = config.instance_id
        self._consumer = Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.group_id,
                "client.id": config.instance_id,
                # Start from the earliest offset the first time this
                # group_id has ever been seen. On every subsequent run,
                # Kafka resumes from the group's last committed offset
                # instead - this setting only matters for a brand-new group.
                "auto.offset.reset": "earliest",
                # Manual commits: design.md section 10 requires the offset
                # to be committed only AFTER the corresponding write to
                # PostgreSQL succeeds. If we let the client auto-commit on
                # a timer instead, a message could be marked "done" before
                # its database write actually happened - and if the
                # process then crashed, that event would be silently lost
                # forever (Kafka would never redeliver it, since Kafka
                # already believes it was processed). main.py calls
                # commit() explicitly, once per message, right after the
                # database confirms the write.
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe(
            [self.topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

    def _on_assign(self, consumer, partitions):
        """
        Fired whenever the group coordinator (re)assigns partitions to
        this process - on initial startup, and again any time group
        membership changes (another instance joins/leaves/crashes) and
        the group rebalances. Purely observational logging: this is what
        makes partition ownership visible when running multiple
        instances of the same group_id (design.md section 26).
        """
        assigned = sorted(p.partition for p in partitions)
        logger.info("[%s] partitions ASSIGNED: %s", self.instance_id, assigned)

    def _on_revoke(self, consumer, partitions):
        revoked = sorted(p.partition for p in partitions)
        logger.info("[%s] partitions REVOKED (rebalance starting): %s", self.instance_id, revoked)

    def poll(self, timeout: float = 1.0):
        """Return one raw Kafka Message, or None if nothing arrived within timeout."""
        return self._consumer.poll(timeout)

    def commit(self, msg):
        """
        Synchronously commit the offset for a single message. Blocking
        (asynchronous=False) means "commit" really does mean "durably
        recorded before we move on" every single time, at the cost of one
        network round-trip per call.
        """
        self._consumer.commit(message=msg, asynchronous=False)

    def commit_offsets(self, offsets_by_partition: dict):
        """
        Synchronously commit multiple partitions' offsets in a single
        network round-trip (design.md section 10.1 - batched commits).

        offsets_by_partition maps partition -> next offset to read on
        resume (i.e. the offset of the last successfully-handled message
        in that partition, PLUS ONE - by Kafka convention a committed
        offset always means "resume here", not "this was last consumed").
        Callers build this by tracking msg.offset() + 1 for every message
        that has already been safely written to Postgres (or deliberately
        skipped as unparseable/invalid) since the last commit - never for
        one that's still pending or failed. That's what keeps this an
        optimization on top of the correctness model in section 10, not
        a replacement for it: the offset committed still never advances
        past a message whose outcome isn't confirmed, it's just done for
        several messages at once instead of one at a time.
        """
        if not offsets_by_partition:
            return
        offsets = [
            TopicPartition(self.topic, partition, offset)
            for partition, offset in offsets_by_partition.items()
        ]
        self._consumer.commit(offsets=offsets, asynchronous=False)

    @staticmethod
    def deserialize(msg) -> dict:
        """
        Parse the message value as JSON.

        Raises ValueError if the payload isn't valid JSON at all - this is
        distinct from a *validation* failure (Phase 9's DLQ candidate):
        a message that isn't even parseable JSON never had a usable shape
        to begin with.
        """
        try:
            return json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"malformed JSON payload: {e}")

    def close(self):
        self._consumer.close()
