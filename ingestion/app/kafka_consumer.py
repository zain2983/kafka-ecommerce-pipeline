"""
Wraps confluent-kafka's Consumer for the ingestion service.
"""

import json
import logging
import os
from dataclasses import dataclass, field

from confluent_kafka import Consumer

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


class EventConsumer:
    def __init__(self, config: KafkaConsumerConfig):
        self.topic = config.topic
        self._consumer = Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.group_id,
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
        self._consumer.subscribe([self.topic])

    def poll(self, timeout: float = 1.0):
        """Return one raw Kafka Message, or None if nothing arrived within timeout."""
        return self._consumer.poll(timeout)

    def commit(self, msg):
        """
        Synchronously commit the offset for this message. Blocking
        (asynchronous=False) is the simplest thing to reason about for a
        learning project - it means "commit" really does mean "durably
        recorded before we move on" every single time, at the cost of one
        network round-trip per message. Phase 6/7 can revisit batching
        commits for throughput once the basic correctness is solid.
        """
        self._consumer.commit(message=msg, asynchronous=False)

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
