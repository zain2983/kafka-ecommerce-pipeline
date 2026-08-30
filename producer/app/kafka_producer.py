"""
Wraps confluent-kafka's Producer to send event dicts to Kafka as JSON,
keyed by user_id.
"""

import json
import logging
import os
from dataclasses import dataclass, field

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class KafkaProducerConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: _str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(default_factory=lambda: _str("KAFKA_TOPIC", "ecommerce-events"))


class EventProducer:
    def __init__(self, config: KafkaProducerConfig):
        self.topic = config.topic

        self._producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                # Wait for the message to be written to all in-sync replicas
                # before considering it acknowledged. With our single-broker
                # setup ISR = 1 broker, but this is the setting that matters
                # once replication is real.
                "acks": "all",
                # Producer-level idempotence: if a send times out and
                # librdkafka retries it, the broker recognizes the retry
                # (via a producer id + per-message sequence number) and
                # discards the duplicate instead of writing it twice.
                # This is a DIFFERENT kind of duplicate than the ones our
                # event generator injects on purpose (same event_id, two
                # separate produce() calls) - idempotence can't and won't
                # catch those, since from Kafka's point of view they are
                # two unrelated messages. Deduplicating *those* is the
                # consumer/database's job (Phase 8).
                "enable.idempotence": True,
                "retries": 5,
                "retry.backoff.ms": 500,
            }
        )

        self.sent_count = 0
        self.delivered_count = 0
        self.failed_count = 0

    def _delivery_callback(self, err, msg):
        if err is not None:
            self.failed_count += 1
            logger.error("Delivery failed (key=%s): %s", msg.key(), err)
        else:
            self.delivered_count += 1
            logger.debug(
                "Delivered key=%s to %s [partition %d @ offset %d]",
                msg.key(),
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def send(self, event: dict):
        """
        Publish one event. The message key is the user_id, so that all
        events for a given user land on the same partition and are
        therefore ordered relative to each other (design.md section 7).
        Corrupted events may be missing user_id entirely - those fall
        back to an unkeyed (round-robin) send.
        """
        key = event.get("user_id")
        value = json.dumps(event)

        self._producer.produce(
            topic=self.topic,
            key=key.encode("utf-8") if key else None,
            value=value.encode("utf-8"),
            on_delivery=self._delivery_callback,
        )
        self.sent_count += 1

        # poll(0) is non-blocking; it just gives librdkafka a chance to
        # run delivery callbacks for messages that have already been
        # acknowledged by the broker since the last call.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0):
        """Block until all outstanding messages are delivered (or timeout)."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("%d messages still undelivered after flush timeout", remaining)
