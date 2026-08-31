"""
Publishes rejected/permanently-failed messages to the dead-letter queue
(design.md section 12, Phase 9).

Until now, an unparseable or invalid message was logged and its Kafka
offset committed anyway (main.py's old "would send to DLQ" comment) -
correct in that it never blocked the main topic, but the record of what
actually went wrong existed only in a log line. This module makes that
literal: every rejected message becomes a record on
`ecommerce-events-dlq`, carrying enough to investigate AND enough to
retry it later without re-deriving anything (see retry_main.py).

DLQ record shape:
{
  "dlq_id":            unique id for this DLQ record (not the original event_id -
                        an unparseable payload might not even HAVE one)
  "reason":            "UNPARSEABLE" | "VALIDATION_FAILED"
  "errors":            list of human-readable validation errors (empty for UNPARSEABLE)
  "original_topic":    topic the failed message came from (ecommerce-events, or
                        ecommerce-events-retry if a retry attempt failed again)
  "original_partition": partition it was read from
  "original_offset":   offset it was read from
  "raw_payload":        the original message value, as a decoded string - NOT
                        re-encoded JSON, since an UNPARSEABLE payload may not
                        parse as JSON at all. This is what gets replayed.
  "failed_at":          ISO timestamp of this rejection
  "retry_count":        how many times this event has already been sent back
                        through the retry topic and failed again (0 the first
                        time it's DLQ'd)
}
"""

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class DLQProducerConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: _str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    dlq_topic: str = field(default_factory=lambda: _str("KAFKA_DLQ_TOPIC", "ecommerce-events-dlq"))


class DLQProducer:
    def __init__(self, config: DLQProducerConfig):
        self.topic = config.dlq_topic
        self._producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def send(
        self,
        *,
        reason: str,
        errors: list,
        original_topic: str,
        original_partition: int,
        original_offset: int,
        raw_payload: str,
        retry_count: int = 0,
        key: str = None,
    ):
        record = {
            "dlq_id": uuid.uuid4().hex,
            "reason": reason,
            "errors": errors,
            "original_topic": original_topic,
            "original_partition": original_partition,
            "original_offset": original_offset,
            "raw_payload": raw_payload,
            "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "retry_count": retry_count,
        }
        self._producer.produce(
            topic=self.topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(record).encode("utf-8"),
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)
        return record["dlq_id"]

    def _delivery_callback(self, err, msg):
        if err is not None:
            # A failure to even reach the DLQ is logged loudly but does
            # not raise - the original message's Kafka offset has
            # already been (or is about to be) committed by the caller,
            # matching design.md section 12: a rejected message must
            # never be able to block the main topic, even if the DLQ
            # itself is unreachable. The tradeoff is an un-investigable
            # rejection in that rare case, which is why this is logged
            # at error level rather than silently swallowed.
            logger.error("Failed to deliver DLQ record to %s: %s", msg.topic(), err)

    def flush(self, timeout: float = 10.0):
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("%d DLQ record(s) still undelivered after flush timeout", remaining)

    def close(self):
        self.flush()
