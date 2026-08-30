"""
Entry point for Phase 4: consume events from Kafka, normalize, validate,
and report what would happen to each one.

There is no database yet (Phase 5) and no DLQ topic yet (Phase 9), so
this phase just logs the outcome for each message:
  - a valid event logs "would write to PostgreSQL"
  - an invalid event logs "would send to DLQ" along with why
Nothing is actually persisted or re-routed yet - that's what the next
few phases add, on top of this same consume/validate loop.
"""

import logging
import signal

from app.kafka_consumer import EventConsumer, KafkaConsumerConfig
from app.validator import normalize_event, validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    logger.info("Received signal %s, shutting down after current message...", signum)
    _shutdown = True


def main():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    config = KafkaConsumerConfig()
    consumer = EventConsumer(config)

    logger.info(
        "Consuming from topic=%s group=%s at bootstrap=%s",
        config.topic,
        config.group_id,
        config.bootstrap_servers,
    )

    consumed = valid_count = invalid_count = 0

    while not _shutdown:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("Kafka error: %s", msg.error())
            continue

        consumed += 1

        try:
            raw_event = consumer.deserialize(msg)
        except ValueError as e:
            invalid_count += 1
            logger.warning(
                "[partition %d offset %d] UNPARSEABLE (would send to DLQ): %s",
                msg.partition(),
                msg.offset(),
                e,
            )
            continue

        event = normalize_event(raw_event)
        errors = validate_event(event)

        if errors:
            invalid_count += 1
            logger.warning(
                "[partition %d offset %d] INVALID event_id=%s (would send to DLQ): %s",
                msg.partition(),
                msg.offset(),
                event.get("event_id"),
                "; ".join(errors),
            )
        else:
            valid_count += 1
            logger.info(
                "[partition %d offset %d] VALID %s user_id=%s (would write to PostgreSQL)",
                msg.partition(),
                msg.offset(),
                event["event_type"],
                event.get("user_id"),
            )

        if consumed % 50 == 0:
            logger.info(
                "stats: consumed=%d valid=%d invalid=%d", consumed, valid_count, invalid_count
            )

    logger.info(
        "Final stats: consumed=%d valid=%d invalid=%d", consumed, valid_count, invalid_count
    )
    consumer.close()


if __name__ == "__main__":
    main()
