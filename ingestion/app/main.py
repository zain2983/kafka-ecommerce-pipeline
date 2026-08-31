"""
Entry point for Phase 5: consume events from Kafka, normalize, validate,
write valid ones to PostgreSQL, and only then commit the Kafka offset.

There is still no DLQ topic (Phase 9) - invalid events are logged and
the offset is committed anyway (skipping forward, since without a DLQ
there's nowhere else to put them right now).

Offset-commit sequencing (design.md section 10):
    1. Consume message
    2. Validate / normalize
    3. Successfully write to PostgreSQL
    4. ONLY THEN commit the Kafka offset

If step 3 fails (e.g. PostgreSQL is temporarily unreachable), we do NOT
commit. Importantly, not committing is not enough on its own: Kafka's
poll() advances through a partition regardless of what you've committed
- nothing stops you from polling the next message even though the
previous one was never acknowledged. So on a DB write failure, this loop
retries the SAME message a bounded number of times (with a short delay
and a fresh DB connection each attempt) and deliberately does NOT poll()
again until it either succeeds or gives up. If it gives up, the process
stops entirely rather than silently skipping forward - because skipping
would mean that event (and the fact that this partition ever got stuck)
disappears with no record of it. This is a blunt tool: a real retry/
backoff/DLQ strategy is Phase 7 (Offsets + Failure Recovery) and Phase 9
(Retries + DLQ). For now, "stop and make the failure loud" is the
correct - if unglamorous - behavior, and it's still exactly Kafka's
at-least-once model: nothing is lost, because nothing was committed.
"""

import logging
import signal
import time

from app.database import DatabaseConfig, EventDatabase
from app.kafka_consumer import EventConsumer, KafkaConsumerConfig
from app.validator import normalize_event, validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_RETRY_ATTEMPTS = 5
DB_RETRY_BACKOFF_SECONDS = 2

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    logger.info("Received signal %s, shutting down after current message...", signum)
    _shutdown = True


def main():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    kafka_config = KafkaConsumerConfig()
    db_config = DatabaseConfig()

    consumer = EventConsumer(kafka_config)
    db = EventDatabase(db_config)

    logger.info(
        "[%s] Consuming from topic=%s group=%s at bootstrap=%s -> writing to postgres db=%s@%s:%s",
        consumer.instance_id,
        kafka_config.topic,
        kafka_config.group_id,
        kafka_config.bootstrap_servers,
        db_config.dbname,
        db_config.host,
        db_config.port,
    )

    consumed = inserted_count = duplicate_count = invalid_count = 0

    while not _shutdown:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("Kafka error: %s", msg.error())
            continue

        consumed += 1
        partition, offset = msg.partition(), msg.offset()

        try:
            raw_event = consumer.deserialize(msg)
        except ValueError as e:
            invalid_count += 1
            logger.warning(
                "[partition %d offset %d] UNPARSEABLE (would send to DLQ): %s",
                partition,
                offset,
                e,
            )
            consumer.commit(msg)
            continue

        event = normalize_event(raw_event)
        errors = validate_event(event)

        if errors:
            invalid_count += 1
            logger.warning(
                "[partition %d offset %d] INVALID event_id=%s (would send to DLQ): %s",
                partition,
                offset,
                event.get("event_id"),
                "; ".join(errors),
            )
            consumer.commit(msg)
            continue

        was_inserted = None
        for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
            try:
                was_inserted = db.insert_event(event, partition, offset)
                break
            except Exception as e:
                logger.warning(
                    "[partition %d offset %d] DB write attempt %d/%d failed for "
                    "event_id=%s: %s",
                    partition,
                    offset,
                    attempt,
                    DB_RETRY_ATTEMPTS,
                    event.get("event_id"),
                    e,
                )
                if attempt < DB_RETRY_ATTEMPTS:
                    time.sleep(DB_RETRY_BACKOFF_SECONDS)
                    try:
                        db.reconnect()
                    except Exception as reconnect_error:
                        # The DB may still be down - that's just another
                        # failed attempt, not a reason to crash. If
                        # reconnect() itself dies, self._conn is left as
                        # whatever _connect() partially produced (or the
                        # old closed one); either way the NEXT
                        # insert_event() call will raise again and this
                        # loop will retry reconnecting once more.
                        logger.warning(
                            "[partition %d offset %d] reconnect attempt %d/%d failed: %s",
                            partition,
                            offset,
                            attempt,
                            DB_RETRY_ATTEMPTS,
                            reconnect_error,
                        )

        if was_inserted is None:
            logger.critical(
                "[partition %d offset %d] Giving up on event_id=%s after %d attempts. "
                "Stopping WITHOUT committing this offset - restart the consumer once "
                "PostgreSQL is healthy again and it will safely re-process this "
                "message and everything after it (event_id is a PRIMARY KEY, so "
                "anything already-written stays a no-op, not a duplicate).",
                partition,
                offset,
                event.get("event_id"),
                DB_RETRY_ATTEMPTS,
            )
            break  # stop consuming entirely - do NOT poll() past this message

        if was_inserted:
            inserted_count += 1
            logger.info(
                "[partition %d offset %d] INSERTED %s user_id=%s",
                partition,
                offset,
                event["event_type"],
                event.get("user_id"),
            )
        else:
            duplicate_count += 1
            logger.info(
                "[partition %d offset %d] DUPLICATE event_id=%s ignored (already in raw.events)",
                partition,
                offset,
                event["event_id"],
            )

        consumer.commit(msg)

        if consumed % 50 == 0:
            logger.info(
                "[%s] stats: consumed=%d inserted=%d duplicate=%d invalid=%d",
                consumer.instance_id,
                consumed,
                inserted_count,
                duplicate_count,
                invalid_count,
            )

    logger.info(
        "[%s] Final stats: consumed=%d inserted=%d duplicate=%d invalid=%d",
        consumer.instance_id,
        consumed,
        inserted_count,
        duplicate_count,
        invalid_count,
    )
    db.close()
    consumer.close()


if __name__ == "__main__":
    main()
