"""
Entry point for the retry consumer (design.md section 13, Phase 9):
consumes ecommerce-events-retry and attempts to re-process each record
through the exact same normalize/validate/insert pipeline main.py uses.

Nothing publishes to ecommerce-events-retry automatically - a message
only lands there when an operator (or tests/kafka/replay_dlq.py) decides
a DLQ'd event is worth trying again, optionally after fixing the
underlying data. That is a deliberate design choice: this process never
re-queues a message to the retry topic itself, only back to the DLQ on
renewed failure, so there is no automatic retry loop for a message that
keeps failing the exact same way - only ever as many attempts as an
operator chooses to make.

Per attempt:
    1. Consume a DLQ-record-shaped message from ecommerce-events-retry
       (dlq.py's DLQProducer.send() schema: reason, raw_payload,
       retry_count, ...).
    2. Re-parse raw_payload, normalize, validate - exactly what main.py
       does for a message from the primary topic.
    3. Success (or a DB-caught duplicate) -> the event has escaped the
       DLQ. Commit this retry-topic offset and move on.
    4. Still unparseable/invalid -> route back to ecommerce-events-dlq
       with retry_count incremented, then commit this retry-topic
       offset (a message that fails a retry attempt must never be able
       to block the retry topic, same principle as design.md section 12
       for the primary topic). retry_count reaching RETRY_MAX_ATTEMPTS
       is logged as a hint to stop replaying this one, not enforced -
       there's no automatic loop to break in the first place.
    5. Postgres itself unreachable (not the event's fault) -> handled
       exactly like main.py's DB_RETRY_ATTEMPTS/give-up path: stop
       WITHOUT committing, so this record is safely re-attempted on
       restart rather than being misclassified as a bad event and sent
       back to the DLQ.
"""

import json
import logging
import os
import signal
import sys
import time

from app.database import DatabaseConfig, EventDatabase
from app.dedup_cache import DedupCache
from app.dlq import DLQProducer, DLQProducerConfig
from app.kafka_consumer import EventConsumer, KafkaConsumerConfig
from app.pipeline import insert_with_retry
from app.validator import normalize_event, validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_RETRY_ATTEMPTS = 5
DB_RETRY_BACKOFF_SECONDS = 2

RETRY_MAX_ATTEMPTS = int(os.environ.get("RETRY_MAX_ATTEMPTS", "3"))

DEDUP_CACHE_SIZE = int(os.environ.get("DEDUP_CACHE_SIZE", "10000"))
DEDUP_CACHE_TTL_SECONDS = float(os.environ.get("DEDUP_CACHE_TTL_SECONDS", "300.0"))

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    logger.info("Received signal %s, shutting down after current message...", signum)
    _shutdown = True


def _retry_kafka_config() -> KafkaConsumerConfig:
    # Distinct env vars from main.py's KAFKA_TOPIC/KAFKA_CONSUMER_GROUP
    # so both processes can share one environment (e.g. the same .env
    # file, or the same docker-compose service defaults) without one
    # silently overriding the other's topic/group.
    return KafkaConsumerConfig(
        topic=os.environ.get("KAFKA_RETRY_TOPIC", "ecommerce-events-retry"),
        group_id=os.environ.get("KAFKA_RETRY_CONSUMER_GROUP", "retry-service"),
    )


def _connect_with_retry(db_config: DatabaseConfig):
    """See main.py's _connect_with_retry - identical policy, duplicated
    rather than shared because it's short and each caller's failure
    message names a different process."""
    for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
        try:
            return EventDatabase(db_config)
        except Exception as e:
            logger.warning(
                "Postgres connection attempt %d/%d failed: %s", attempt, DB_RETRY_ATTEMPTS, e
            )
            if attempt < DB_RETRY_ATTEMPTS:
                time.sleep(DB_RETRY_BACKOFF_SECONDS)
    logger.critical(
        "Could not connect to Postgres after %d attempts. Not starting.", DB_RETRY_ATTEMPTS
    )
    return None


def main():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    kafka_config = _retry_kafka_config()
    db_config = DatabaseConfig()

    consumer = EventConsumer(kafka_config)
    dedup_cache = DedupCache(max_size=DEDUP_CACHE_SIZE, ttl_seconds=DEDUP_CACHE_TTL_SECONDS)
    dlq = DLQProducer(DLQProducerConfig())
    db = _connect_with_retry(db_config)
    if db is None:
        consumer.close()
        sys.exit(1)

    logger.info(
        "[%s] Retry-consuming from topic=%s group=%s at bootstrap=%s -> writing to postgres "
        "db=%s@%s:%s",
        consumer.instance_id,
        kafka_config.topic,
        kafka_config.group_id,
        kafka_config.bootstrap_servers,
        db_config.dbname,
        db_config.host,
        db_config.port,
    )

    consumed = recovered_count = redlq_count = 0

    while not _shutdown:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error("Kafka error: %s", msg.error())
            continue

        consumed += 1

        try:
            record = consumer.deserialize(msg)
        except ValueError as e:
            # A retry-topic message that isn't even our own DLQ-record
            # JSON shouldn't be possible in normal operation (only
            # replay_dlq.py and this process ever touch this topic) -
            # log loudly and move on rather than trying to DLQ a record
            # about a record.
            logger.error(
                "[offset %d] Retry-topic message is not valid JSON, skipping: %s",
                msg.offset(),
                e,
            )
            consumer.commit(msg)
            continue

        retry_count = record.get("retry_count", 0)
        raw_payload = record.get("raw_payload", "")
        original_topic = record.get("original_topic", "unknown")

        log_ctx = f"[retry_count={retry_count} dlq_id={record.get('dlq_id')}]"

        def _requeue_to_dlq(reason, errors):
            new_count = retry_count + 1
            dlq_id = dlq.send(
                reason=reason,
                errors=errors,
                original_topic=original_topic,
                original_partition=record.get("original_partition", -1),
                original_offset=record.get("original_offset", -1),
                raw_payload=raw_payload,
                retry_count=new_count,
                key=msg.key().decode("utf-8", errors="replace") if msg.key() else None,
            )
            level = logger.critical if new_count >= RETRY_MAX_ATTEMPTS else logger.warning
            level(
                "%s retry FAILED (%s), sent back to DLQ as dlq_id=%s with retry_count=%d%s",
                log_ctx,
                reason,
                dlq_id,
                new_count,
                " - at/past RETRY_MAX_ATTEMPTS, consider not replaying this one again"
                if new_count >= RETRY_MAX_ATTEMPTS
                else "",
            )

        try:
            raw_event = json.loads(raw_payload)
        except (json.JSONDecodeError, TypeError) as e:
            redlq_count += 1
            _requeue_to_dlq("UNPARSEABLE", [f"malformed JSON payload: {e}"])
            consumer.commit(msg)
            continue

        event = normalize_event(raw_event)
        errors = validate_event(event)

        if errors:
            redlq_count += 1
            _requeue_to_dlq("VALIDATION_FAILED", errors)
            consumer.commit(msg)
            continue

        event_id = event["event_id"]
        if not dedup_cache.contains(event_id):
            was_inserted = insert_with_retry(
                db,
                event,
                record.get("original_partition", -1),
                record.get("original_offset", -1),
                attempts=DB_RETRY_ATTEMPTS,
                backoff_seconds=DB_RETRY_BACKOFF_SECONDS,
                log_context=log_ctx,
            )
            if was_inserted is None:
                logger.critical(
                    "%s Postgres unreachable after %d attempts - stopping WITHOUT "
                    "committing this retry-topic offset, will re-attempt on restart.",
                    log_ctx,
                    DB_RETRY_ATTEMPTS,
                )
                break
            dedup_cache.add(event_id)

        recovered_count += 1
        logger.info("%s RECOVERED event_id=%s - now in raw.events", log_ctx, event_id)
        consumer.commit(msg)

        if consumed % 20 == 0:
            logger.info(
                "[%s] stats: consumed=%d recovered=%d sent_back_to_dlq=%d",
                consumer.instance_id,
                consumed,
                recovered_count,
                redlq_count,
            )

    logger.info(
        "[%s] Final stats: consumed=%d recovered=%d sent_back_to_dlq=%d",
        consumer.instance_id,
        consumed,
        recovered_count,
        redlq_count,
    )
    dlq.close()
    db.close()
    consumer.close()


if __name__ == "__main__":
    main()
