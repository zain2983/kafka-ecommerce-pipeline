"""
Entry point: consume events from Kafka, normalize, validate, write valid
ones to PostgreSQL, and only then commit the Kafka offset.

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
disappears with no record of it. A real DLQ strategy for genuinely
invalid messages is still Phase 9 - what this file now does handle
(Phase 7) is the "PostgreSQL was temporarily unreachable" case: stop and
make the failure loud, which is unglamorous but is still exactly Kafka's
at-least-once model - nothing is lost, because nothing was committed.

Batched offset commits (design.md section 10.1, Phase 7): rather than
one commit() round-trip per message, this loop tracks each partition's
latest *confirmed* offset (written to Postgres, or deliberately skipped
as unparseable/invalid) in `pending_offsets` and flushes it - a single
commit() call covering every partition at once - every COMMIT_BATCH_SIZE
messages or COMMIT_INTERVAL_SECONDS, whichever comes first. The
correctness rule from section 10 is unchanged: an offset only ever
enters `pending_offsets` after its message's outcome is confirmed, so a
crash between flushes just means more messages get re-processed on
restart (still safe - see idempotency, section 11) - never that an
unconfirmed message's offset gets committed early. On a DB write giving
up entirely, everything already in `pending_offsets` is flushed before
the process stops, so a stuck message never holds up offsets for work
that's already safely done.

Startup-time PostgreSQL outages (Phase 7): the per-message retry loop
below only covers PostgreSQL going down WHILE this process is already
running. If it's already down when the process starts, connecting has
to succeed before there's a Kafka offset to protect in the first place -
_connect_with_retry() applies the same retry-then-give-up-loudly policy
to that case too, instead of letting an unhandled connection error kill
the process with a bare traceback.
"""

import logging
import os
import signal
import sys
import time

from app.database import DatabaseConfig, EventDatabase
from app.kafka_consumer import EventConsumer, KafkaConsumerConfig
from app.validator import normalize_event, validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_RETRY_ATTEMPTS = 5
DB_RETRY_BACKOFF_SECONDS = 2

COMMIT_BATCH_SIZE = int(os.environ.get("COMMIT_BATCH_SIZE", "20"))
COMMIT_INTERVAL_SECONDS = float(os.environ.get("COMMIT_INTERVAL_SECONDS", "2.0"))

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    logger.info("Received signal %s, shutting down after current message...", signum)
    _shutdown = True


def _connect_with_retry(db_config: DatabaseConfig):
    """
    EventDatabase.__init__ connects eagerly, so if PostgreSQL happens to
    already be down when this process starts (as opposed to going down
    mid-stream, which the main loop's retry logic already handles), a
    plain EventDatabase(db_config) call raises before the consumer has
    even subscribed to Kafka - an unhandled traceback instead of the
    same "retry, then give up loudly" behavior section 10/13 promise for
    every other kind of PostgreSQL outage. This applies the same
    DB_RETRY_ATTEMPTS/DB_RETRY_BACKOFF_SECONDS policy to startup too.

    Returns None (rather than exiting directly) on total failure -
    exiting here, before the caller has had a chance to close() its
    already-subscribed Kafka consumer, would leave this process as a
    "phantom" group member: Kafka only learns a member is gone once its
    session times out (minutes, by default) rather than immediately,
    which would leave the NEXT attempt's rebalance stuck waiting behind
    a membership slot nobody is going to free. See main()'s caller.
    """
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
        "Could not connect to Postgres after %d attempts. Not starting - "
        "no Kafka offsets have been touched, so this is safe to retry "
        "(restart the process once Postgres is reachable).",
        DB_RETRY_ATTEMPTS,
    )
    return None


def main():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    kafka_config = KafkaConsumerConfig()
    db_config = DatabaseConfig()

    consumer = EventConsumer(kafka_config)
    db = _connect_with_retry(db_config)
    if db is None:
        # Explicitly close so Kafka's group coordinator learns this
        # member is leaving right away (a clean LeaveGroupRequest)
        # instead of waiting out this member's session timeout before
        # the next run's rebalance can proceed - see _connect_with_retry.
        consumer.close()
        sys.exit(1)

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

    # partition -> next offset to resume at, for every message whose
    # outcome is confirmed but not yet committed to Kafka. Flushed by
    # _flush_pending() below (design.md section 10.1). Note this dict has
    # at most one entry per partition (a later message just overwrites
    # its partition's entry with a higher offset), so batch-size progress
    # is tracked separately via `messages_since_commit` - checking
    # len(pending_offsets) here would only ever count up to the number of
    # partitions this instance owns, never actual message volume.
    pending_offsets = {}
    messages_since_commit = 0
    last_commit_at = time.monotonic()

    def _flush_pending(reason: str):
        nonlocal pending_offsets, messages_since_commit, last_commit_at
        if pending_offsets:
            consumer.commit_offsets(pending_offsets)
            logger.debug("Flushed %d partition(s)' offsets (%s)", len(pending_offsets), reason)
            pending_offsets = {}
        messages_since_commit = 0
        last_commit_at = time.monotonic()

    def _mark_confirmed(partition: int, offset: int):
        nonlocal messages_since_commit
        pending_offsets[partition] = offset + 1
        messages_since_commit += 1
        if messages_since_commit >= COMMIT_BATCH_SIZE or (
            time.monotonic() - last_commit_at >= COMMIT_INTERVAL_SECONDS
        ):
            _flush_pending("batch size or interval reached")

    while not _shutdown:
        msg = consumer.poll(1.0)
        if msg is None:
            if pending_offsets and time.monotonic() - last_commit_at >= COMMIT_INTERVAL_SECONDS:
                _flush_pending("interval elapsed, idle")
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
            _mark_confirmed(partition, offset)
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
            _mark_confirmed(partition, offset)
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
            # Flush whatever was ALREADY confirmed before this message -
            # a stuck message must never hold up offsets for work that's
            # already safely written, but the stuck message's own offset
            # is never added to pending_offsets in the first place.
            _flush_pending("giving up, flushing prior confirmed work")
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

        _mark_confirmed(partition, offset)

        if consumed % 50 == 0:
            logger.info(
                "[%s] stats: consumed=%d inserted=%d duplicate=%d invalid=%d",
                consumer.instance_id,
                consumed,
                inserted_count,
                duplicate_count,
                invalid_count,
            )

    # Graceful shutdown (SIGINT/SIGTERM) exits the loop normally rather
    # than via the give-up break above, so anything confirmed since the
    # last automatic flush still needs to go out before we close - an
    # abrupt kill (SIGKILL) skips this, which is fine: whatever never got
    # committed is safely re-processed on the next run (section 11).
    _flush_pending("shutting down")

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
