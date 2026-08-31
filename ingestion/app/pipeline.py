"""
Shared processing helper used by both consumer entry points: main.py
(reads ecommerce-events) and retry_main.py (reads ecommerce-events-retry,
Phase 9). Both need the exact same "insert with bounded retry against a
possibly-flaky Postgres, reconnecting between attempts" policy - this is
that policy, kept in one place instead of two copies to keep in sync.
"""

import logging
import time

logger = logging.getLogger(__name__)


def insert_with_retry(db, event: dict, partition: int, offset: int, *, attempts: int,
                       backoff_seconds: float, log_context: str):
    """
    Attempts db.insert_event() up to `attempts` times, reconnecting
    between failures (see database.py's reconnect()). Returns True
    (freshly inserted), False (event_id already existed - a no-op
    duplicate), or None (every attempt failed - caller decides what
    that means for offsets/DLQ, since that's specific to which topic
    is being consumed).
    """
    was_inserted = None
    for attempt in range(1, attempts + 1):
        try:
            was_inserted = db.insert_event(event, partition, offset)
            break
        except Exception as e:
            logger.warning(
                "%s DB write attempt %d/%d failed for event_id=%s: %s",
                log_context,
                attempt,
                attempts,
                event.get("event_id"),
                e,
            )
            if attempt < attempts:
                time.sleep(backoff_seconds)
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
                        "%s reconnect attempt %d/%d failed: %s",
                        log_context,
                        attempt,
                        attempts,
                        reconnect_error,
                    )
    return was_inserted
