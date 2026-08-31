"""
Writes validated, normalized events into PostgreSQL's raw.events table.
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg2

logger = logging.getLogger(__name__)


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class DatabaseConfig:
    host: str = field(default_factory=lambda: _str("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(_str("POSTGRES_PORT", "5432")))
    dbname: str = field(default_factory=lambda: _str("POSTGRES_DB", "ecommerce"))
    user: str = field(default_factory=lambda: _str("POSTGRES_USER", "ecommerce"))
    password: str = field(default_factory=lambda: _str("POSTGRES_PASSWORD", "ecommerce"))


# ON CONFLICT (event_id) DO NOTHING is what makes this insert idempotent
# (design.md section 11): if Kafka redelivers a message we've already
# written - a real retry, or one of the duplicate event_ids the event
# generator injects on purpose - this becomes a no-op instead of a
# constraint-violation error or a second row.
INSERT_SQL = """
    INSERT INTO raw.events (
        event_id, event_type, event_timestamp, user_id, product_id,
        quantity, unit_price, ingested_at, kafka_partition, kafka_offset
    ) VALUES (
        %(event_id)s, %(event_type)s, %(event_timestamp)s, %(user_id)s, %(product_id)s,
        %(quantity)s, %(unit_price)s, %(ingested_at)s, %(kafka_partition)s, %(kafka_offset)s
    )
    ON CONFLICT (event_id) DO NOTHING
"""


class EventDatabase:
    def __init__(self, config: DatabaseConfig):
        self._config = config
        self._conn = self._connect()

    def _connect(self):
        conn = psycopg2.connect(
            host=self._config.host,
            port=self._config.port,
            dbname=self._config.dbname,
            user=self._config.user,
            password=self._config.password,
        )
        conn.autocommit = False
        return conn

    def reconnect(self):
        """
        Used after a failed write to get a fresh connection before
        retrying - once a psycopg2 connection has errored (e.g. the
        server was unreachable mid-query) it stays unusable, so retrying
        on the same connection object would just fail again immediately.
        """
        try:
            self._conn.close()
        except Exception:
            pass
        self._conn = self._connect()

    def insert_event(self, event: dict, partition: int, offset: int) -> bool:
        """
        Insert one event. Returns True if a new row was written, False if
        the event_id already existed (a duplicate was correctly ignored).
        """
        params = {
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "event_timestamp": event["timestamp"],
            "user_id": event.get("user_id"),
            "product_id": event.get("product_id"),
            "quantity": event.get("quantity"),
            "unit_price": event.get("unit_price"),
            "ingested_at": datetime.now(timezone.utc),
            "kafka_partition": partition,
            "kafka_offset": offset,
        }
        with self._conn.cursor() as cur:
            cur.execute(INSERT_SQL, params)
            inserted = cur.rowcount == 1
        self._conn.commit()
        return inserted

    def close(self):
        self._conn.close()
