#!/usr/bin/env python3
"""
Tests EventDatabase (ingestion/app/database.py) directly against the
real Postgres container - no Kafka involved. Confirms:
  - a normal insert creates a row
  - inserting the same event_id again is a no-op, not an error and not
    a second row (idempotency, design.md section 11)
  - reconnect() produces a working connection again afterward

Requires Postgres running:
    docker compose up -d postgres

Usage:
    python tests/postgres/test_database.py
"""

import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ingestion"))

from app.database import DatabaseConfig, EventDatabase  # noqa: E402

RUN_ID = uuid.uuid4().hex[:8]


def make_event(n: int) -> dict:
    return {
        "event_id": f"evt_test_db_{RUN_ID}_{n}",
        "event_type": "PRODUCT_VIEW",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id": "user_test",
        "product_id": "prod_test",
    }


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def main():
    db = EventDatabase(DatabaseConfig())
    failures = []

    # Clean up any test rows left behind by previous runs of this script,
    # so results are never affected by leftover state.
    with db._conn.cursor() as cur:
        cur.execute("DELETE FROM raw.events WHERE event_id LIKE 'evt_test_db_%'")
    db._conn.commit()

    event = make_event(1)

    inserted_first = db.insert_event(event, partition=0, offset=1)
    expect(inserted_first is True, "first insert of a new event_id returns True", failures)

    inserted_second = db.insert_event(event, partition=0, offset=2)
    expect(
        inserted_second is False,
        "inserting the same event_id again returns False (idempotent, no error)",
        failures,
    )

    db.reconnect()
    event2 = make_event(2)
    inserted_after_reconnect = db.insert_event(event2, partition=0, offset=3)
    expect(inserted_after_reconnect is True, "insert works normally after reconnect()", failures)

    db.close()

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
