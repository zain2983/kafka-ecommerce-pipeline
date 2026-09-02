#!/usr/bin/env python3
"""
Phase 10 test: confirms sales_by_interval.sql's aggregation is actually
correct, not just that `dbt run` exits 0.

Inserts a small, known set of PURCHASE rows directly into raw.events
(bypassing Kafka entirely - this is testing dbt's SQL logic, not
ingestion, the same reasoning test_database.py uses to talk to Postgres
directly) all timestamped exactly on a fixed, far-in-the-past 15-minute
bucket boundary that real pipeline traffic will never land on, so this
test's expected totals are exactly its own rows - nothing else in
raw.events can contribute to that bucket.

Runs the real `dbt build` (run + test) as a subprocess, then checks
analytics.sales_by_interval for that bucket against hand-computed
expected orders/units_sold/revenue.

Test rows (event_id prefixed evt_test_dbt_...) are deleted before AND
after this runs, so re-running is always safe and it never leaves stale
data behind for a real `dbt run` to later aggregate by accident.

Requires the full stack running, plus dbt installed:
    docker compose up -d
    source .venv/bin/activate
    pip install -r dbt/requirements.txt   # once

Usage:
    python tests/dbt/test_sales_by_interval.py
"""

import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone

import psycopg2

RUN_ID = uuid.uuid4().hex[:8]
DBT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "dbt")

# A 15-minute bucket boundary no real traffic will ever use - safe to
# own exclusively. Exactly on the boundary so to_timestamp(floor(...))
# in sales_by_interval.sql maps every test row to this same bucket.
TEST_INTERVAL_START = "2020-06-15T12:00:00Z"

PG_CONFIG = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
    user=os.environ.get("POSTGRES_USER", "ecommerce"),
    password=os.environ.get("POSTGRES_PASSWORD", "ecommerce"),
)

# (quantity, unit_price) per test order.
TEST_ORDERS = [(2, 19.99), (1, 49.50), (5, 3.20)]
EXPECTED_ORDERS = len(TEST_ORDERS)
EXPECTED_UNITS = sum(q for q, _ in TEST_ORDERS)
EXPECTED_REVENUE = round(sum(q * p for q, p in TEST_ORDERS), 2)


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def eid(n):
    return f"evt_test_dbt_{RUN_ID}_{n}"


def cleanup(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM raw.events WHERE event_id LIKE 'evt_test_dbt_%'")
    conn.commit()


def insert_test_orders(conn):
    with conn.cursor() as cur:
        for n, (quantity, unit_price) in enumerate(TEST_ORDERS):
            cur.execute(
                """
                INSERT INTO raw.events (
                    event_id, event_type, event_timestamp, user_id, product_id,
                    quantity, unit_price, kafka_partition, kafka_offset
                ) VALUES (%s, 'PURCHASE', %s, %s, 'prod_test', %s, %s, 0, %s)
                """,
                (
                    eid(n),
                    TEST_INTERVAL_START,
                    f"user_test_dbt_{RUN_ID}",
                    quantity,
                    unit_price,
                    n,
                ),
            )
    conn.commit()
    print(f"Inserted {len(TEST_ORDERS)} test PURCHASE rows in bucket {TEST_INTERVAL_START} (run_id={RUN_ID})")


def run_dbt(*args):
    env = dict(os.environ, DBT_PROFILES_DIR=".")
    result = subprocess.run(
        ["dbt", *args],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result.returncode == 0, result.stdout + result.stderr


def query_sales_by_interval():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT orders, units_sold, revenue FROM analytics.sales_by_interval WHERE interval_start = %s",
        (TEST_INTERVAL_START,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def main():
    failures = []
    conn = psycopg2.connect(**PG_CONFIG)

    try:
        cleanup(conn)  # in case a previous run of this script failed midway
        insert_test_orders(conn)

        build_ok, output = run_dbt("build")
        expect(build_ok, "`dbt build` (run + test) exited successfully", failures)

        row = query_sales_by_interval()
        expect(row is not None, f"a sales_by_interval row exists for {TEST_INTERVAL_START}", failures)

        if row is not None:
            orders, units_sold, revenue = row
            expect(
                orders == EXPECTED_ORDERS,
                f"orders == {EXPECTED_ORDERS} (got {orders})",
                failures,
            )
            expect(
                units_sold == EXPECTED_UNITS,
                f"units_sold == {EXPECTED_UNITS} (got {units_sold})",
                failures,
            )
            expect(
                float(revenue) == EXPECTED_REVENUE,
                f"revenue == {EXPECTED_REVENUE} (got {revenue})",
                failures,
            )

        print()
        if failures:
            print(f"{len(failures)} test(s) FAILED")
            print("\n--- dbt build output (tail) ---")
            print("\n".join(output.splitlines()[-40:]))
        else:
            print("All tests passed.")
    finally:
        cleanup(conn)
        conn.close()
        # Rebuild sales_by_interval so its table no longer carries a
        # leftover row for TEST_INTERVAL_START now that the source rows
        # are gone - otherwise the table (unlike a view) would keep that
        # stale row until the next unrelated `dbt run`.
        run_dbt("run", "--select", "sales_by_interval")

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
