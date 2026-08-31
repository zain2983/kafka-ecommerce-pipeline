#!/usr/bin/env python3
"""
Read-only inspector for raw.events - the Postgres-side counterpart to
tests/kafka/inspect_kafka_topic.py. Shows row/distinct-id counts (a
mismatch there would mean the idempotent insert is broken), a breakdown
by event_type, and the most recent rows.

Usage:
    python tests/postgres/inspect_raw_events.py
    python tests/postgres/inspect_raw_events.py --limit 20
    python tests/postgres/inspect_raw_events.py --user-id user_42
"""

import argparse
import os

import psycopg2
import psycopg2.extras


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--host", default=os.environ.get("POSTGRES_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("POSTGRES_PORT", "5432")))
    p.add_argument("--dbname", default=os.environ.get("POSTGRES_DB", "ecommerce"))
    p.add_argument("--user", default=os.environ.get("POSTGRES_USER", "ecommerce"))
    p.add_argument("--password", default=os.environ.get("POSTGRES_PASSWORD", "ecommerce"))
    p.add_argument("--limit", type=int, default=10, help="How many recent rows to print")
    p.add_argument("--user-id", default=None, help="Only show rows for this user_id")
    return p.parse_args()


def main():
    args = parse_args()
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT count(*) AS total, count(DISTINCT event_id) AS distinct_ids FROM raw.events")
    row = cur.fetchone()
    print(f"Total rows: {row['total']}   Distinct event_ids: {row['distinct_ids']}")
    if row["total"] != row["distinct_ids"]:
        print("WARNING: total rows != distinct event_ids - the idempotent insert may be broken!")
    print()

    cur.execute("SELECT event_type, count(*) AS n FROM raw.events GROUP BY event_type ORDER BY n DESC")
    print("By event_type:")
    for r in cur.fetchall():
        print(f"  {r['event_type']:<18} {r['n']:>6}")
    print()

    where = ""
    params = []
    if args.user_id:
        where = "WHERE user_id = %s"
        params.append(args.user_id)

    cur.execute(
        f"""
        SELECT event_id, event_type, event_timestamp, user_id, product_id,
               quantity, unit_price, kafka_partition, kafka_offset, ingested_at
        FROM raw.events
        {where}
        ORDER BY ingested_at DESC
        LIMIT %s
        """,
        params + [args.limit],
    )
    label = f" for {args.user_id}" if args.user_id else ""
    print(f"Most recent {args.limit} row(s){label}:")
    for r in cur.fetchall():
        print(
            f"  [{r['kafka_partition']}:{r['kafka_offset']}] {r['event_id']} "
            f"{r['event_type']:<16} user={r['user_id']} product={r['product_id']} "
            f"qty={r['quantity']} price={r['unit_price']}"
        )

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
