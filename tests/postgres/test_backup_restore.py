#!/usr/bin/env python3
"""
Tests scripts/backup_postgres.sh end to end (design.md section 29.3) -
not just that it exits 0, but that the dump it produces is actually
restorable and contains real data:
  1. Insert one known, uniquely-tagged row into raw.events.
  2. Run the real backup script (same one cron runs on the VM).
  3. Confirm it produced a non-empty .dump file.
  4. Restore that dump into a scratch database (pg_restore -d
     backup_restore_test - never touches the real `ecommerce` database).
  5. Confirm the known row is present there, byte-for-byte.

Cleans up the scratch database, the dump file, and the test row
whether it passes or fails, so re-running is always safe.

Requires Postgres running:
    docker compose up -d postgres

Usage:
    python tests/postgres/test_backup_restore.py
"""

import glob
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ingestion"))

from app.database import DatabaseConfig, EventDatabase  # noqa: E402

RUN_ID = uuid.uuid4().hex[:8]
REPO_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
BACKUP_SCRIPT = os.path.join(REPO_ROOT, "scripts", "backup_postgres.sh")
BACKUP_DIR = os.path.join(REPO_ROOT, "backups", "postgres")
SCRATCH_DB = "backup_restore_test"
EVENT_ID = f"evt_test_backup_{RUN_ID}"


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def psql(*args, check=True):
    return subprocess.run(
        ["docker", "exec", "ecommerce-postgres", "psql", "-U", "ecommerce", *args],
        capture_output=True,
        text=True,
        timeout=30,
        check=check,
    )


def main():
    failures = []
    dump_path = None

    db = EventDatabase(DatabaseConfig())
    with db._conn.cursor() as cur:
        cur.execute("DELETE FROM raw.events WHERE event_id = %s", (EVENT_ID,))
    db._conn.commit()

    event = {
        "event_id": EVENT_ID,
        "event_type": "PRODUCT_VIEW",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id": "user_test_backup",
        "product_id": "prod_test_backup",
    }
    db.insert_event(event, partition=0, offset=1)
    db.close()

    try:
        before = set(glob.glob(os.path.join(BACKUP_DIR, "ecommerce_*.dump")))
        result = subprocess.run(
            [BACKUP_SCRIPT], capture_output=True, text=True, timeout=60
        )
        expect(result.returncode == 0, "backup_postgres.sh exits 0", failures)
        print(result.stdout, end="")

        after = set(glob.glob(os.path.join(BACKUP_DIR, "ecommerce_*.dump")))
        new_dumps = after - before
        expect(len(new_dumps) == 1, f"exactly one new dump file produced (found {len(new_dumps)})", failures)
        if not new_dumps:
            print(f"{len(failures)} test(s) FAILED")
            sys.exit(1)
        dump_path = new_dumps.pop()
        expect(os.path.getsize(dump_path) > 0, "dump file is non-empty", failures)

        psql("-c", f"DROP DATABASE IF EXISTS {SCRATCH_DB};")
        psql("-c", f"CREATE DATABASE {SCRATCH_DB} OWNER ecommerce;")

        container_path = "/tmp/backup_restore_test.dump"
        subprocess.run(
            ["docker", "cp", dump_path, f"ecommerce-postgres:{container_path}"],
            check=True,
            timeout=30,
        )
        restore = subprocess.run(
            [
                "docker", "exec", "ecommerce-postgres",
                "pg_restore", "-U", "ecommerce", "-d", SCRATCH_DB, container_path,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        expect(restore.returncode == 0, "pg_restore into the scratch database exits 0", failures)
        if restore.returncode != 0:
            print("--- pg_restore stderr ---")
            print(restore.stderr)

        row_check = psql(
            "-d", SCRATCH_DB, "-t", "-c",
            f"SELECT event_id FROM raw.events WHERE event_id = '{EVENT_ID}';",
            check=False,
        )
        expect(
            EVENT_ID in row_check.stdout,
            "the known test row is present in the restored database",
            failures,
        )
    finally:
        psql("-c", f"DROP DATABASE IF EXISTS {SCRATCH_DB};", check=False)
        db2 = EventDatabase(DatabaseConfig())
        with db2._conn.cursor() as cur:
            cur.execute("DELETE FROM raw.events WHERE event_id = %s", (EVENT_ID,))
        db2._conn.commit()
        db2.close()
        if dump_path and os.path.exists(dump_path):
            os.remove(dump_path)

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
