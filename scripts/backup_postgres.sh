#!/usr/bin/env bash
#
# Recurring Postgres backup (design.md section 29.3). postgres_data was
# previously a single Docker volume with no dump/snapshot process at
# all - this is what closes that gap for Postgres specifically (see
# docs/FAILURE_SCENARIOS.md's "Data loss / backup story" section for the
# Kafka side of 29.3, which is a documented accepted-risk decision
# rather than a backup mechanism, and for why that split makes sense).
#
# Runs `pg_dump` inside the ecommerce-postgres container in custom
# format (-Fc - self-compressed, restorable with pg_restore, supports
# selective/parallel restore unlike a plain .sql dump) and writes the
# result to backups/postgres/ on the host, timestamped. Prunes dumps
# older than RETENTION_DAYS so this directory doesn't grow forever.
#
# Meant to run on a schedule (cron - see the crontab line below and
# docs/DEPLOYMENT.md's "Backups" section), same pattern as
# scripts/run_dbt.sh's dbt refresh.
#
# Usage:
#   ./scripts/backup_postgres.sh
#
# Restore (destructive - --clean drops existing objects first, --create
# drops/recreates the database itself - see `pg_restore --help` before
# running this against anything you care about):
#   docker cp backups/postgres/<file>.dump ecommerce-postgres:/tmp/restore.dump
#   docker exec ecommerce-postgres pg_restore -U ecommerce -d postgres --clean --create /tmp/restore.dump
#
# Crontab entry used on the deploy VM (adjust the path to match where
# the repo actually lives) - every 6 hours, see docs/DEPLOYMENT.md's
# "Backups" section for why that cadence was chosen:
#   0 */6 * * * /home/deploy/DE-Demo-Project/scripts/backup_postgres.sh >> /home/deploy/DE-Demo-Project/logs/postgres_backup_cron.log 2>&1

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

POSTGRES_USER="${POSTGRES_USER:-ecommerce}"
POSTGRES_DB="${POSTGRES_DB:-ecommerce}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-7}"

BACKUP_DIR="$REPO_ROOT/backups/postgres"
mkdir -p "$BACKUP_DIR"

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$BACKUP_DIR/ecommerce_${TIMESTAMP}.dump"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Backing up postgres (db=${POSTGRES_DB}) -> ${OUT_FILE}"

docker exec ecommerce-postgres pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc > "$OUT_FILE"

if [ ! -s "$OUT_FILE" ]; then
    echo "backup produced an empty file - treating as a failure" >&2
    rm -f "$OUT_FILE"
    exit 1
fi

SIZE="$(du -h "$OUT_FILE" | cut -f1)"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Backup complete (${SIZE})"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Pruning dumps older than ${RETENTION_DAYS} days"
find "$BACKUP_DIR" -name 'ecommerce_*.dump' -mtime "+${RETENTION_DAYS}" -print -delete
