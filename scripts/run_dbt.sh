#!/usr/bin/env bash
#
# Runs `dbt run` once and exits - meant to be invoked on a schedule
# (cron, every 15 minutes on the deployed VM) so analytics.sales_by_interval
# stays close to live instead of only updating on a manual dbt run or
# during ./scripts/setup.sh. sales_by_interval.sql buckets PURCHASE
# events into 15-minute windows, so a 15-minute refresh cadence matches
# the model's own granularity - matching this to the cron interval is
# what actually keeps the Grafana panels honest.
#
# Usage:
#   ./scripts/run_dbt.sh
#
# Crontab entry used on the deploy VM (adjust the path to match where
# the repo actually lives):
#   */15 * * * * /home/deploy/DE-Demo-Project/scripts/run_dbt.sh >> /home/deploy/DE-Demo-Project/logs/dbt_cron.log 2>&1

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# shellcheck disable=SC1091
source .venv/bin/activate

if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

# dbt runs on the host (not in a container), so it needs the
# host-published port rather than the docker-compose service name.
export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Running dbt..."
cd dbt
DBT_PROFILES_DIR=. dbt run
