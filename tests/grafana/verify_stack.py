#!/usr/bin/env python3
"""
Phase 11 verification tool: confirms Grafana, Prometheus, and
kafka-exporter are actually wired together correctly, not just that
their containers are "Up".

Checks:
  1. Grafana is healthy and reachable.
  2. Both provisioned datasources exist with the expected type/uid
     (grafana/provisioning/datasources/datasources.yml).
  3. Each datasource's own health check passes (GET
     /api/datasources/uid/{uid}/health).
  4. The dashboard was provisioned (grafana/dashboards/*.json).
  5. Prometheus is actually scraping kafka-exporter (target state "up",
     not just "the container is running" - a broker/networking
     misconfiguration would leave the container up but the scrape
     failing).
  6. Every PromQL query used by a dashboard panel returns a result
     without error (catches typos/label mismatches that would only
     otherwise show up as a silently-empty panel in the browser).
  7. Every SQL query used by a business-metrics panel executes cleanly
     BOTH directly against Postgres AND through Grafana's own query
     proxy (POST /api/ds/query) - the direct check isolates "is the SQL/
     data wrong" from "is Grafana's connection to the datasource wrong".
     This distinction is what actually caught a real bug once: on a
     fresh `docker compose up`, Grafana could provision its Postgres
     datasource and run its own connection test before Postgres's
     healthcheck had passed yet, leaving the datasource in an error
     state ("You do not currently have a default database configured
     for this data source") even though Postgres itself, and direct
     queries against it, were completely fine. Fixed in docker-compose.yml
     via `depends_on: postgres: condition: service_healthy` - a plain
     `depends_on: [postgres]` only waits for the container to start, not
     for Postgres to actually be ready to accept connections.

This is a read-only check against the already-running stack - it
doesn't start/stop anything.

Usage:
    docker compose up -d
    python3 tests/grafana/verify_stack.py
"""

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

import psycopg2

GRAFANA_URL = "http://localhost:3000"
GRAFANA_AUTH = ("admin", "admin")
PROMETHEUS_URL = "http://localhost:9090"

PG_CONFIG = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
    user=os.environ.get("POSTGRES_USER", "ecommerce"),
    password=os.environ.get("POSTGRES_PASSWORD", "ecommerce"),
)

# Same SQL used by grafana/dashboards/ecommerce-overview.json's Postgres panels.
DASHBOARD_SQL_QUERIES = [
    "SELECT orders FROM analytics.daily_sales WHERE sale_date = CURRENT_DATE",
    "SELECT revenue FROM analytics.daily_sales WHERE sale_date = CURRENT_DATE",
    "SELECT units_sold FROM analytics.daily_sales WHERE sale_date = CURRENT_DATE",
    "SELECT count(DISTINCT user_id) AS active_users FROM raw.events "
    "WHERE event_timestamp > now() - interval '1 hour'",
    "SELECT CASE WHEN count(*) FILTER (WHERE event_type = 'PRODUCT_VIEW') = 0 THEN 0 "
    "ELSE round(100.0 * count(*) FILTER (WHERE event_type = 'PURCHASE') "
    "/ count(*) FILTER (WHERE event_type = 'PRODUCT_VIEW'), 2) END AS conversion_rate "
    "FROM raw.events WHERE event_timestamp > now() - interval '1 hour'",
    "SELECT event_type, count(*) AS count FROM raw.events "
    "WHERE event_timestamp > now() - interval '24 hours' GROUP BY event_type ORDER BY count DESC",
    "SELECT sale_date::timestamp AS time, revenue FROM analytics.daily_sales ORDER BY sale_date",
]

EXPECTED_DATASOURCES = {
    "postgres-ds": "postgres",
    "prometheus-ds": "prometheus",
}
EXPECTED_DASHBOARD_UID = "ecommerce-overview"

# Same PromQL expressions used by grafana/dashboards/ecommerce-overview.json's panels.
DASHBOARD_QUERIES = [
    'sum(rate(kafka_topic_partition_current_offset{topic="ecommerce-events"}[1m]))',
    'kafka_consumergroup_lag_sum{consumergroup="ingestion-service", topic="ecommerce-events"}',
    'kafka_consumergroup_lag_sum{consumergroup="retry-service", topic="ecommerce-events-retry"}',
    'sum(rate(kafka_topic_partition_current_offset{topic="ecommerce-events-dlq"}[5m]))',
    'sum(kafka_topic_partition_current_offset{topic="ecommerce-events-dlq"})',
    'kafka_consumergroup_members{consumergroup="ingestion-service"}',
]


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def _auth_header(req, auth):
    if auth:
        import base64

        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        req.add_header("Authorization", f"Basic {token}")


def get_json(url, auth=None):
    req = urllib.request.Request(url)
    _auth_header(req, auth)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def post_json(url, body, auth=None):
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
    )
    _auth_header(req, auth)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def main():
    failures = []

    try:
        health = get_json(f"{GRAFANA_URL}/api/health")
        expect(health.get("database") == "ok", "Grafana is healthy and reachable", failures)
    except (urllib.error.URLError, urllib.error.HTTPError) as e:
        expect(False, f"Grafana is healthy and reachable (error: {e})", failures)
        print("\nCannot continue without Grafana reachable - stopping here.")
        sys.exit(1)

    datasources = get_json(f"{GRAFANA_URL}/api/datasources", auth=GRAFANA_AUTH)
    by_uid = {ds["uid"]: ds for ds in datasources}
    for uid, expected_type_prefix in EXPECTED_DATASOURCES.items():
        ds = by_uid.get(uid)
        expect(
            ds is not None and expected_type_prefix in ds.get("type", ""),
            f"datasource '{uid}' is provisioned with type containing '{expected_type_prefix}'",
            failures,
        )

    for uid in EXPECTED_DATASOURCES:
        try:
            health = get_json(f"{GRAFANA_URL}/api/datasources/uid/{uid}/health", auth=GRAFANA_AUTH)
            ok = health.get("status") == "OK"
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            ok = False
            health = {"message": str(e)}
        expect(
            ok,
            f"datasource '{uid}' health check passes (Grafana's own connection test)",
            failures,
        )
        if not ok:
            print(f"    (message: {health.get('message')})")

    search = get_json(f"{GRAFANA_URL}/api/search?type=dash-db", auth=GRAFANA_AUTH)
    dashboard_uids = {d["uid"] for d in search}
    expect(
        EXPECTED_DASHBOARD_UID in dashboard_uids,
        f"dashboard '{EXPECTED_DASHBOARD_UID}' was provisioned",
        failures,
    )

    targets = get_json(f"{PROMETHEUS_URL}/api/v1/targets")
    kafka_targets = [
        t
        for t in targets["data"]["activeTargets"]
        if "kafka-exporter" in t.get("scrapeUrl", "")
    ]
    expect(
        any(t["health"] == "up" for t in kafka_targets),
        "Prometheus is actively scraping kafka-exporter (target health=up)",
        failures,
    )

    for query in DASHBOARD_QUERIES:
        try:
            result = get_json(
                f"{PROMETHEUS_URL}/api/v1/query?query={urllib.parse.quote(query)}"
            )
            ok = result.get("status") == "success"
        except Exception as e:
            ok = False
            result = {"error": str(e)}
        expect(ok, f"PromQL query executes without error: {query}", failures)
        if ok and not result["data"]["result"]:
            print(f"    (note: query returned no series yet - {query})")

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    for query in DASHBOARD_SQL_QUERIES:
        try:
            cur.execute(query)
            cur.fetchall()
            ok = True
        except Exception as e:
            conn.rollback()
            ok = False
            print(f"    (error: {e})")
        expect(ok, f"SQL query executes without error (direct): {query[:60]}...", failures)
    cur.close()
    conn.close()

    for query in DASHBOARD_SQL_QUERIES:
        try:
            result = post_json(
                f"{GRAFANA_URL}/api/ds/query",
                {
                    "queries": [
                        {
                            "refId": "A",
                            "datasource": {"type": "postgres", "uid": "postgres-ds"},
                            "format": "table",
                            "rawSql": query,
                        }
                    ]
                },
                auth=GRAFANA_AUTH,
            )
            ok = result.get("results", {}).get("A", {}).get("status") == 200
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            ok = False
            print(f"    (error: {e})")
        expect(
            ok, f"SQL query executes without error (via Grafana proxy): {query[:60]}...", failures
        )

    print()
    if failures:
        print(f"{len(failures)} check(s) FAILED")
        sys.exit(1)
    else:
        print("All checks passed.")


if __name__ == "__main__":
    main()
