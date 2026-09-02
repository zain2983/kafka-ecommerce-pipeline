# Real-Time E-Commerce Streaming Platform

A 100% local, free, portfolio-grade data engineering project: synthetic
e-commerce events flow through Kafka, get validated/deduplicated/ingested
by a Python consumer into PostgreSQL, get transformed into analytics
tables by dbt, and show up live on a Grafana dashboard - with every
piece of that path Dockerized, tested, and proven to survive Postgres
outages, Kafka outages, and process crashes.

No cloud services, no paid services, no manual clicking-together of
infrastructure. `docker compose up -d` (or `./scripts/setup.sh`) brings
up the whole stack.

## Architecture

```text
EVENT GENERATOR (Python)
        │
        ▼
KAFKA PRODUCER (Python)
        │
        ▼
┌───────────────────────────────────┐
│               KAFKA                │
│  ecommerce-events (3 partitions)   │
│  ecommerce-events-dlq              │
│  ecommerce-events-retry            │
└──────────────┬─────────────────────┘
               │
               ▼
      KAFKA CONSUMER (Python)
   validate → normalize → dedup
               │
        ┌──────┴──────┐
        ▼             ▼
   valid events   invalid/unparseable
        │             │
        ▼             ▼
   PostgreSQL      DLQ topic ──▶ retry consumer ──▶ back into the flow
   raw.events          (or dead, with reason + retry_count preserved)
        │
        ▼
       dbt
  staging → analytics
        │
        ▼
     GRAFANA
  business metrics (Postgres) + streaming metrics (Prometheus/kafka-exporter)
```

Full design rationale, every architectural decision, and the "why not
Airflow" discussion live in [`design.md`](design.md).

## What this demonstrates

- **Kafka**: producers, consumers, partitions, consumer groups,
  rebalancing, offsets, consumer lag, message keys, at-least-once
  delivery.
- **Streaming ingestion correctness**: validation, normalization,
  idempotent writes (`ON CONFLICT DO NOTHING` backed by a Postgres
  primary key - not just an in-memory cache), batched offset commits,
  dead-letter queue + operator-driven retry.
- **Failure recovery, proven not assumed**: Postgres outage, Kafka
  broker outage, and a hard `SIGKILL` of the consumer mid-stream all
  have real, runnable tests confirming zero data loss and zero
  duplicate rows on recovery - see [`FAILURE_SCENARIOS.md`](FAILURE_SCENARIOS.md).
- **Analytics engineering**: dbt staging/analytics models, schema
  tests, a custom singular test, and a test that verifies the
  aggregation math itself (not just "did dbt run without erroring").
- **Observability**: Prometheus scraping Kafka broker/consumer-group
  metrics via kafka-exporter, a Grafana dashboard mixing business
  metrics (Postgres) and streaming metrics (Prometheus) in one view,
  all provisioned as code.
- **Infrastructure**: every service (including the two custom Python
  apps) containerized with health checks and startup-order dependencies
  that actually wait for readiness, not just "container started."
- **A real test suite** - 17 scripts covering unit-level logic,
  full end-to-end pipeline runs, three distinct failure-recovery
  scenarios, dbt correctness, and the monitoring stack itself - see
  [`tests/README.md`](tests/README.md).

## Quickstart

Requires Docker and Python 3.

```bash
git clone <this-repo>
cd DE-Demo-Project
./scripts/setup.sh   # venv, docker compose up, topics, initial dbt run, verifies the stack end-to-end
./scripts/run.sh      # starts the producer + ingestion consumer, streams their logs
```

Open **http://localhost:3000** (`admin` / `admin`) for the live Grafana
dashboard, or **http://localhost:9090** for Prometheus directly.

`./scripts/run.sh` runs the producer/consumer as host processes so
Ctrl+C gives a clean graceful shutdown; alternatively, since Phase 13,
the same two services (plus the DLQ retry consumer) can run fully
containerized instead:

```bash
docker compose up -d --build   # brings up producer/ingestion/retry as containers too
```

Don't run both at once against the test suite - see the note in
[`tests/README.md`](tests/README.md#18-dockerized-produceringestionretry-phase-13)
about shared consumer-group IDs.

## Repository structure

```text
producer/        event generator + Kafka producer (Python)
ingestion/        Kafka consumer: validate/normalize/dedup/write to Postgres,
                  plus the DLQ retry consumer (retry_main.py)
dbt/              staging + analytics models, schema tests, one custom test
postgres/init/    schema/table DDL, run once on a fresh volume
kafka/init/       topic creation (main + DLQ + retry), idempotent
grafana/          provisioned datasources + the one dashboard, as code
prometheus/       scrape config for kafka-exporter
scripts/          setup.sh (one-time) and run.sh (start the pipeline)
tests/            manual/exploratory test scripts - see tests/README.md
design.md         full design doc: every decision and its rationale
FAILURE_SCENARIOS.md   every tested failure mode, how it's proven, how to reproduce it
```

## Development approach

Built in 14 incremental phases - Docker+Kafka, generator, producer,
consumer, Postgres ingestion, partitions/consumer groups, offset/failure
recovery, idempotency/dedup, retries/DLQ, dbt, Grafana, monitoring +
failure testing, Dockerizing the Python services, and finally this
documentation pass - with each phase working end-to-end, verified by a
real test, before moving to the next. See design.md section 27 for the
full progression.
