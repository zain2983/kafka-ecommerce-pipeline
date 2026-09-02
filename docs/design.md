# Real-Time E-Commerce Streaming Platform

## 1. Project Overview

This project is a **100% local, free, portfolio-level data engineering project** designed to demonstrate how a real-time streaming data platform can be built using Apache Kafka.

The project simulates an e-commerce platform generating real-time user and transaction events.

Events are produced by a Python application, streamed through Apache Kafka, consumed and ingested into PostgreSQL, and then transformed into analytics-ready datasets using dbt.

Everything runs locally using Docker.

There are **no cloud services or paid services**.

### Core technologies

* Python
* Apache Kafka
* Docker / Docker Compose
* PostgreSQL
* dbt Core
* Grafana
* Git / GitHub

Airflow is intentionally **not part of the initial architecture** because this is primarily a streaming project and Kafka + the Python consumer already handle the streaming ingestion workflow.

---

# 2. High-Level Architecture

The overall pipeline is:

```text
                    EVENT GENERATOR
                         Python
                           │
                           ▼
                    KAFKA PRODUCER
                         Python
                           │
                           ▼
                    ┌─────────────┐
                    │    KAFKA    │
                    │   Docker    │
                    │             │
                    │ ecommerce-  │
                    │ events      │
                    └──────┬──────┘
                           │
                           ▼
                    KAFKA CONSUMER
                         Python
                           │
                 ┌─────────┴─────────┐
                 │                   │
                 ▼                   ▼
           Valid Events        Invalid Events
                 │                   │
                 ▼                   ▼
            PostgreSQL          Kafka DLQ
                 │
                 ▼
            RAW DATABASE
                 │
                 ▼
                DBT
                 │
                 ▼
         ANALYTICS TABLES
                 │
                 ▼
              GRAFANA
```

The key architectural idea is that Kafka handles the **streaming transport**, the Python consumer handles **stream-level processing and ingestion**, PostgreSQL stores the data, and dbt handles **analytics transformations**.

---

# 3. Why This Project Exists

The goal is not simply to demonstrate that we know how to produce and consume Kafka messages.

The goal is to demonstrate understanding of a realistic streaming data platform, including:

* Event-driven data ingestion
* Kafka producers
* Kafka consumers
* Kafka topics
* Kafka partitions
* Consumer groups
* Kafka offsets
* Consumer lag
* Message keys
* Serialization
* Data validation
* Error handling
* Dead-letter queues
* Retry mechanisms
* Deduplication
* Idempotent processing
* PostgreSQL ingestion
* Analytics transformations
* dbt models and tests
* Real-time analytics
* Monitoring
* Dockerized infrastructure

The project should eventually be able to demonstrate failure scenarios and explain how the system recovers from them.

---

# 4. Everything Runs Locally

There are no cloud dependencies.

The entire environment will run on a developer laptop using Docker.

Conceptually:

```text
Laptop
│
└── Docker Compose
    │
    ├── Kafka
    ├── PostgreSQL
    ├── Producer
    ├── Ingestion Consumer
    └── Grafana
```

dbt can initially run from the host machine and may later be containerized as part of the project.

No GCP, AWS, Azure, BigQuery, Cloud Run, or other paid cloud infrastructure is required.

---

# 5. Event Source

There is no external streaming API.

The project will generate its own synthetic e-commerce events.

A Python event generator will continuously create events that simulate activity on an e-commerce website.

Example event types:

```text
USER_SIGNUP
PRODUCT_VIEW
ADD_TO_CART
CHECKOUT_STARTED
PURCHASE
```

Example event:

```json
{
  "event_id": "evt_12345",
  "event_type": "PURCHASE",
  "timestamp": "2026-08-28T20:45:32Z",
  "user_id": "user_183",
  "product_id": "prod_981",
  "quantity": 2,
  "unit_price": 49.99
}
```

The event generator should be configurable so that we can control:

* Event generation rate
* Number of users
* Number of products
* Event types
* Probability of each event
* Invalid-event generation
* Duplicate-event generation

This will allow us to simulate both normal and abnormal streaming conditions.

---

# 6. Kafka Producer

The producer is a Python application responsible for sending events to Kafka.

It should not contain the Kafka broker itself.

The distinction is:

```text
kafka/
    Kafka infrastructure/configuration

producer/
    Our Python application that uses Kafka
```

The producer connects to Kafka and publishes events to the appropriate topic.

Conceptually:

```text
Event Generator
      │
      ▼
Python Producer
      │
      ▼
Kafka Topic
```

The producer should eventually support:

* JSON serialization
* Kafka message keys
* Delivery acknowledgements
* Error handling
* Retries
* Configurable bootstrap servers
* Configurable topic names

---

# 7. Kafka Topics

The initial design can use a primary topic:

```text
ecommerce-events
```

This topic contains all e-commerce events.

The topic should have multiple partitions, for example:

```text
ecommerce-events
│
├── partition-0
├── partition-1
└── partition-2
```

The project should use message keys where appropriate.

For example:

```text
key = user_id
```

This allows events belonging to the same user to consistently map to the same partition.

Later, additional topics can be introduced.

Potential topics:

```text
ecommerce-events
ecommerce-events-dlq
ecommerce-events-retry
```

The DLQ is used for events that cannot be processed successfully.

---

# 8. Kafka Consumer / Ingestion Service

The ingestion service is a Python application that consumes events from Kafka.

Its primary responsibility is:

```text
Kafka
  ↓
Consume Event
  ↓
Validate
  ↓
Process
  ↓
Write to PostgreSQL
  ↓
Commit Kafka Offset
```

The consumer should NOT perform all analytics transformations.

It is responsible primarily for **stream-level processing and reliable ingestion**.

---

# 9. Responsibilities of the Kafka Consumer

The consumer should handle:

### Message consumption

Read events from Kafka.

### Deserialization

Convert Kafka message payloads into Python objects.

### Validation

Ensure the event contains valid fields.

For example:

```text
event_id must exist
event_type must be valid
timestamp must be valid
quantity must be numeric
price must be numeric
```

### Normalization

Perform lightweight ingestion transformations such as:

```text
"purchase" → "PURCHASE"
"49.99" → 49.99
"2" → 2
```

### Deduplication

Detect duplicate events using `event_id`.

### Metadata

Potentially add ingestion metadata such as:

```text
ingested_at
kafka_partition
kafka_offset
```

### Error handling

Malformed or permanently unprocessable events should be sent to a DLQ.

### Database ingestion

Valid events are written into PostgreSQL.

---

# 10. Kafka Offset Management

The consumer should follow the principle:

```text
1. Consume message
2. Validate/process message
3. Successfully write to PostgreSQL
4. Commit Kafka offset
```

The consumer should NOT commit the offset before the database operation succeeds.

Otherwise, a failure could result in:

```text
Kafka message consumed
       ↓
Offset committed
       ↓
PostgreSQL write fails
```

Kafka now believes the message was processed even though the database never received it.

The project should therefore demonstrate an **at-least-once processing model**.

## 10.1 Future Enhancement: Batched Commits

The basic version commits the offset synchronously after every single message. This is correct and easy to reason about, but limits throughput to one Kafka round-trip per event.

Once the basic (per-message) version is working end-to-end, this can be revisited for throughput: buffer N events (or flush every K seconds), write them to PostgreSQL together, and commit the Kafka offset once per batch rather than once per message - as long as the offset committed never advances past a message whose write hasn't been confirmed. This is an optimization on top of the correctness model above, not a replacement for it, and should only be tackled after the unbatched version is proven correct under failure (Phase 7).

---

# 11. Idempotency

Because at-least-once processing can result in a message being processed more than once, the database ingestion must be idempotent.

For example:

```text
event_id = evt_12345
```

should only result in one database record.

The PostgreSQL table can enforce uniqueness:

```sql
event_id PRIMARY KEY
```

or use an appropriate unique constraint.

Then, if Kafka delivers the same event again:

```text
evt_12345
evt_12345
```

the database should not create duplicate business records.

This allows the project to demonstrate the relationship between:

```text
Kafka at-least-once delivery
+
consumer retries
+
database idempotency
```

---

# 12. Dead-Letter Queue

Invalid or permanently failed events should not crash the consumer.

The architecture should support:

```text
                 Kafka
                   │
                   ▼
                Consumer
                   │
             ┌─────┴─────┐
             │           │
           Valid       Invalid
             │           │
             ▼           ▼
        PostgreSQL      DLQ
```

The DLQ topic can be:

```text
ecommerce-events-dlq
```

Example invalid event:

```json
{
  "event_id": "evt_123",
  "event_type": "PURCHASE",
  "quantity": "INVALID"
}
```

The DLQ should retain enough information to investigate the original failure.

---

# 13. Retry Mechanism

Transient failures should be retried.

For example, if PostgreSQL temporarily becomes unavailable:

```text
Kafka
  ↓
Consumer
  ↓
PostgreSQL
  ↓
FAIL
  ↓
Retry
  ↓
Retry
  ↓
Retry
```

Permanent failures can eventually be routed to the DLQ.

The retry strategy should be designed carefully so that a poison message does not block the entire consumer indefinitely.

---

# 14. PostgreSQL

PostgreSQL is the local persistence layer.

It runs inside Docker.

We are not implementing PostgreSQL itself.

We are configuring and initializing it.

The repository may contain:

```text
postgres/
└── init/
    ├── 01_create_schemas.sql
    └── 02_create_tables.sql
```

These scripts initialize the database.

---

# 15. PostgreSQL Data Layers

The database should contain logical layers:

```text
PostgreSQL
│
├── raw
│
├── staging
│
└── analytics
```

### RAW

Contains data as ingested from Kafka with only necessary ingestion-level processing.

Example:

```text
raw.events
```

Potential columns:

```text
event_id
event_type
event_timestamp
user_id
product_id
quantity
unit_price
ingested_at
kafka_partition
kafka_offset
```

The raw layer should preserve enough information to trace an event back to Kafka.

---

# 16. Why dbt is included

dbt is intentionally part of this architecture.

The Python Kafka consumer handles **streaming ingestion transformations**.

dbt handles **analytics transformations**.

These are different responsibilities.

### Python/Kafka Consumer

Handles:

```text
Validation
Normalization
Deduplication
Error handling
Streaming processing
Kafka metadata
Ingestion
```

### dbt

Handles:

```text
Business logic
Aggregations
Analytics models
Metrics
Joins
Dimensional modeling
Data quality tests
Documentation
```

Therefore:

```text
Kafka
  ↓
Python Consumer
  ↓
PostgreSQL RAW
  ↓
DBT
  ↓
Analytics
```

---

# 17. dbt Transformation Layer

dbt will read from the PostgreSQL raw/staging data and build analytics models.

Example:

```text
raw.events
     │
     ▼
stg_events
     │
     ├──────────────┐
     ▼              ▼
daily_sales   product_performance
     │
     ├──────────────┐
     ▼              ▼
customer_metrics   conversion_metrics
```

---

# 18. Example dbt Models

### Staging

```text
stg_events.sql
```

Responsible for preparing raw data for analytics.

Example:

```sql
SELECT
    event_id,
    event_type,
    event_timestamp,
    user_id,
    product_id,
    quantity,
    unit_price
FROM raw.events
```

---

### Daily Sales

```text
daily_sales.sql
```

Could produce:

```text
sale_date
orders
units_sold
revenue
```

Example:

```text
2026-08-28 | 1243 | 2841 | 58231.42
```

---

### Product Performance

Could produce:

```text
product_id
views
cart_additions
purchases
revenue
conversion_rate
```

---

### Customer Metrics

Could produce:

```text
user_id
total_views
total_cart_additions
total_purchases
total_revenue
```

---

# 19. Grafana

Grafana will provide a local analytics/monitoring dashboard.

It will connect to PostgreSQL.

Potential dashboards include:

### Business metrics

```text
Orders
Revenue
Units Sold
Active Users
Conversion Rate
```

### Product metrics

```text
Top Products
Top Categories
Product Views
Purchases
```

### Streaming metrics

```text
Events / second
Events / minute
Consumer Lag
Failed Events
DLQ Messages
Processing Latency
```

The goal is to demonstrate both:

```text
business analytics
+
streaming-system observability
```

---

# 20. Docker

Docker is used to make the entire environment reproducible.

Docker Compose will eventually manage:

```text
Kafka
PostgreSQL
Producer
Ingestion Consumer
Grafana
```

The goal is to be able to start the core platform with:

```bash
docker compose up -d
```

A new developer should be able to clone the repository, install minimal prerequisites, and start the platform without manually installing Kafka or PostgreSQL.

---

# 21. Why Airflow Is NOT Included

Airflow is intentionally excluded.

The main pipeline is streaming:

```text
Producer
   ↓
Kafka
   ↓
Consumer
   ↓
PostgreSQL
```

Airflow would not provide much value for this core ingestion path.

Airflow is more appropriate for workflows such as:

```text
Extract
  ↓
Transform
  ↓
Load
  ↓
Run tests
  ↓
Generate report
```

on a scheduled basis.

Since Kafka is already providing the streaming event transport and the consumer is continuously processing events, adding Airflow would introduce unnecessary complexity.

If future requirements introduce significant batch workflows, Airflow can be added later.

---

# 22. Repository Structure

The initial repository should look approximately like:

```text
real-time-ecommerce-streaming/
│
├── docker-compose.yml
├── .env
├── .gitignore
├── README.md
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       ├── main.py
│       ├── event_generator.py
│       ├── kafka_producer.py
│       └── schemas.py
│
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       ├── main.py
│       ├── kafka_consumer.py
│       ├── validator.py
│       ├── processor.py
│       └── database.py
│
├── kafka/
│   └── init/
│       └── create_topics.sh
│
├── postgres/
│   └── init/
│       ├── 01_create_schemas.sql
│       └── 02_create_tables.sql
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_events.sql
│   │   │   └── schema.yml
│   │   │
│   │   └── analytics/
│   │       ├── daily_sales.sql
│   │       ├── product_performance.sql
│   │       ├── customer_metrics.sql
│   │       └── conversion_metrics.sql
│   │
│   └── tests/
│
└── grafana/
    └── dashboards/
```

---

# 23. Important Folder Responsibility

The distinction between folders is important.

```text
kafka/
    Kafka infrastructure/configuration

postgres/
    PostgreSQL infrastructure/schema initialization

producer/
    Python application that PRODUCES Kafka events

ingestion/
    Python application that CONSUMES Kafka events
    and ingests them into PostgreSQL

dbt/
    Analytics transformation logic

grafana/
    Dashboard configuration
```

We are NOT implementing Kafka or PostgreSQL themselves.

Kafka and PostgreSQL are external open-source systems that we run using Docker.

Our repository contains the configuration, initialization scripts, and applications that interact with them.

---

# 24. End-to-End Data Flow

A normal event should follow this path:

```text
1. Event Generator
       │
       ▼
2. Kafka Producer
       │
       ▼
3. Kafka Topic
       │
       ▼
4. Kafka Consumer
       │
       ├── Validate
       ├── Normalize
       ├── Deduplicate
       └── Add metadata
       │
       ▼
5. PostgreSQL RAW
       │
       ▼
6. dbt Staging Models
       │
       ▼
7. dbt Analytics Models
       │
       ▼
8. PostgreSQL Analytics
       │
       ▼
9. Grafana
```

---

# 25. Failure Flow

The system should also support:

```text
                    Kafka
                      │
                      ▼
                   Consumer
                      │
              ┌───────┴────────┐
              │                │
            Valid            Invalid
              │                │
              ▼                ▼
         PostgreSQL           DLQ
              │
              ▼
             dbt
              │
              ▼
          Analytics
```

For transient database failures:

```text
Consumer
   │
   ▼
PostgreSQL
   │
   X FAILURE
   │
   ▼
Retry
   │
   ▼
Success
   │
   ▼
Commit Kafka Offset
```

For permanent failures:

```text
Consumer
   │
   ▼
Processing Failure
   │
   ▼
Retry Attempts
   │
   ▼
DLQ
```

---

# 26. Portfolio-Level Features

The project should eventually demonstrate the following.

## Kafka

* Producers
* Consumers
* Topics
* Partitions
* Consumer groups
* Offsets
* Consumer lag
* Message keys
* Retention
* Rebalancing

## Streaming Processing

* Continuous event processing
* Validation
* Normalization
* Deduplication
* Idempotency
* Retry logic
* Dead-letter queues
* Failure recovery
* Batched offset commits for throughput (see 10.1)

## PostgreSQL

* Raw layer
* Staging layer
* Analytics layer
* Constraints
* Indexing
* Upserts
* Query optimization

## dbt

* Staging models
* Analytics models
* Incremental models where appropriate
* Tests
* Documentation
* Model dependencies
* Business transformations

## Infrastructure

* Docker
* Docker Compose
* Environment variables
* Service networking
* Health checks

## Observability

* Consumer lag
* Throughput
* Processing latency
* Failed events
* DLQ volume
* Database failures

---

# 27. Development Philosophy

The project should be developed incrementally.

Do not build the entire architecture at once.

Recommended progression:

```text
PHASE 1
Docker + Kafka
        ↓
PHASE 2
Python Event Generator
        ↓
PHASE 3
Kafka Producer
        ↓
PHASE 4
Kafka Consumer
        ↓
PHASE 5
PostgreSQL Ingestion
        ↓
PHASE 6
Partitions + Consumer Groups
        ↓
PHASE 7
Offsets + Failure Recovery
        ↓
PHASE 8
Idempotency + Deduplication
        ↓
PHASE 9
Retries + DLQ
        ↓
PHASE 10
dbt Analytics Layer
        ↓
PHASE 11
Grafana
        ↓
PHASE 12
Monitoring + Failure Testing
        ↓
PHASE 13
Dockerize Everything
        ↓
PHASE 14
Documentation + Portfolio Polish
```

Each phase should be working before moving to the next one.

---

# 28. Final Architecture

The final system should conceptually look like:

```text
                         ┌──────────────────────┐
                         │   EVENT GENERATOR    │
                         │       Python         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    KAFKA PRODUCER    │
                         │       Python         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────┐
                    │            KAFKA             │
                    │          Docker              │
                    │                             │
                    │     ecommerce-events        │
                    │     ecommerce-events-dlq    │
                    │     ecommerce-events-retry  │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                         ┌──────────────────────┐
                         │   KAFKA CONSUMER     │
                         │       Python         │
                         │                      │
                         │ Validate             │
                         │ Normalize            │
                         │ Deduplicate          │
                         │ Process              │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │      POSTGRESQL      │
                         │        Docker        │
                         │                      │
                         │       RAW            │
                         │        ↓             │
                         │     STAGING          │
                         │        ↓             │
                         │     ANALYTICS         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                              ┌──────────┐
                              │   DBT    │
                              │          │
                              │ Business │
                              │ Logic    │
                              │ Models   │
                              │ Tests    │
                              └────┬─────┘
                                   │
                                   ▼
                         ┌──────────────────────┐
                         │  ANALYTICS TABLES    │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │       GRAFANA        │
                         │                      │
                         │ Business Analytics   │
                         │ +                    │
                         │ Streaming Monitoring │
                         └──────────────────────┘
```

The project is intentionally designed to demonstrate the separation of concerns:

```text
Kafka
    → transports streaming events

Python Consumer
    → handles reliable streaming ingestion

PostgreSQL
    → stores the data

dbt
    → performs analytics/business transformations

Grafana
    → visualizes analytics and system metrics

Docker
    → runs the entire platform locally
```

The entire platform should remain **free, local, reproducible, and portfolio-ready**.

---

# 29. Roadmap: Hardening Before Real Data

The pipeline itself (Kafka → ingestion → PostgreSQL → dbt → Grafana,
including DLQ/retry/dedup) is built and verified end-to-end against
synthetic data. Before swapping the synthetic producer for a real data
source, the following gaps should be closed, in order:

## 29.1 CI / automated tests

`tests/` is currently a set of manual/exploratory scripts (see
`tests/README.md`) - real coverage (dedup stress, Kafka/Postgres
kill-recovery, DLQ replay), but nothing runs them automatically, so
regressions are only caught by hand. Wire these into a CI pipeline
(e.g. GitHub Actions) that runs on push/PR, so a change can't silently
break dedup, offset-commit ordering, or DLQ handling.

## 29.2 Alerting on top of Prometheus

`prometheus/prometheus.yml` currently only scrapes kafka-exporter and
node-exporter into Grafana for manual viewing - there's no
Alertmanager and no alerting rules. Add rules (and a notification
channel) for at least: consumer lag growing unbounded, DLQ volume
spiking, and a service failing its health check - so problems page
someone instead of waiting to be noticed on the dashboard.

## 29.3 Backup story for Postgres and Kafka

`postgres_data` and `kafka_data` are single Docker volumes with no
dump/snapshot process. `docs/FAILURE_SCENARIOS.md` covers services
being temporarily *unreachable*, not data loss or corruption. Add a
recurring `pg_dump` (or volume snapshot) for Postgres, and decide on
an acceptable data-loss window for Kafka (single broker, replication
factor 1 everywhere) - even a documented "acceptable loss" answer is
better than the current silent gap.

These three are scoped intentionally narrow: they harden the
*current* synthetic-data pipeline. Changes required specifically for
ingesting a real data source (event-type/schema assumptions currently
duplicated across the producer, `ingestion/app/validator.py`, and the
dbt staging tests; the flat `raw.events` schema; the PURCHASE-only
revenue model in `dbt/models/analytics/sales_by_interval.sql`; rigid
timestamp parsing; no schema registry) are deliberately deferred until
that real source is actually in hand, to avoid designing against
assumptions that may turn out wrong.
