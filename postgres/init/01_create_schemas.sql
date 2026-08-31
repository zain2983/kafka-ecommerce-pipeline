-- design.md section 15: PostgreSQL has three logical layers.
-- raw       - data as ingested from Kafka, with minimal transformation
-- staging   - dbt's cleaned-up view of raw (Phase 10)
-- analytics - business-level aggregates built on staging (Phase 10)

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
