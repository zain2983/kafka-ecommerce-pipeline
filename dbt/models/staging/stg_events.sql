-- design.md section 18: prepares raw data for analytics. The ingestion
-- consumer (Phase 5-9) already normalizes and validates every row
-- before it reaches raw.events, so there's deliberately no further
-- cleanup logic here - this model exists to (a) select only the
-- columns analytics models should build on, dropping Kafka-tracing
-- metadata (ingested_at/kafka_partition/kafka_offset) that's an
-- ingestion-layer concern, not a business one, and (b) give every
-- downstream model one single, renameable point of contact with the
-- raw schema instead of each one reading raw.events directly.

select
    event_id,
    event_type,
    event_timestamp,
    user_id,
    product_id,
    quantity,
    unit_price
from {{ source('raw', 'events') }}
