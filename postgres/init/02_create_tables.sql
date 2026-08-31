-- design.md section 15/11: raw.events is the landing table for validated,
-- normalized events coming out of the ingestion consumer.
--
-- event_id is PRIMARY KEY on purpose: it's what makes ingestion
-- idempotent. Kafka gives us at-least-once delivery (design.md section
-- 10), meaning the same message can legitimately arrive twice - a
-- redelivery after a crash, or (in this project) a duplicate the event
-- generator injects on purpose to simulate that. Either way, inserting
-- the same event_id twice should only ever produce one row, not an
-- error and not a duplicate record. The ingestion code enforces this
-- with `INSERT ... ON CONFLICT (event_id) DO NOTHING`.
CREATE TABLE IF NOT EXISTS raw.events (
    event_id          TEXT PRIMARY KEY,
    event_type        TEXT NOT NULL,
    event_timestamp   TIMESTAMPTZ NOT NULL,
    user_id           TEXT,
    product_id        TEXT,
    quantity          INTEGER,
    unit_price        NUMERIC(10, 2),

    -- Ingestion metadata (design.md section 9/15) - lets us trace any row
    -- in Postgres back to the exact Kafka message it came from.
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    kafka_partition   INTEGER NOT NULL,
    kafka_offset      BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON raw.events (event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id ON raw.events (user_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_timestamp ON raw.events (event_timestamp);
