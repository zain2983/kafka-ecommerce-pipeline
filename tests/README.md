# Manual testing scripts

These are exploratory/manual scripts, not an automated pytest suite. They
exist so you can poke at the system by hand and see what's actually
happening.

```
tests/
├── producer/
│   └── test_event_generator.py   tests producer/app code in isolation, no Kafka
├── kafka/
│   └── inspect_kafka_topic.py    inspects any topic on the cluster (not producer-specific -
│                                  also useful for the DLQ/retry topics added in later phases)
├── ingestion/
│   ├── test_validator.py         tests normalize_event()/validate_event() in isolation, no Kafka
│   └── test_end_to_end.py        full pipeline: produce -> real consumer subprocess -> verify Postgres
└── postgres/
    ├── test_database.py          tests EventDatabase's idempotent insert directly, no Kafka
    └── inspect_raw_events.py     inspects what's actually in raw.events
```

## Setup (once)

```bash
cd /Users/dev/Desktop/Personal/DE-Demo-Project
source .venv/bin/activate
```

Make sure Kafka is running for the second script:

```bash
docker compose up -d
docker ps --filter name=ecommerce-kafka
```

## 1. `test_event_generator.py` - test the generator alone

Generates events using the exact same code the producer uses, but never
touches Kafka. Good for checking the shape of events, and for confirming
the invalid/duplicate injection rates roughly match what you configured.

```bash
# Print 30 events + a summary
python3 tests/producer/test_event_generator.py --count 30

# Reproducible output (same seed -> same events every run)
python3 tests/producer/test_event_generator.py --count 30 --seed 42

# Large sample, summary only (useful for checking injection rates are
# close to INVALID_EVENT_PROBABILITY / DUPLICATE_EVENT_PROBABILITY)
python3 tests/producer/test_event_generator.py --count 5000 --quiet
```

You can also override generator settings via env vars before running, same
as the real producer:

```bash
NUM_USERS=10 INVALID_EVENT_PROBABILITY=0.2 python3 tests/producer/test_event_generator.py --count 20
```

## 2. `inspect_kafka_topic.py` - see what's actually in Kafka

Requires the `.venv` to be active (`source .venv/bin/activate`) - this
script imports `confluent_kafka`, which is only installed inside the
venv, not system-wide. `test_event_generator.py` above has no such
requirement since it only uses the standard library, which is why this
is easy to forget.

Always prints a summary table first: partition count, which broker leads
each partition, and how many messages are currently in each partition
(the "high watermark minus low watermark" - i.e. what's available to
read, not counting anything already deleted by retention).

```bash
# Just the summary - no messages consumed
python3 tests/kafka/inspect_kafka_topic.py
```

```
Topic: ecommerce-events
Partition count: 3

Partition  Leader Broker  Low Offset  High Offset   Messages
------------------------------------------------------------
        0              1           0          147        147
        1              1           0          114        114
        2              1           0          131        131

Total messages currently in topic: 392
```

Add `--show` to also print the actual messages (partition, offset, key,
value):

```bash
# Show the first 20 messages ever produced, across all partitions
python3 tests/kafka/inspect_kafka_topic.py --show --from-beginning --max-messages 20

# Only look at partition 1
python3 tests/kafka/inspect_kafka_topic.py --show --from-beginning --partition 1 --max-messages 10

# Watch for new messages live (like `tail -f`) - run the producer in
# another terminal at the same time and watch them show up here
python3 tests/kafka/inspect_kafka_topic.py --show --max-messages 1000 --idle-timeout 30
```

Notes:

- Every run uses a brand-new, random consumer group id, so this tool never
  affects the committed offsets of the real ingestion consumer you'll
  build in Phase 4 - it's a pure observer, exactly like `tail`-ing a log
  file.
- Without `--from-beginning`, it only shows messages produced *after* the
  script starts (`auto.offset.reset=latest`) - useful for the "watch it
  live" case above.
- When reading all partitions (no `--partition` given), the consumer has
  to join a consumer group and get partitions assigned before it can read
  anything - that rebalance can take a couple of seconds, which is why
  the default `--idle-timeout` is 10s.
- All partitions currently show the same "Leader Broker" (`1`) because
  there is only one broker in this local cluster. In a multi-broker
  cluster, partition leadership would be spread across brokers - that's
  part of how Kafka distributes load and survives a broker failing.

## 3. `test_validator.py` - test normalization/validation alone

Feeds hand-written cases (clean, messy-but-fixable, and genuinely broken)
through the real `ingestion/app/validator.py` code. No Kafka, no Postgres.

```bash
python3 tests/ingestion/test_validator.py
```

## 4. `test_database.py` - test idempotent inserts alone

Talks to `ingestion/app/database.py` directly, with no Kafka involved.
Confirms a fresh insert creates a row, inserting the same `event_id`
again is silently ignored rather than erroring or duplicating (design.md
section 11), and that `reconnect()` leaves the connection usable
afterward - the same reconnect path the ingestion consumer relies on
after a Postgres outage.

Requires Postgres running:

```bash
docker compose up -d postgres
python3 tests/postgres/test_database.py
```

Test rows use event_ids prefixed `evt_test_db_...` and are cleaned up
automatically at the start of each run, so re-running is always safe.

## 5. `inspect_raw_events.py` - see what's actually in Postgres

The Postgres-side counterpart to `inspect_kafka_topic.py`. Prints row
count vs. distinct `event_id` count (these should always be equal - a
mismatch would mean the idempotent insert is broken), a breakdown by
`event_type`, and the most recent rows.

```bash
python3 tests/postgres/inspect_raw_events.py
python3 tests/postgres/inspect_raw_events.py --limit 20
python3 tests/postgres/inspect_raw_events.py --user-id user_42
```

## 6. `test_end_to_end.py` - the full pipeline, automated

This is the automated version of what we did by hand while building
Phase 5: produce a small set of known events straight to Kafka (one
event sent twice on purpose, one deliberately invalid), launch the real
`ingestion/app/main.py` as a subprocess, wait for its consumer group's
lag to hit zero, stop it, and check Postgres for exactly what should be
there - the valid events once each, the duplicate not creating a second
row, and the invalid event never inserted at all.

Requires the full stack running:

```bash
docker compose up -d
python3 tests/ingestion/test_end_to_end.py
```

It shares the real `ingestion-service` consumer group, so it will also
drain any other backlog sitting in the topic before it can confirm
lag=0 - if you've been generating a lot of traffic, the first run after
that may take longer (default timeout: 30s).

## Generating some real traffic to inspect

```bash
cd producer
EVENTS_PER_SECOND=20 python3 -m app.main
# Ctrl+C to stop - it flushes and prints final sent/delivered/failed counts
```
