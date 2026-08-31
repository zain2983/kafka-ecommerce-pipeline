# Manual testing scripts

These are exploratory/manual scripts, not an automated pytest suite. They
exist so you can poke at the system by hand and see what's actually
happening.

```
tests/
├── producer/
│   └── test_event_generator.py   tests producer/app code in isolation, no Kafka
├── kafka/
│   ├── inspect_kafka_topic.py    inspects any topic on the cluster (not producer-specific -
│   │                              also useful for the DLQ/retry topics added in later phases)
│   └── inspect_consumer_group.py inspects a consumer group: member assignment + per-partition lag
├── ingestion/
│   ├── test_validator.py                    tests normalize_event()/validate_event() in isolation, no Kafka
│   ├── test_end_to_end.py                   full pipeline: produce -> real consumer subprocess -> verify Postgres
│   ├── test_failure_recovery_postgres.py    kills postgres mid-stream, restarts it, confirms recovery
│   └── test_failure_recovery_crash.py       SIGKILLs the consumer mid-stream, restarts it, confirms recovery
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

## 3. `inspect_consumer_group.py` - see partition assignment + lag

Also requires the `.venv` to be active. Read-only (AdminClient calls only)
- never joins the group or consumes anything, so running it has zero
effect on the real consumer's assignment or committed offsets.

Prints who currently belongs to the `ingestion-service` consumer group,
which partitions each member owns, and each partition's lag (committed
offset vs. high watermark - i.e. its unprocessed backlog).

```bash
python3 tests/kafka/inspect_consumer_group.py
```

```
Group: ingestion-service
State: ConsumerGroupState.STABLE
Partition assignor: range

2 member(s):

  client.id=consumer-A  host=/192.168.65.1
    owns partitions: [0, 1]
  client.id=consumer-B  host=/192.168.65.1
    owns partitions: [2]

Partition  Committed   High Watermark     Lag
------------------------------------------------
        0        774              774       0
        1        750              750       0
        2        831              831       0

Total lag across all partitions: 0
```

Add `--watch 2` to refresh every 2 seconds - useful for watching a
rebalance happen live while you start/stop consumer instances in other
terminals.

### Demonstrating partitions + consumer groups (design.md section 26)

`ecommerce-events` has 3 partitions, and the ingestion consumer's
`group.id` defaults to `ingestion-service` (`KAFKA_CONSUMER_GROUP`).
Running more than one `app.main` process with the *same* group.id joins
them into the same group, and Kafka splits the partitions between them
- that's what a consumer group is for: horizontal scaling within a
topic, with Kafka guaranteeing no two members ever read the same
partition at the same time.

```bash
cd ingestion

# Terminal 1
KAFKA_CONSUMER_INSTANCE_ID=consumer-A python3 -m app.main

# Terminal 2 (same group.id - the default - so it joins consumer-A's group)
KAFKA_CONSUMER_INSTANCE_ID=consumer-B python3 -m app.main
```

`KAFKA_CONSUMER_INSTANCE_ID` is just a readable label for logs (it maps
to Kafka's `client.id`, not the group's internal member id) - it makes
it possible to tell which instance's log lines are which when running
several side by side. Watch either terminal's logs for lines like:

```
partitions REVOKED (rebalance starting): [0, 1, 2]
partitions ASSIGNED: [0, 1]
```

Then run `inspect_consumer_group.py` (above) in a third terminal to see
the same assignment from the broker's point of view. Ctrl+C one of the
two consumers and watch the other one log a REVOKED/ASSIGNED pair as it
picks up the abandoned partition(s) - that's the same rebalance
mechanism Kafka uses for failure recovery (Phase 7): from the group's
perspective, "a consumer crashed" and "a consumer was stopped on
purpose" look identical.

## 4. `test_validator.py` - test normalization/validation alone

Feeds hand-written cases (clean, messy-but-fixable, and genuinely broken)
through the real `ingestion/app/validator.py` code. No Kafka, no Postgres.

```bash
python3 tests/ingestion/test_validator.py
```

## 5. `test_database.py` - test idempotent inserts alone

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

## 6. `inspect_raw_events.py` - see what's actually in Postgres

The Postgres-side counterpart to `inspect_kafka_topic.py`. Prints row
count vs. distinct `event_id` count (these should always be equal - a
mismatch would mean the idempotent insert is broken), a breakdown by
`event_type`, and the most recent rows.

```bash
python3 tests/postgres/inspect_raw_events.py
python3 tests/postgres/inspect_raw_events.py --limit 20
python3 tests/postgres/inspect_raw_events.py --user-id user_42
```

## 7. `test_end_to_end.py` - the full pipeline, automated

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

## 8. `test_failure_recovery_postgres.py` - Postgres outage recovery (Phase 7)

Proves design.md sections 10/13/25's "transient PostgreSQL outage" flow
actually holds, not just that the retry-loop code reads correctly:

1. Produces one known test event.
2. Stops the `ecommerce-postgres` container, THEN starts the ingestion
   consumer - every DB write attempt is guaranteed to fail.
3. Confirms the consumer retries, gives up on its own (doesn't hang),
   and never commits the offset (lag stays >= 1).
4. Restarts postgres, starts a fresh consumer, confirms it re-reads the
   same message (nothing was lost) and writes it exactly once
   (idempotency held even across a full process restart).

```bash
docker compose up -d
python3 tests/ingestion/test_failure_recovery_postgres.py
```

Stops/restarts the shared `ecommerce-postgres` container - don't run
this while relying on postgres for something else. It always leaves
postgres running when it finishes, even if an assertion fails partway
through.

Along the way, this test is also what caught a real gap: `main.py`'s
`EventDatabase(...)` used to connect eagerly at startup with no retry,
so if postgres was already down when the consumer *started* (as opposed
to going down while it was already running), the process died with a
raw, unhandled traceback instead of the same "retry, then give up
loudly" behavior every other outage gets. `_connect_with_retry()` in
`main.py` fixes that - and closes the Kafka consumer before exiting on
total failure, so a `docker exec` shell watching group membership never
sees a "phantom" member sitting there for a stale session timeout.

## 9. `test_failure_recovery_crash.py` - process crash recovery (Phase 7)

Same idea, different failure mode: instead of the *dependency* going
down, the ingestion process itself dies abruptly (SIGKILL - no signal
handler runs, nothing gets a chance to flush pending offsets).

1. Produces 150 known test events.
2. Starts the real consumer, lets it run for ~8s (long enough to join
   the group and process a good chunk of the batch), then SIGKILLs it.
3. Confirms whatever landed in Postgres before the kill has no
   duplicates (every write is its own transaction).
4. Starts a fresh consumer. Kafka only learns the old member is gone
   once its session times out (~45s by default - a real crash gives
   Kafka no chance to be told sooner, which is exactly the scenario
   this is testing), then rebalances the abandoned partitions onto the
   new instance, which may re-process some already-written events.
5. Confirms all 150 events end up present, and exactly 150 rows exist -
   proving the re-processing overlap from step 4 never created a
   duplicate.

```bash
docker compose up -d
python3 tests/ingestion/test_failure_recovery_crash.py
```

Takes roughly a minute, mostly spent waiting out that session timeout -
that wait is Kafka correctly doing its job, not something to work
around.

## Generating some real traffic to inspect

```bash
cd producer
EVENTS_PER_SECOND=20 python3 -m app.main
# Ctrl+C to stop - it flushes and prints final sent/delivered/failed counts
```

## Consumer env vars added in Phase 7

- `KAFKA_CONSUMER_INSTANCE_ID` - readable label for a consumer process's
  logs (maps to Kafka's `client.id`), useful when running multiple
  instances at once (see section 3 above).
- `COMMIT_BATCH_SIZE` (default `20`) / `COMMIT_INTERVAL_SECONDS`
  (default `2.0`) - batched offset commits (design.md section 10.1):
  the consumer commits once per this many confirmed messages or this
  many seconds, whichever comes first, instead of once per message.
