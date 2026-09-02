# Failure Scenarios & Recovery

design.md section 3 sets this as an explicit goal for the project: it
should "eventually be able to demonstrate failure scenarios and explain
how the system recovers from them." This document is that explanation -
one place that ties together every failure mode this project has
actually tested (not just designed for), what guarantees hold, how to
reproduce each one yourself, and where the honest limits are.

Every scenario below has a real, runnable test behind it under `tests/`.
None of this is theoretical - each claim was verified against the real
services, and several were only found *because* of that (see "Bugs this
process actually found," below).

## The two guarantees everything else builds on

1. **At-least-once delivery, never less.** An event's Kafka offset is
   only ever committed *after* its outcome is confirmed - written to
   Postgres, or deliberately routed to the DLQ (design.md section 10).
   A crash between "confirmed" and "committed" just means that event
   gets redelivered and reprocessed on restart. Nothing is ever silently
   dropped because of an offset commit that outran reality.
2. **Idempotency backstops redelivery.** `raw.events.event_id` is a
   Postgres primary key, and every insert uses `ON CONFLICT (event_id)
   DO NOTHING` (design.md section 11). So the "redelivered and
   reprocessed" half of guarantee #1 is always safe - a duplicate
   insert is a no-op, not a second row and not an error.

Almost every scenario below is really just these two guarantees getting
exercised by a different kind of failure.

---

## Scenario: PostgreSQL becomes unreachable mid-stream

**Test:** `tests/ingestion/test_failure_recovery_postgres.py`

| | |
|---|---|
| Stops | `ecommerce-postgres` |
| While running | the ingestion consumer |
| Reproduce | `python3 tests/ingestion/test_failure_recovery_postgres.py` |

The consumer retries the write a bounded number of times (`DB_RETRY_ATTEMPTS`,
reconnecting between attempts), then - if Postgres is still down - stops
the process entirely **without** committing that message's offset,
rather than skipping it and moving on. Skipping would mean the stuck
event, and the fact that the partition ever got stuck, disappears with
no record. Once Postgres is healthy again, restarting the consumer
picks up from the exact same message and continues normally.

Also covers a second, related case: Postgres being down *at startup*,
before the consumer has even subscribed. This used to crash the process
with a raw traceback (`EventDatabase` connects eagerly in its
constructor) - now it retries with the same policy as the mid-stream
case (`main.py`'s `_connect_with_retry`).

## Scenario: the ingestion consumer process crashes outright

**Test:** `tests/ingestion/test_failure_recovery_crash.py`

| | |
|---|---|
| Kills | the ingestion consumer (`SIGKILL`, not a graceful stop) |
| Reproduce | `python3 tests/ingestion/test_failure_recovery_crash.py` |

Sends 150 known events, lets the consumer process a portion of them,
then kills it with no chance to run any cleanup code at all. A fresh
consumer instance is then started. Because the kill can land between "row
written to Postgres" and "offset committed," the restarted consumer may
re-read and reprocess some events that were already written - this is
exactly the at-least-once + idempotency combination working as
designed, not a bug. The test confirms all 150 events end up present,
and confirms **exactly** 150 rows exist - proving the reprocessing
overlap never created a duplicate.

## Scenario: the Kafka broker itself goes down

**Test:** `tests/ingestion/test_failure_recovery_kafka.py`

| | |
|---|---|
| Stops | `ecommerce-kafka` |
| While running | the real producer AND the real ingestion consumer |
| Reproduce | `python3 tests/ingestion/test_failure_recovery_kafka.py` (slow - see below) |

Different from the previous two scenarios: here the shared piece of
infrastructure both the producer and the consumer depend on disappears.
Confirms neither process crashes (librdkafka retries broker connections
transparently; messages the producer sends during the outage are
queued, not dropped, since the outage is well under `message.timeout.ms`),
and that once Kafka is back, both fully recover - the producer's
sent/delivered counts converge to equal, and freshly-produced events
land in Postgres again.

**This can be slow.** A consumer that was already joined to the group
before the outage doesn't notice its coordinator is gone until its own
client-side session timeout elapses (~45s by default) - and that clock
starts counting from whenever the coordinator connection was last
healthy, which can be well before Kafka's own healthcheck reports
"healthy" again. When that happens, a full run can take several
minutes; that's Kafka's own client behavior working as designed, not
something to optimize around - see the test file's docstring for the
full history of *why* its timeout is tuned generously rather than
tightly.

**Verification status:** three consecutive clean passes, including one
with the post-recovery settling buffer (below) deliberately disabled to
force exposure to the coordinator-instability window - the consumer
rejoined and recovered cleanly every time. Before the fix described
below, this test had never passed - it's gone from 0/7 to 3/3 since.

## Scenario: an invalid or unparseable event arrives

**Test:** `tests/ingestion/test_dlq_flow.py`

| | |
|---|---|
| Injects | one unparseable (not valid JSON) and one invalid (fails validation) event |
| Reproduce | `python3 tests/ingestion/test_dlq_flow.py` |

Neither kind of bad event is allowed to block anything after it in the
partition. Both get routed to `ecommerce-events-dlq` with a full
record - reason, validation errors, the original raw payload, and
where it came from (design.md section 12) - and the main topic's offset
still advances. The test then exercises the full recovery loop:
replaying both DLQ records unchanged (both fail again, correctly,
and reappear on the DLQ with `retry_count` incremented - proving a
failed retry re-enters the DLQ rather than vanishing or blocking the
retry topic), then replaying a manually-corrected version of the
invalid one and confirming it lands in `raw.events` exactly once.

Operationally, this loop is: inspect the DLQ
(`tests/kafka/inspect_kafka_topic.py --topic ecommerce-events-dlq --show`),
optionally fix a record, then requeue it with
`tests/kafka/replay_dlq.py`, which `retry_main.py` (a separate consumer
group, `retry-service`) picks up and re-attempts through the same
normalize/validate/insert pipeline as the primary consumer.

## Scenario: the same event is delivered more than once

**Test:** `tests/ingestion/test_dedup_stress.py`

| | |
|---|---|
| Injects | 40 unique events x 5 copies each (200 messages), shuffled so duplicates aren't adjacent |
| Reproduce | `python3 tests/ingestion/test_dedup_stress.py` |

This is the two-guarantee section's idempotency half, stress-tested
deliberately. The consumer keeps an in-memory, bounded (LRU + TTL)
cache of recently-seen `event_id`s as a fast path - a cache hit skips
the Postgres round-trip entirely. The test runs with `DEDUP_CACHE_SIZE`
set deliberately smaller than the number of unique events, forcing the
cache to evict mid-run, so some duplicates are guaranteed to miss the
fast path and fall through to Postgres's `ON CONFLICT DO NOTHING`. The
test confirms both paths actually fire (not just one, by luck of
timing) and that **exactly** 40 rows land - zero duplicates, regardless
of which layer caught which copy.

## Scenario: a consumer instance joins or leaves the group

**Manual demo:** `tests/kafka/inspect_consumer_group.py --watch 2`

Not a failure exactly, but the mechanism every other scenario above
relies on for recovery. Run two `python -m app.main` processes with the
same `group.id` (the default, `ingestion-service`) and Kafka
automatically splits the topic's 3 partitions between them - watch it
happen live via `--watch`, or in either process's own logs
(`partitions ASSIGNED`/`partitions REVOKED`). Stop one and watch the
survivor absorb the abandoned partitions. This is the exact same
rebalance mechanism that makes the crash-recovery and Kafka-outage
scenarios above actually work - "a consumer crashed" and "a consumer
was stopped on purpose" look identical to Kafka.

## Scenario: the monitoring stack itself fails

**Test:** `tests/grafana/test_monitoring_resilience.py`

| | |
|---|---|
| Kills | `kafka-exporter` (SIGKILL) |
| Reproduce | `python3 tests/grafana/test_monitoring_resilience.py` |

Confirms observability tooling is a passenger, not a dependency: a real
ingestion consumer keeps ingesting normally the entire time
`kafka-exporter` is dead (nothing in `ingestion/app/*` imports or
configures anything related to it). Confirms Prometheus notices the
target went down rather than silently serving stale data, and that once
`kafka-exporter` is back, Prometheus resumes scraping and every Grafana
dashboard panel query works again (reusing
`tests/grafana/verify_stack.py`'s own checks).

Also confirms `restart: unless-stopped` (`docker-compose.yml`) is
actually configured on `kafka-exporter` - though see the note on
`docker kill` below for why that's checked statically rather than
triggered live in this specific test.

---

## Startup-ordering races (not "failures" exactly, but real bugs)

Two services used to start before something they depended on was
*actually* ready, as opposed to merely "started":

- **Grafana → Postgres:** on a fresh `docker compose up`, Grafana could
  provision its Postgres datasource and run its own connection test
  before Postgres's healthcheck had passed - leaving the datasource
  stuck showing a connection error until something retried it.
- **kafka-exporter → Kafka:** similarly, kafka-exporter could try to
  connect before Kafka was actually accepting connections - and unlike
  this project's own Python services (which retry, then give up
  loudly), kafka-exporter treats that as fatal and just exits.

Both are fixed the same way: real `healthcheck:` blocks on `kafka` and
`postgres`, and `depends_on: ...: condition: service_healthy` on
whatever needs to wait for them (`docker-compose.yml`). A plain
`depends_on: [service]` only waits for the container to *start*, never
for it to be *ready* - this distinction is the root cause behind more
than one bug in this project.

## Bugs this process actually found

Worth naming explicitly, since finding real bugs is the actual point of
building and running these tests rather than just designing for the
scenarios on paper:

1. **Grafana/kafka-exporter startup races** (above) - found by testing
   a genuinely cold `docker compose down && up`, not just a restart.
2. **Unhandled `KafkaException` on offset commit.** A Kafka broker
   restart briefly leaves the group coordinator unavailable, and the
   consumer's commit call used to raise, unhandled, crashing the
   process outright - confirmed against a real Kafka restart while
   building `test_failure_recovery_kafka.py`.
3. **Blocking offset commits could starve the poll loop entirely.**
   Worse than #2, and much less obvious: that same commit call used to
   be *blocking* with no bounded timeout. Under sustained coordinator
   instability it could hang for minutes - and while hung, the consumer
   never called `poll()` again, which Kafka's `max.poll.interval.ms`
   (5 minutes by default) treats as "this consumer is dead," evicting
   it from the group and turning a transient hiccup into a much bigger
   one. Fixed at the source: `kafka_consumer.py`'s commits are
   asynchronous now, which structurally cannot block the poll loop no
   matter how unstable the coordinator gets.

## Known limitation: a long-lived consumer can, rarely, get wedged after a Kafka outage

While validating the fix above, one run surfaced something the fix
didn't fully explain: the *same* long-lived consumer process, after
surviving a Kafka outage and going through the session-timeout-then-
rejoin cycle, was observed alive (TCP-connected, no errors) but
permanently making zero progress - not a member of the consumer group,
not processing anything, indefinitely. A **brand-new** consumer process
pointed at the exact same group joined in about 3 seconds and drained
everything instantly, proving the broker, the data, and the group were
never the problem.

This is documented rather than silently fixed because the root cause
isn't understood yet - it may be a librdkafka internal state issue
after repeated rebalance cycles, or something else client-side.

**Update:** after the async-commit fix above, `test_failure_recovery_kafka.py`
was re-run three times in a row - once with its post-recovery settling
buffer deliberately disabled to force exposure to the same
coordinator-instability window - and the wedge did not recur; the
consumer rejoined and recovered cleanly every time. The original
occurrence happened during a long debugging session with many
concurrent background processes, so it may have been sensitive to
system load in a way that's hard to reproduce on demand rather than a
deterministic bug - but that's a plausible explanation, not a
confirmed one, since the mechanism is still not understood. The test
was also upgraded to write subprocess logs straight to a file instead
of only capturing them at the end, specifically so that if this recurs,
there will be real-time visibility into what the consumer was doing
throughout, not just a confusing "no output for 13 minutes" artifact.

**Practical mitigation, still worth knowing:** if a consumer ever
appears stuck after a Kafka outage (lag not decreasing, no new
`INSERTED`/`DUPLICATE`/`INVALID` log lines, and
`tests/kafka/inspect_consumer_group.py` shows it missing from the
group's member list), restart the process - a fresh instance has
rejoined instantly in every case observed, including the one that
prompted this section.

## Data loss / backup story (design.md section 29.3)

Everything above covers services being temporarily *unreachable* -
Postgres/Kafka/the consumer come back and no data is lost, because the
two guarantees at the top of this document hold. None of it covers the
volume itself being lost or corrupted (disk failure, `docker volume rm`
by accident, a bad `docker compose down -v`). `postgres_data` and
`kafka_data` were both single Docker volumes with no dump/snapshot
process at all until this section - here's what closes that gap, and
why the two services get different answers.

### Postgres: a real, tested backup

`scripts/backup_postgres.sh` runs `pg_dump` (custom format - self-
compressed, restorable with `pg_restore`) against the live
`ecommerce-postgres` container, on a cron schedule (every 6 hours on
the deployed VM - see `docs/DEPLOYMENT.md`'s "Backups" step), pruning
dumps older than `BACKUP_RETENTION_DAYS` (7, by default). This gives
Postgres an RPO (recovery point objective) of **at most 6 hours** - if
`postgres_data` is destroyed the instant before a scheduled backup, up
to 6 hours of ingested events could be lost; typically much less, since
that's the worst case, not the average.

`tests/postgres/test_backup_restore.py` proves this isn't just "the
script exits 0": it inserts a known row, runs the real backup script,
restores the resulting dump into a scratch database, and confirms the
row is actually there - the same drill documented as a manual restore
procedure in `backup_postgres.sh`'s own header, automated and run in CI
on every push (`.github/workflows/ci.yml`) so a change that silently
broke backups (a schema change pg_dump can't handle, a permissions
issue, a renamed container) would fail loudly instead of being
discovered the first time an actual restore is needed.

Why 6 hours, not more or less frequent: this project's traffic is
synthetic and disposable (design.md's stated goal is "free, local,
reproducible, and portfolio-ready," not a real business with a real
uptime SLA), so a multi-hour RPO is an accepted tradeoff against
runtime cost/complexity, not a compromise forced by a real constraint.
Tightening it is a one-line change (`BACKUP_RETENTION_DAYS`/the cron
schedule in `docs/DEPLOYMENT.md`) whenever real data actually justifies it.

### Kafka: a documented accepted-loss window, not a backup mechanism

Kafka here is a single broker with replication factor 1 *everywhere*
(`docker-compose.yml`'s `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`/
`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`/topic creation in
`kafka/init/create_topics.sh` all set RF=1) - there is no second copy
of any message anywhere. If `kafka_data` is lost, everything on it is
gone; no `pg_dump`-equivalent exists for Kafka in this project, and
none is being added.

That's a deliberate decision, not a silent gap, for two reasons:

1. **Kafka here is a transport layer, not the system of record.**
   Every message that matters is meant to land in `raw.events`
   (Postgres) within seconds of being produced - that's what the
   consumer's job *is*. Once a message is durably in Postgres, losing
   Kafka's copy of it is a non-event. Postgres's own backup story above
   is what actually protects against permanent data loss; Kafka's job
   is to survive the *transient* outages covered earlier in this
   document, not to be a second, independent archive.
2. **Real replication (RF≥3, multiple brokers) is the standard fix, and
   is explicitly out of scope for a single-VM demo project** - it needs
   multiple broker processes/volumes/disks to actually protect against
   anything (RF=3 pointed at one disk protects against nothing a single
   broker doesn't already have), which is real infrastructure cost and
   complexity this project's stated goals (design.md: "free, local,
   reproducible, and portfolio-ready") don't call for.

**The accepted data-loss window for Kafka, stated plainly:** anything
sitting in `ecommerce-events`/`ecommerce-events-dlq`/
`ecommerce-events-retry` that the relevant consumer hasn't yet
committed an offset for, at the moment `kafka_data` is destroyed, is
gone permanently. In steady-state operation this is small - the
Grafana dashboard's Consumer Lag panels (and the `ConsumerLagGrowingUnbounded`
alert, design.md section 29.2) are what make that window visible, since
lag *is* the size of this exposure at any given moment. It grows
exactly when something is already wrong (a stuck consumer, a Kafka
outage in progress) - which is precisely when both those signals would
already be firing.

## Note on `docker kill` vs. a genuine crash

Several tests above SIGKILL a container to simulate a crash. Worth
knowing if you extend this: Docker's restart policies (`restart:
unless-stopped`, used everywhere in this project) deliberately do
**not** apply when a container is stopped via an explicit API call -
`docker kill`, `docker stop`, and even a
`kill` sent through `docker exec` all count as "you told me to stop,"
which Docker assumes was intentional and won't override. The policy
only fires when the containerized process dies completely unprompted.
This is why `test_monitoring_resilience.py` checks the restart policy's
*configuration* directly (`docker inspect`'s `HostConfig.RestartPolicy`)
rather than trying to trigger it live - and why the policy having
worked, once, organically (kafka-exporter's original startup-race
crash in Phase 11) is the strongest real evidence it works, even though
no test here can reproduce that exact trigger on demand.
