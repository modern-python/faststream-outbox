# Comparison

`faststream-outbox` is one shape of "transactional outbox" with one set of
trade-offs. This page names the alternatives, says when each is the better
choice, and ends each comparison with a one-line verdict so a scanning
reader can lift the answer without reading the discussion.

## vs. writing your own outbox table and worker

A bespoke outbox is the most common starting point — the pattern itself is
straightforward, and an MVP can ship in an afternoon. What `faststream-outbox`
buys you, in concrete terms, is the pile of pieces that turn that MVP into a
production system: per-row **lease tokens** with a load-bearing invariant
that any new fetch / terminal path must preserve; the **partial-index
design** the fetch CTE depends on (without those, the disjunctive WHERE
clause falls back to seq-scan as the table grows); the **fetch-and-claim
CTE shape** with `FOR UPDATE SKIP LOCKED` that reclaims both unleased rows
and expired leases without a separate reaper; the
`_RetryStrategyTemplate` hierarchy enforcing `max_attempts` and
`max_total_delay_seconds` uniformly; `validate_schema()` via
Alembic's `autogenerate.compare_metadata`; **drain semantics** on stop with
the `running` / `_stopping` two-flag dance and parallel-gathered subscriber
shutdown; **`LISTEN/NOTIFY`** short-circuit on top of polling, with NOTIFY
suppression on future-dated rows and `timer_id` conflict no-ops;
**`timer_id` dedup** via a partial unique index plus
`on_conflict_do_nothing`; the **DLQ atomicity CTE** that rolls back the
DELETE when the DLQ insert fails. None of these is hard individually; in
aggregate they decide whether the outbox survives the second year of
production load.

You also pick up the [Subscriber](../usage/subscriber.md),
[Publisher](../usage/publisher.md), [Dead-letter queue](../usage/dlq.md), and
[Observability](../usage/observability.md) reference pages — written to
the level you would otherwise have to write yourself.

**TL;DR.** Build it yourself if you only ever need the MVP shape. Use
`faststream-outbox` if you expect the system to live for a couple of years.

## vs. CDC (Debezium, logical replication)

Change-data capture sits one layer below outbox: instead of writing rows
to an outbox table, you read your write-ahead log directly. The producer
code is unchanged — any write to the underlying tables becomes an event.
Debezium and similar tools have spent the last decade hardening this path
for Postgres, MySQL, and others; the operator playbook is well-known.

CDC wins when you already need WAL-level capture for analytics or
reverse-ETL anyway, when you want to capture writes from services you do
not control (i.e. not all your producers are FastStream apps), or when
the polling overhead of an outbox is unacceptable. CDC also wins when
the events you care about are derivable from row state — "an order
exists with status='paid'" rather than "an
`OrderPaid` event was published."

`faststream-outbox` wins when you control the producer code (so the
outbox row is cheap to write inline with the domain write), when you
need **handler-level retry, DLQ, and scheduled-delivery semantics
inline** (CDC pushes those concerns to a separate consumer layer), and
when the **async-Python logical-replication tooling gap** is too thin
to lean on. The last point is the load-bearing one for this project — a
2026-05-07 reassessment confirmed the gap had not closed sufficiently to
make CDC the recommended path here.

**TL;DR.** Pick CDC when you already need WAL capture or have producers
outside your control. Pick this when you own the producer and want
retry/DLQ/timers in-process.

## vs. Kafka transactions (or RabbitMQ publisher confirms)

Atomic `DB-write + bus-publish` is also achievable on a real bus, just
not for free. Kafka transactions plus two-phase commit, or an
idempotent-producer pattern combined with an inbox table on the consumer
side, can give you the same end-to-end at-least-once guarantee without a
DB-backed outbox.

The trade-offs are: you need the bus (Kafka or Rabbit) in your
infrastructure footprint, with all the operational mass that entails
(schema registry, consumer-group rebalancing, partition planning,
Connect, MirrorMaker, etc.); there is no native message-cancellation
analog of [`cancel_timer`](../usage/timers.md#cancellation); there is no
native `timer_id`-style deduplication built into the producer path; and
the "single transaction with arbitrary domain writes" contract is harder
to preserve, because the transactional boundary belongs to two different
systems.

`faststream-outbox` is a strict subset of what a real bus can do —
one-process producer, one Postgres table — at the price of being
Postgres-only and polling-based.

**TL;DR.** Kafka transactions / Rabbit confirms win at scale where the
bus is already running. `faststream-outbox` wins when Postgres is your
only durable store.

## vs. plain `LISTEN/NOTIFY`

`LISTEN/NOTIFY` is tempting because the wakeup channel is right there in
Postgres. The problem is that the channel is fire-and-forget and lossy
across listener disconnect: a NOTIFY emitted while the listener's
connection is dead, or during a reconnect, is silently dropped. There is
no replay, no persistence, no retry.

`faststream-outbox` keeps the **outbox row** as the durability boundary
and uses NOTIFY only as a wake-up short-circuit on top of polling. If
the NOTIFY is lost — listener reconnecting, channel name too long
(Postgres' 63-char limit), `LISTEN` setup failed at startup — the
subscriber still finds the row on its next poll cycle. The worst case
is one `max_fetch_interval` of idle latency (default 10 seconds), not
data loss.

**TL;DR.** Raw `LISTEN/NOTIFY` is a wake-up, not a delivery guarantee.
Use the outbox row for durability and let NOTIFY shave idle latency.

## vs. Celery (or RQ, Dramatiq) with a DB backend

Celery and friends are *task queues* — you submit "go do this thing" and
a worker picks it up later. `faststream-outbox` is *message routing* with
FastStream's subscriber/publisher semantics — the row is an event tied
to a domain write, and the handler is the consumer for events on that
queue.

The two abstractions overlap, but the right one depends on what you are
modelling. Celery wins for ad-hoc background jobs initiated from
arbitrary points in your app (request handlers, admin commands, cron),
where the relationship to a database transaction is incidental. Use
`faststream-outbox` when you want **at-least-once dispatch of events
that must commit atomically with a domain write**, and prefer
FastStream's `@broker.subscriber` model over Celery's task decorator.

The two can also coexist — Celery for fire-and-forget background jobs,
`faststream-outbox` for the transactional event tier. They are not in
direct competition for the same problem.

**TL;DR.** Celery for ad-hoc background jobs. `faststream-outbox` for
events tied to DB transactions.

## vs. FastStream + `KafkaBroker` / `RabbitBroker` directly

If you have no domain write to atomically commit alongside the bus
publish, drop the outbox entirely — use the foreign broker directly via
FastStream's native `KafkaBroker`, `RabbitBroker`, `NatsBroker`, etc.
You skip the polling overhead and the Postgres dependency; you keep the
same `@broker.subscriber` ergonomics.

The interesting case is **both at once**: domain code writes to Postgres
*and* needs the event to reach Kafka. That is the canonical
transactional-outbox shape, and it composes the two: the outbox row
captures the event in the domain transaction; a
[Relay](../usage/relay.md) subscriber forwards it to Kafka with the
at-least-once contract preserved end to end. Don't pick between
`faststream-outbox` and a real bus — use both, with the outbox as the
durability boundary in front of the bus.

**TL;DR.** No DB write to commit with? Use the foreign broker directly.
Need atomicity with a DB write? Use this *plus* the foreign broker via
[Relay](../usage/relay.md).
