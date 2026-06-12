# faststream-outbox

`faststream-outbox` is a [FastStream](https://faststream.airt.ai) broker
integration for the **transactional outbox pattern** — a Postgres table is
the message queue. A producer writes a domain entity and an outbox row in
the *same* SQLAlchemy transaction; a subscriber polls the table with
`FOR UPDATE SKIP LOCKED`, runs the handler, and deletes the row on
success. The table *is* the queue — no separate message bus, no relay
process, no Kafka.

## Use it when

- You already have Postgres and don't want to add a message bus just to
  get at-least-once delivery alongside your domain writes.
- You want the row insert to commit atomically with the rest of your
  SQLAlchemy transaction (no two-phase commit, no Sagas).
- You're building on FastStream or FastAPI and want the same
  subscriber / dependency-injection ergonomics for an outbox.
- You need a durable, transactionally-published feed of events that a
  separate worker [relays to Kafka / RabbitMQ / NATS](usage/relay.md)
  with the at-least-once contract preserved end-to-end.

## Reach for something else when

- You're already running Kafka / Rabbit / NATS *and* don't need
  transactional atomicity with a DB write → use that broker directly.
- You need sub-second scheduled-delivery precision → see
  [Timers § latency floor](usage/timers.md#latency-floor).
- You're on a non-Postgres database → this package is Postgres-only
  at v0. CDC / Debezium may be a better fit (see
  [Comparison](concepts/comparison.md)).
- You're modelling ad-hoc background jobs rather than events tied to a
  DB transaction → see [Comparison § vs Celery](concepts/comparison.md#vs-celery-or-rq-dramatiq-with-a-db-backend).

## Start where you're going

| If you want to… | Start at |
|---|---|
| See it work end-to-end on a FastAPI app | [FastAPI integration](usage/fastapi.md) |
| Relay outbox rows to Kafka / RabbitMQ / NATS / Redis | [Relay to Kafka / RabbitMQ / NATS](usage/relay.md) |
| Understand the architecture before adopting | [How it works](introduction/how-it-works.md) |
| Compare against CDC / Kafka transactions / a hand-rolled outbox | [Comparison](concepts/comparison.md) |
| Deploy to production safely | [Production checklist](operations/checklist.md) |
| Install and write the first publisher / subscriber | [Installation](introduction/installation.md) → [Tutorial: Your first outbox app](tutorials/first-outbox-app.md) |

## Documentation

### Getting started

- [Installation](introduction/installation.md) — install, optional
  extras (`asyncpg`, `fastapi`, `validate`, `prometheus`,
  `opentelemetry`), Postgres setup.
- [Basic usage](usage/basic.md) — declare the table, create the
  broker, publish a row, register a subscriber.

### Concepts

- [How it works](introduction/how-it-works.md) — two-loop subscriber,
  lease-token invariant, at-least-once semantics, opt-in DLQ on terminal
  failure.
- [Comparison](concepts/comparison.md) — vs writing your own, vs CDC,
  vs Kafka transactions, vs `LISTEN/NOTIFY`, vs Celery, vs FastStream
  foreign-broker direct.

### Guides

- [FastAPI integration](usage/fastapi.md) — the canonical use case:
  HTTP routes and outbox subscribers share one `AsyncSession`.
- [Relay to Kafka / RabbitMQ / NATS](usage/relay.md) — forward outbox
  rows to a real bus with one decorator; at-least-once preserved.
- [Timers](usage/timers.md) — `activate_in` / `activate_at`,
  `timer_id` dedup, `cancel_timer`.
- [Testing](usage/testing.md) — `TestOutboxBroker` sync and
  loop-driven modes.
- [Schema validation](usage/schema-validation.md) — opt-in
  Alembic-driven check for `/health` and CI.

### Reference

- [Subscriber](usage/subscriber.md) — options, ack policies, retry
  strategies, connection budget, slow-handler queue segregation.
- [Publisher](usage/publisher.md) — `publish`, `publish_batch`,
  `OutboxPublisher`, chained publishing via `OutboxResponse`.
- [Router](usage/router.md) — `OutboxRouter`, `OutboxRoute`,
  walking every subscriber via `broker.subscribers`.
- [Dead-letter queue](usage/dlq.md) — opt-in audit table, atomicity
  via a single CTE, `dlq_written` metric, retention patterns.
- [Observability](usage/observability.md) — recorder seam plus
  native Prometheus / OpenTelemetry middleware.
