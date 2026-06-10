# How it works

`faststream-outbox` is a FastStream broker integration whose transport is
**Postgres rows**, not a message bus. A producer writes an outbox row in the
same SQLAlchemy transaction as its domain entity; a subscriber polls the
table, claims rows with `FOR UPDATE SKIP LOCKED`, runs the handler, and
deletes the row on success.

## The transactional outbox pattern

Distributed systems need two writes to atomically succeed or fail together:
the business write (place an order) and the message-bus write (notify
downstream). Brokers don't participate in your database transaction, so a
crash between the two leaves them out of sync.

The outbox solves this by collapsing both writes into a single database
transaction. Instead of publishing to a broker, you `INSERT` a row into an
`outbox` table on the same `AsyncSession` that holds your domain write. A
separate process polls the table and forwards rows to their consumers. The
row commits with your domain write or rolls back with it — atomicity is
free.

`faststream-outbox` collapses the third "separate process" into the
subscriber itself: the same Postgres table holds the queue, and the
subscriber's polling loop *is* the consumer. No relay process, no Kafka, no
Rabbit.

*See [Comparison](../concepts/comparison.md) for when CDC or Kafka
transactions are the better fit.*

## Producer side

`broker.publish(body, *, queue, session, ...)` inserts an outbox row through
the caller's `AsyncSession`. It does **not** flush, commit, or open its own
transaction — the row must commit with the caller's domain writes:

```python
async with session_factory() as session, session.begin():
    session.add(order)                                    # domain write
    await broker.publish(order.id, queue="orders", session=session)
    # session.begin() commits both atomically on exit
```

`publish_batch(*bodies, queue, session, ...)` does the same with a single
round-trip for many rows.

The producer also emits `SELECT pg_notify('outbox_<table>', queue)` on the
caller's session right after the INSERT, **except** when the row is
future-dated (`activate_in` / `activate_at` set) or a `timer_id` conflict
made the insert a no-op. NOTIFY is transactional, so listeners only see it
after the user's transaction commits — atomicity with the row insert is
automatic.

## Subscriber: two async loops

Per subscriber, two loops run concurrently:

**1. Fetch loop.** Owns a long-lived `AsyncConnection` for the fetch CTE and
a separate raw asyncpg connection for `LISTEN outbox_<table>`. A single CTE
claims rows:

```sql
WITH claimed AS (
    SELECT id FROM outbox
    WHERE queue = :queue
      AND next_attempt_at <= now()
      AND (
        acquired_token IS NULL
        OR acquired_at < now() - make_interval(secs => :lease_ttl)
      )
    ORDER BY id
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox SET acquired_token = :uuid, acquired_at = now()
WHERE id IN (SELECT id FROM claimed)
RETURNING *
```

The CTE reclaims both unleased rows AND rows whose lease has expired
(`acquired_at < now() - lease_ttl_seconds`), so there is no separate stuck-row
reaper. The idle-sleep is short-circuited by NOTIFY via an `asyncio.Event` —
idle dispatch latency drops from up to `max_fetch_interval` (default 10s) to
~10ms. If LISTEN setup fails (asyncpg missing, non-asyncpg driver, permission
error), the loop logs once and falls back to polling.

**2. Worker loop** (× `max_workers`). Pulls from an in-process
`asyncio.Queue(maxsize=fetch_batch_size)`, dispatches via the handler, then
flushes the row's terminal state (`DELETE` on success, `UPDATE
next_attempt_at` for retry). Each worker owns a long-lived `AsyncConnection`,
so draining N rows costs O(workers) pool checkouts, not O(rows).

## The lease-token invariant

Every terminal write filters on `acquired_token`:

```sql
DELETE FROM outbox WHERE id = :id AND acquired_token = :token
```

If a slow handler's lease expired and another worker reclaimed the row with
a fresh token, the slow handler's `DELETE` finds `rowcount == 0` and is
silently dropped — preventing it from clobbering the new lease holder. This
is the load-bearing invariant; any new fetch or terminal path must preserve
it.

`lease_ttl_seconds` (default `60.0`, per-subscriber) **must exceed the P99
handler duration with margin**, otherwise healthy in-flight handlers race
their own lease expiry and trigger duplicate deliveries. The lease cutoff is
computed server-side via `make_interval(secs => :lease_ttl)`, so it's
immune to worker / DB clock skew.

When the invariant fires, the broker emits a WARNING with structured fields:

```python
extra={"event": "lease_lost", "phase": "terminal" | "retry",
       "row_id": ..., "queue": ..., "deliveries_count": ...}
```

Recurring `event=lease_lost` records mean `lease_ttl_seconds < handler P99`
— that's the operator playbook signal. Log-pipeline aggregators can alert
on the `event` field without parsing the message.

## At-least-once delivery

The row is removed from the table only after the handler completes
successfully. If the worker dies mid-handler, the lease expires and another
worker re-claims the row. The same applies if the handler ran but the
worker crashed before the terminal `DELETE` landed.

The trade-off: handlers must be **idempotent**. A handler that succeeded
but whose `DELETE` failed to land will be retried.

## Opt-in DLQ on terminal failure

By default, terminal failures `DELETE` the row — no archive table, no
dead-letter queue. Pass `dlq_table=make_dlq_table(metadata)` to the broker
and terminal-by-failure rows are copied into a sibling audit table in the
same Postgres statement as the `DELETE`:

```python
from faststream_outbox import OutboxBroker, make_dlq_table, make_outbox_table

metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")
dlq_table = make_dlq_table(metadata, table_name="outbox_dlq")
broker = OutboxBroker(engine, outbox_table=outbox_table, dlq_table=dlq_table)
```

Successful rows are never archived — the success path stays a plain
`DELETE`. Three failure paths land in the DLQ with a `failure_reason`
column: `max_deliveries`, `retry_terminal`, `rejected`. Atomicity is via a
single CTE (`DELETE … RETURNING` → `INSERT INTO <dlq>`), so DLQ-write
failures roll back the `DELETE` — misconfiguration surfaces as outbox
growth plus `lease_lost` spikes rather than silent audit loss. When
`dlq_table` is configured, `broker.validate_schema()` checks both tables
in one call and reports drift on either one. See the
[Dead-letter queue](../usage/dlq.md) page for the schema, atomicity, and
retention story.

If you don't want a DLQ, you can still preserve failed messages by logging
from the handler before the terminal failure propagates, or by attaching
an audit column to the outbox table (the schema validator ignores extras
you add).

## Failure modes

- **Handlers must be idempotent.** Crash between commit-of-handler-side-effects and the broker's `DELETE` re-delivers the message.
- **Best-effort ordering only.** `FOR UPDATE SKIP LOCKED` does not preserve strict order under concurrent workers. If you need strict per-aggregate ordering, route to a single subscriber and run a single worker.
- **DLQ is opt-in.** Without `dlq_table=`, terminal failures `DELETE` the row.

## Relay to Kafka / RabbitMQ / NATS / Redis

> **Relay outbox rows to Kafka / RabbitMQ / NATS / Redis with a single decorator → [Relay tutorial](../usage/relay.md).**

## Acknowledgements

The architecture of this package is heavily informed by Arseniy Popov's
[PR #2704](https://github.com/ag2ai/faststream/pull/2704) (`feat: add sqla
broker`) on upstream FastStream — the FastStream broker/registrator/subscriber
wiring, the `SELECT … FOR UPDATE SKIP LOCKED` fetch-and-claim CTE, the retry
strategy hierarchy, and the in-transaction publish contract all originate
from there. This package is a Postgres-only reimplementation that diverges in
storage model (lease tokens instead of an explicit state column, opt-in
DLQ instead of a mandatory archive table), loop structure (two loops
instead of four), wake-up mechanism (`LISTEN/NOTIFY`), and adds timer
mechanics. Credit for the original design
belongs to Arseniy.
