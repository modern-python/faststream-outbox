# Dead-letter queue

Opt-in audit for terminal failures. Pass `dlq_table=make_dlq_table(metadata)`
to the broker and every row that fails terminally is copied into the DLQ in
the same Postgres statement as the outbox `DELETE`. Default behavior is
unchanged when `dlq_table` is omitted — no audit table, no new code paths.

## Quickstart

Build the DLQ `Table` on the same `MetaData` as your outbox table so
Alembic discovers both, then wire it into the broker:

```python
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_outbox import OutboxBroker, make_dlq_table, make_outbox_table


metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")
dlq_table = make_dlq_table(metadata, table_name="outbox_dlq")

engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")
broker = OutboxBroker(engine, outbox_table=outbox_table, dlq_table=dlq_table)
```

The package does not create or migrate the table — run `metadata.create_all`
(or your Alembic migration) once both tables are declared. Subscribers and
publishers need no further configuration; the broker reads `dlq_table` from
its own config when it builds the terminal-flush SQL.

## What gets archived

A row lands in the DLQ when it is **terminal-by-failure**, i.e. the
subscriber's terminal flush would otherwise `DELETE` the row because of a
failure (not a clean ack). Three paths produce that:

| `failure_reason` | Trigger |
|---|---|
| `max_deliveries` | `deliveries_count > max_deliveries` — handler is never invoked for this attempt. |
| `retry_terminal` | Handler raised; the retry strategy returned `None` (attempts / total-delay exhausted, or `NoRetry()`). |
| `rejected` | Handler called `await msg.reject()` directly, or `AckPolicy.REJECT_ON_ERROR` rejected the row on an exception. |

Successful rows (`ack`) are never archived; the success path stays a plain
`DELETE` and never touches the DLQ. See
[Ack policy](./subscriber.md#ack-policy) and
[Retry strategies](./subscriber.md#retry-strategies) for the upstream
behaviors that decide which reason fires.

## Schema reference

`make_dlq_table(metadata, table_name="outbox_dlq")` declares:

| Column | Type | Notes |
|---|---|---|
| `id` | `BigInteger`, PK, autoincrement | DLQ row identity. |
| `original_id` | `BigInteger`, not null | The outbox row's id, for operator forensics. Not unique — a re-delivered `timer_id` row could legitimately land here twice. |
| `queue` | `String(255)`, not null | Source queue name. |
| `payload` | `LargeBinary`, not null | Verbatim copy of the outbox payload bytes. |
| `headers` | `JSONB`, nullable | Verbatim copy, including the inherited `correlation_id`. |
| `deliveries_count` | `BigInteger`, not null | Attempt count at the moment of failure. |
| `created_at` | `DateTime(timezone=True)`, not null | The outbox row's original `created_at` — measures time-to-terminal-failure. |
| `failed_at` | `DateTime(timezone=True)`, not null, default `now()` | When the audit row was written. |
| `failure_reason` | `String(64)`, not null | One of the three values in the table above. |
| `last_exception` | `String`, nullable | `repr()` of the raised exception, bounded at 8 KiB (see below). `None` on manual `reject()` without an exception. |

Index: `(queue, failed_at)` (btree, non-unique) — supports "show me recent
failures for queue X" queries without a sequential scan as the DLQ grows.

No foreign key references the outbox table: the source row is gone in the
same transaction, so the constraint would be unsatisfiable. There is also no
`LISTEN/NOTIFY` channel — nobody polls the DLQ.

## Atomicity

When `dlq_table` is configured, `OutboxClient.delete_with_lease` switches to
a single CTE statement:

```sql
WITH deleted AS (
    DELETE FROM <outbox> WHERE id = :id AND acquired_token = :token
    RETURNING id, queue, payload, headers, deliveries_count, created_at
)
INSERT INTO <dlq> (original_id, queue, payload, headers, deliveries_count,
                   created_at, failure_reason, last_exception)
SELECT id, queue, payload, headers, deliveries_count, created_at,
       :failure_reason, :last_exception
FROM deleted;
```

Two operator-visible properties fall out of this shape:

- **Lease-lost is a transparent no-op.** If another worker reclaimed the
  row after a lease expiry, `WHERE acquired_token = :token` matches
  nothing, `deleted` is empty, the INSERT inserts zero rows, and the
  caller sees `rowcount == 0` — same observable as the no-DLQ path. The
  lease-token guard documented in [Subscriber](./subscriber.md) is
  preserved.
- **DLQ-write failure rolls back the DELETE.** If the INSERT fails
  (column mismatch, disk full, ENUM violation), the whole statement
  rolls back. The outbox row stays leased and is reclaimed when the
  lease expires. Misconfiguration surfaces as outbox growth plus
  `lease_lost` spikes rather than silent audit loss.

The statement runs on the worker's autocommit writer connection — one
round-trip per terminal flush, same cost as the no-DLQ path.

## `last_exception` truncation

The serialized exception (`repr(exc)`) is bounded at 8 KiB by
`_LAST_EXCEPTION_MAX_CHARS` in `faststream_outbox/subscriber/usecase.py`.
Anything longer is truncated and `…[truncated]` appended.

Rationale: some exceptions carry MB-scale payloads — pydantic validation
errors with the rejected request body, asyncpg `DataError` with the full
row, etc. An unbounded `repr` would extend the writer round-trip on a
poison row by hundreds of milliseconds and bloat the DLQ table. 8 KiB
preserves the traceback and any structured detail while bounding worst
case.

## Schema validation

When `dlq_table` is set, `await broker.validate_schema()` checks both
tables and surfaces missing columns / indexes on either one. The DLQ
table is validated independently — drift in one table does not mask drift
in the other. See [Schema validation](./schema-validation.md) for the
opt-in install + `/health` pattern.

## Metric: `dlq_written`

`_flush_terminal` emits a `dlq_written` recorder event after the CTE
commits successfully. Skipped on the lease-lost path (no audit row was
written, so nothing to count).

Tags:

| Tag | Notes |
|---|---|
| `queue` | Source queue. |
| `subscriber` | Subscriber handler name (`call_name`). |
| `deliveries_count` | Attempt count at terminal flush. |
| `failure_reason` | Same value set as the schema column. |
| `exception_type` | Present only when `last_exception` was set (omitted for `max_deliveries` and manual `reject()` without an exception). |

The bundled adapters surface the event without further wiring:

- **Prometheus**: counter `faststream_outbox_dlq_written_total{reason}`.
- **OpenTelemetry**: counter `messaging.outbox.dlq_written` with the
  `messaging.outbox.dlq_reason` attribute and the standard
  `error.type` attribute when present.

Pair with `nacked_terminal` to alert on DLQ misconfiguration: every
terminal-failure row should produce one `nacked_terminal` *and* one
`dlq_written`. A persistent divergence (terminal rate > DLQ rate) means
either the CTE keeps rolling back (DLQ schema drift) or the lease keeps
expiring before flush (`lease_ttl_seconds` too low for handler P99) —
both are operator-actionable signals. See
[Observability](./observability.md) for the broader recorder + middleware
story.

## Retention

There is no built-in pruning. Operators are responsible for archival or
expiry.

Recommended pattern: partition the DLQ by `failed_at` (monthly or
weekly) and drop old partitions via a cron job. The `(queue, failed_at)`
index already supports partition pruning in operator queries; convert it
to a partitioned table at create time if you expect a steady DLQ
inflow.

For low-volume DLQs a plain `DELETE FROM <dlq> WHERE failed_at < now() -
interval '90 days'` from a daily cron is enough.

## Test broker

`TestOutboxBroker` accumulates audit rows in
`broker.fake_client.dlq_rows` so tests can assert on archive content
without a real Postgres. The fake mirrors the production CTE
side-effect: the source row is removed from `fake_client.rows` and an
audit dict is appended to `fake_client.dlq_rows` in the same call.

```python
from faststream_outbox import NoRetry, OutboxBroker, TestOutboxBroker, make_dlq_table, make_outbox_table


metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")
dlq_table = make_dlq_table(metadata, table_name="outbox_dlq")
broker = OutboxBroker(outbox_table=outbox_table, dlq_table=dlq_table)


@broker.subscriber("orders", retry_strategy=NoRetry())
async def handle(body: dict) -> None:
    raise RuntimeError("boom")


test_broker = TestOutboxBroker(broker)
async with test_broker:
    await broker.publish({"order_id": 1}, queue="orders")

assert test_broker.fake_client.rows == []
assert len(test_broker.fake_client.dlq_rows) == 1
assert test_broker.fake_client.dlq_rows[0]["failure_reason"] == "retry_terminal"
```

See [Testing](./testing.md) for the broader test-broker contract.
