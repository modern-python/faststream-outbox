# Schema validation

The package never creates or migrates your schema — that's Alembic's job
— but it does provide an opt-in helper that verifies the live table has
everything the broker needs at runtime.

`broker.validate_schema()` delegates to Alembic's
`autogenerate.compare_metadata` against a throwaway `MetaData` populated
by `make_outbox_table(...)`. The canonical `Table` is the single source
of truth; the validator never duplicates the schema declaration. When the
broker was constructed with a `dlq_table`, `validate_schema()` runs a
second pass over the DLQ table the same way.

## Install

Alembic is an **optional dependency**:

```bash
pip install 'faststream-outbox[validate]'
```

Calling `validate_schema()` without this extra raises `ImportError`. Every
other code path (producers, subscribers, retries, timers, LISTEN/NOTIFY)
works without it.

## Usage

```python
await broker.validate_schema()
```

Raises `RuntimeError` if the live table is missing what the broker needs —
absent table, missing columns, mismatched column types, flipped
nullability, missing partial indexes.

Extras are intentionally ignored: the validator only flags **missing**
schema (`add_*` / `modify_*` ops). `remove_*` ops are silently dropped so
you can attach your own audit columns or additional indexes without the
validator complaining.

Some drift cannot be fixed by re-running `alembic revision --autogenerate` — a
missing/altered `outbox_lease_ck` CHECK or a drifted partial-index predicate.
For those, the `RuntimeError` ends with a pointer to
[Alembic migrations § Fixing drift autogenerate can't see](../operations/alembic.md#fixing-drift-autogenerate-cant-see),
which holds the hand-written migration recipe.

!!! warning "Server defaults are not checked"
    The diff runs with `compare_server_default=False` — Alembic's
    server-default comparison is flaky against Postgres' normalized
    expressions (`now()` vs `CURRENT_TIMESTAMP`), so it is disabled to avoid
    false positives. A **green** `validate_schema()` therefore does **not**
    prove your server defaults exist. The load-bearing case: a table missing
    `server_default=now()` on `next_attempt_at` leaves fresh rows with NULL
    `next_attempt_at`, which the fetch CTE's `next_attempt_at <= now()`
    predicate silently filters out — a silent broker outage that validation
    will not catch. Generate your migration from `make_outbox_table(...)` so
    the defaults are in place to begin with.

## Where to call it

Call it from a `/health` endpoint or startup hook — **not** at
`broker.start()`. The reason: if `validate_schema()` ran at startup and
your migration hadn't been applied yet, the broker would crash-loop
itself. Operators need to be able to roll out a new schema version and
have Alembic catch up against the same DB without a startup loop.

A typical pattern under FastAPI:

```python
from fastapi import FastAPI


app = FastAPI()


@app.get("/health")
async def health() -> dict:
    await broker.validate_schema()
    return {"ok": True}
```

Or as a one-shot CI check after running migrations:

```python
import asyncio

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_outbox import OutboxBroker, make_outbox_table


async def main() -> None:
    engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost/outbox")
    outbox_table = make_outbox_table(MetaData(), table_name="outbox")
    broker = OutboxBroker(engine, outbox_table=outbox_table)
    await broker.validate_schema()
    print("schema OK")


asyncio.run(main())
```

*CI recipe: [Alembic migrations § Drift detection in CI](../operations/alembic.md#drift-detection-in-ci).*

## In tests

`FakeOutboxClient.validate_schema()` raises `NotImplementedError` — there
is no real DB to validate against, and a silent pass would let users ship
broken schemas while their `TestOutboxBroker`-backed tests stay green.

Tests that need real schema validation must construct an
`OutboxClient(real_engine, table)` against the same DSN the migrations
ran against. See [Testing](./testing.md).
