# Installation

## Install `faststream-outbox`

=== "uv"

    ```bash
    uv add faststream-outbox
    ```

=== "pip"

    ```bash
    pip install faststream-outbox
    ```

=== "poetry"

    ```bash
    poetry add faststream-outbox
    ```

## Requirements

- Python 3.11+
- PostgreSQL 12+ — the features used (partial indexes, `FOR UPDATE SKIP
  LOCKED`, `make_interval`, `pg_notify`) all predate 12. The examples and
  CI run on 17; that is what's exercised, so 17 is the safest choice if
  you're starting fresh.
- A running Postgres instance accessible via SQLAlchemy `AsyncEngine`

## Free-threaded Python

`faststream-outbox` supports free-threaded (no-GIL) CPython and is tested on
**3.14t** in CI, with the GIL asserted disabled. The package is pure-Python
asyncio, so nothing in it depends on the GIL; installing on a `python3.14t`
interpreter resolves the free-threaded wheels of the compiled dependencies
(`asyncpg`, `sqlalchemy`, `pydantic-core`) automatically.

!!! note "Keep the GIL disabled: `DISABLE_SQLALCHEMY_CEXT_RUNTIME=1`"

    SQLAlchemy's Cython extensions ship free-threaded wheels but do not yet
    declare themselves free-thread-safe, so importing SQLAlchemy re-enables the
    GIL process-wide. Your outbox code still runs correctly either way, but if
    you want the GIL to stay disabled — for example because other parts of your
    process use threads for parallelism — set `DISABLE_SQLALCHEMY_CEXT_RUNTIME=1`
    (SQLAlchemy's own switch; it falls back to pure-Python implementations). This
    is what CI runs, and it is what lets the GIL stay off.

    The same caveat applies to any foreign-broker client you install for the
    [relay feature](../usage/relay.md): if it hasn't declared free-thread safety
    (for example `aiokafka`), importing it re-enables the GIL. That is the
    client library's limitation, not the outbox's — your outbox code still runs
    correctly.

What this does **not** change: the subscriber runs a single event loop by
design, so free-threading does not add cross-core parallelism within one
process. To use more cores, run more subscriber processes — the same scaling
lever as on a GIL build.

Free-threaded wheels for the compiled dependencies currently exist for 3.14t
only (there are no `cp313t` wheels), so 3.13t is not a supported target.

## Postgres

If you don't have a Postgres instance, you can start one with Docker:

```bash
docker run -d -p 5432:5432 \
    -e POSTGRES_USER=outbox \
    -e POSTGRES_PASSWORD=outbox \
    -e POSTGRES_DB=outbox \
    postgres:17
```

## Optional extras

The base install ships only `faststream` + `sqlalchemy[asyncio]` — **no
async Postgres driver**, so you must install one (the `asyncpg` extra below,
or another async driver such as `psycopg`); the `postgresql+asyncpg://` DSNs
in the examples need `asyncpg` specifically. Each optional extra unlocks one
feature; nothing else changes if you omit them.

| Extra | Install | What it enables |
|---|---|---|
| `asyncpg` | `pip install 'faststream-outbox[asyncpg]'` | The `asyncpg` SQLAlchemy driver. Required to get `LISTEN/NOTIFY` short-circuit wakeups in the subscriber's fetch loop. With a *different* async driver (e.g. `psycopg`) but no `asyncpg`, the broker still works but the loop falls back to plain polling, which adds up to `max_fetch_interval` (default 10s) of idle latency between an INSERT and a dispatch; with no async driver at all the engine can't connect. |
| `fastapi` | `pip install 'faststream-outbox[fastapi]'` | The `faststream_outbox.fastapi.OutboxRouter` — see [FastAPI integration](../usage/fastapi.md). |
| `validate` | `pip install 'faststream-outbox[validate]'` | Alembic, for `broker.validate_schema()` — see [Schema validation](../usage/schema-validation.md). Calling `validate_schema()` without this extra raises `ImportError`; every other code path works. |
| `prometheus` | `pip install 'faststream-outbox[prometheus]'` | The `PrometheusRecorder` metrics adapter and native `OutboxPrometheusMiddleware` — see [Observability](../usage/observability.md). |
| `opentelemetry` | `pip install 'faststream-outbox[opentelemetry]'` | The `OpenTelemetryRecorder` metrics adapter and native `OutboxTelemetryMiddleware` — see [Observability](../usage/observability.md). |

Combine extras with commas:

```bash
pip install 'faststream-outbox[asyncpg,fastapi,prometheus]'
```

Or use the `all` extra to pull in every optional extra at once
(`asyncpg`, `validate`, `fastapi`, `prometheus`, `opentelemetry`):

```bash
pip install 'faststream-outbox[all]'
```
