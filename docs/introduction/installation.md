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

- Python 3.13+
- PostgreSQL 12+
- A running Postgres instance accessible via SQLAlchemy `AsyncEngine`

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

The base install ships only the SQLAlchemy-driven polling broker. Each
optional extra unlocks one feature; nothing else changes if you omit them.

| Extra | Install | What it enables |
|---|---|---|
| `asyncpg` | `pip install 'faststream-outbox[asyncpg]'` | The `asyncpg` SQLAlchemy driver. Required to get `LISTEN/NOTIFY` short-circuit wakeups in the subscriber's fetch loop — without it the loop falls back to plain polling, which adds up to `max_fetch_interval` (default 10s) of idle latency between an INSERT and a dispatch. |
| `fastapi` | `pip install 'faststream-outbox[fastapi]'` | The `faststream_outbox.fastapi.OutboxRouter` — see [FastAPI integration](../usage/fastapi.md). |
| `validate` | `pip install 'faststream-outbox[validate]'` | Alembic, for `broker.validate_schema()` — see [Schema validation](../usage/schema-validation.md). Calling `validate_schema()` without this extra raises `ImportError`; every other code path works. |
| `prometheus` | `pip install 'faststream-outbox[prometheus]'` | The `PrometheusRecorder` metrics adapter and native `OutboxPrometheusMiddleware` — see [Observability](../usage/observability.md). |
| `opentelemetry` | `pip install 'faststream-outbox[opentelemetry]'` | The `OpenTelemetryRecorder` metrics adapter and native `OutboxTelemetryMiddleware` — see [Observability](../usage/observability.md). |

Combine extras with commas:

```bash
pip install 'faststream-outbox[asyncpg,fastapi,prometheus]'
```
