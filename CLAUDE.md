# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`faststream-outbox` is a FastStream broker integration that uses a Postgres table as the message queue (transactional outbox pattern). Postgres-only at v0; polling-only (no LISTEN/NOTIFY).

## Commands

- `just test` — full suite in docker compose (Postgres 17). Forwards args: `just test tests/test_unit.py -k name`.
- `just lint` — `eof-fixer`, `ruff format`, `ruff check --fix`, `ty check`.
- `just lint-ci` — same checks in non-mutating mode.
- `just install` — `uv lock --upgrade && uv sync --all-extras --all-groups --frozen`.
- `just build` / `just down` / `just sh` — image build, teardown, shell into the app container.

`tests/test_unit.py` and `tests/test_fake.py` need no Postgres — runnable with `uv run pytest tests/test_unit.py` directly. `tests/test_integration.py` requires Postgres at `POSTGRES_DSN` (default `postgresql+asyncpg://outbox:outbox@localhost:5432/outbox`); the `pg_engine` fixture skips if unreachable. Coverage is on by default (`pyproject.toml` `addopts`).

## Architecture

The package wires a FastStream `Broker`/`Registrator`/`Subscriber` trio whose transport is Postgres rows, not a message bus.

### Producer side

`broker.publish(body, *, queue, session, headers=None, correlation_id=None)` and `broker.publish_batch(*bodies, queue, session, ...)` insert outbox rows through the caller's `AsyncSession` (`session.execute(insert(table).values(...))`). They do **not** flush, commit, or open their own transaction — the row must commit with the caller's domain writes. Both reject anything that is not an `AsyncSession` with `TypeError`.

`broker.request` raises `NotImplementedError` (outbox is fire-and-forget). `OutboxRegistrator.publisher` also raises. The `_NoProducer` stub exists only to satisfy FastStream's broker producer slot.

`_encode_payload` (in `envelope.py`) is the internal helper that turns `body` into `(payload_bytes, headers_dict)`. Not exported.

### User-owned schema

`make_outbox_table(metadata, table_name="outbox")` returns a `sqlalchemy.Table` attached to the user's `MetaData`. The package never creates or migrates the table — that's Alembic's job. `validate_schema()` is **opt-in** (call from `/health` or a startup hook, not `broker.start()`) so migrations can run against the same DB without a startup loop. `OutboxState` is `PENDING` / `PROCESSING` only; terminal failures `DELETE` (no archive, no DLQ).

### Three-loop subscriber (`subscriber/usecase.py`)

Per subscriber:
1. **`_fetch_loop`** — single CTE: `SELECT … FOR UPDATE SKIP LOCKED → UPDATE state='processing', acquired_token=:uuid RETURNING *`. Adaptive idle backoff with jitter (capped by `_BACKOFF_EXP_CAP=30`); separate exponential backoff on fetch errors.
2. **`_worker_loop`** × `max_workers` — pulls from an in-process `asyncio.Queue(maxsize=fetch_batch_size)`, dispatches via `consume()`, then flushes the row's terminal state. Default `AckPolicy.NACK_ON_ERROR`.
3. **`_release_stuck_loop`** — flips `processing` rows older than `release_stuck_timeout` back to `pending`, wrapped in a `pg_try_advisory_xact_lock` keyed off the table name so multiple processes don't fight.

### Lease-token invariant — load-bearing

Every terminal write (`delete_with_lease`, `mark_pending_with_lease`) filters on `acquired_token`. If a slow handler's lease expired and `release_stuck` re-claimed the row, the slow handler's `DELETE`/`UPDATE` finds `rowcount == 0` and is silently dropped — preventing it from clobbering the new lease holder. Any new fetch/terminal path must preserve this.

`release_stuck` computes its cutoff server-side via `make_interval(secs => :timeout)` to be immune to worker/DB clock skew.

### Test broker

`TestOutboxBroker` (in `testing.py`) swaps in a `FakeOutboxClient` (in-memory list of `_FakeRow` dicts) but runs the **real** `OutboxSubscriber` loops — fetch / worker / release-stuck — so tests exercise the actual delivery path. Subscribers without registered handlers are skipped in `_fake_start` (mirrors `OutboxSubscriber.start`'s `if not self.calls: return`).

### Engine ownership

The caller owns the `AsyncEngine`. `OutboxBrokerConfig.disconnect()` deliberately does nothing; `EngineState` is just a lazy holder so the broker can be constructed before the engine is wired (used by the test broker).

### Retry strategies (`retry.py`)

`get_next_attempt_at(...)` receives the raised `exception` so subclasses can retry only on transient errors (return `None` for terminal). `_RetryStrategyTemplate` enforces `max_attempts` and `max_total_delay_seconds`. `ExponentialRetry` has optional jitter and `max_delay_seconds`.

## Conventions

- Python 3.13+.
- **Never use local/inline imports.** All imports go at the top of the module — no `import` statements inside functions, methods, or `if TYPE_CHECKING` exception aside. This applies to test files too. If a `# noqa: PLC0415` is the only way to keep an import inline, hoist it instead.
- `ruff` is set to `select = ["ALL"]` with a documented ignore list in `pyproject.toml`; many `# noqa: XXX` comments are intentional and align with that list.
- Type checker is `ty`. Use `# ty: ignore[<rule>]` for intentional escapes (matches existing usage in `broker.py`, `registrator.py`).
