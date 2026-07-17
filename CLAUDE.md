# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`faststream-outbox` is a FastStream broker integration whose transport is a Postgres table (transactional outbox pattern). Postgres-only at v0. Subscribers poll the table and use LISTEN/NOTIFY to short-circuit idle waits.

## Commands

`just` (task runner) + `uv` (package manager); the [`Justfile`](Justfile) is the source of truth for recipes — run `just --list` or read it. The non-obvious bits:

- `just test [args]` — full suite in docker compose (Postgres 17). Args forward **unquoted**, so a spaced `-k` expression (`-k "a or b"`) word-splits and fails (`file or directory not found: or`) — run one keyword per invocation, or a single substring matching all targets. `tests/test_unit.py` + `tests/test_fake.py` need no Postgres (`uv run pytest tests/test_unit.py` works directly); `tests/test_integration.py` needs Postgres at `POSTGRES_DSN` (default `postgresql+asyncpg://outbox:outbox@localhost:5432/outbox`; `pg_engine` skips if unreachable). Coverage is on with `--cov-fail-under=100` — partial runs fail that gate; pass `--no-cov` or `--cov-fail-under=0` when iterating.
- `just lint` / `just lint-ci` — autofix vs non-mutating; `lint-ci` also runs the planning-change validator.
- `just docs-serve` / `just docs-build` — local hot-reload at `http://127.0.0.1:8000` / one-shot strict `mkdocs build`.

## Workflow

Planning uses a portable convention — `architecture/` (repo root) is the living **truth home** and promotion target; `planning/changes/` holds the per-change files. Start at the [Quick path](planning/README.md#quick-path-start-here) in `planning/README.md` (the authoritative spec) to pick a lane — **Full** (design template), **Lightweight** (change template), or **Tiny** (just a commit) — and ship. `just check-planning` validates changes; `just index` prints the change + decision listing; `planning/_templates/` are copy-and-fill starting points. A design decision taken **without** a code change — especially a **rejected** option with a load-bearing reason — goes in `planning/decisions/YYYY-MM-DD-<slug>.md` (`status: accepted|superseded`) with a **Revisit trigger** so future reviews don't re-litigate it.

## Architecture

> Quick orientation + the invariants Claude must not break. Each capability's full implementation detail lives in its `architecture/<capability>.md` (the truth home); user-facing docs live in `docs/`. **When a change alters a capability's behavior, update the matching `architecture/<capability>.md` in the same PR** — that promotion is what keeps `architecture/` true.

The package wires a FastStream `Broker`/`Registrator`/`Subscriber` trio whose transport is Postgres rows, not a message bus.

**Invariants — do not break (detail in the linked capability file):**

- **Producer / transactional contract** — `broker.publish`/`publish_batch` insert through the caller's `AsyncSession` and **never flush/commit/open their own transaction**; the row commits with the caller's domain writes. Non-`AsyncSession` → `TypeError`; `broker.request` → `NotImplementedError`. → [producer.md](architecture/producer.md)
- **Relay to foreign broker** — native FastStream publisher-chain (`@kafka_pub @broker_outbox.subscriber("q")`); bad chain composition raises `_OutboxConfigError` and retries via **lease expiry**, not the retry strategy. → [relay.md](architecture/relay.md)
- **Timers** — `activate_in`/`activate_at` are mutually exclusive and gate eligibility; `timer_id` is "at most one *live* row per `(queue, timer_id)`", not a global idempotency key; `cancel_timer`'s `acquired_token IS NULL` guard is load-bearing. → [timers.md](architecture/timers.md)
- **User-owned schema** — caller owns the table; the three partial indexes + the `<table>_lease_ck` CHECK are load-bearing; **no `state` column**; `validate_schema()` is opt-in. → [schema.md](architecture/schema.md)
- **Opt-in DLQ** — with `dlq_table=None` every path is **bit-for-bit identical**; the `DLQFailureReason` `Literal` is the **public contract** (changes are API-breaking); delivery records one disjoint `Outcome` (`Ack`/`Retry`/`Terminal`) that `dispatch_one` matches on (`terminal_failure_reason`/`pending_delay_seconds`/`state_set` are read-only views). → [dlq.md](architecture/dlq.md)
- **Two-loop subscriber + lease-token invariant** — fetch loop + N worker loops; **every terminal write filters on `acquired_token`** (a stale write finds `rowcount==0` and is dropped) — any new fetch/terminal path must preserve this; `AckPolicy.ACK_FIRST` is rejected at registration; `lease_ttl_seconds` must exceed handler P99. → [subscriber.md](architecture/subscriber.md)
- **Drain on stop** — custom `stop()` overrides + the `dispatch_one` shutdown-race guard prevent row leaks on rolling deploys; **re-check both overrides when touching shutdown**. → [drain.md](architecture/drain.md)
- **Test broker** — `TestOutboxBroker` swaps in `FakeOutboxClient`; sync mode dispatches the handler before `publish` returns; `test_client_contract.py` pins real-vs-fake parity. → [test-broker.md](architecture/test-broker.md)
- **Integration** — annotations `Context` shortcuts; `OutboxRouter` bridges FastAPI deps so `OutboxResponse(session=...)` commits with the handler's writes; caller owns the `AsyncEngine` (broker never disposes it). → [integration.md](architecture/integration.md)
- **Metrics + native middleware** — two complementary seams, **don't collapse them**; each fires for events the other physically cannot observe; canonical `messaging.system` label is `"outbox"`. → [metrics.md](architecture/metrics.md)
- **Retry strategies** — `get_next_attempt_delay` returns delay-seconds or `None` (terminal); the default is `ExponentialRetry(...)`, **not** delete-on-error (`NoRetry()` to opt out); `max_total_delay_seconds` is a lower bound. → [retry.md](architecture/retry.md)

## Conventions

- Python 3.11+.
- **Never use local/inline imports.** All imports at module top — no `import` inside functions/methods/`if TYPE_CHECKING` exception aside. Tests included. If `# noqa: PLC0415` is the only way to keep an import inline, hoist it instead.
- `ruff` runs `select = ["ALL"]` with documented ignores in `pyproject.toml`; many `# noqa` are intentional.
- Type checker is `ty`. Use `# ty: ignore[<rule>]` for intentional escapes.
- Suppressions audit (PLR0913, ARG002, `invalid-method-override`, `BrokerUsecase` invariance, etc.) → `planning/lint-suppressions.md`. Consult before removing one.
