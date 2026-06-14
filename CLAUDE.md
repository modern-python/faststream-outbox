# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`faststream-outbox` is a FastStream broker integration whose transport is a Postgres table (transactional outbox pattern). Postgres-only at v0. Subscribers poll the table and use LISTEN/NOTIFY to short-circuit idle waits.

## Commands

- `just test` — full suite in docker compose (Postgres 17). Forwards args: `just test tests/test_unit.py -k name`.
- `just lint` — `eof-fixer`, `ruff format`, `ruff check --fix`, `ty check`. `just lint-ci` is the non-mutating variant.
- `just install` — `uv lock --upgrade && uv sync --all-extras --all-groups --frozen`.
- `just build` / `just down` / `just sh` — image build, teardown, shell into the app container.
- `just docs-serve` / `just docs-build` — serve docs locally at `http://127.0.0.1:8000` with hot-reload, or one-shot `mkdocs build --strict`. `just docs-deploy` is reserved for CI (force-pushes to `gh-pages`).

`tests/test_unit.py` and `tests/test_fake.py` need no Postgres — `uv run pytest tests/test_unit.py` works directly. `tests/test_integration.py` requires Postgres at `POSTGRES_DSN` (default `postgresql+asyncpg://outbox:outbox@localhost:5432/outbox`); `pg_engine` skips if unreachable. Coverage is on by default with `--cov-fail-under=100` — partial runs fail that gate; pass `--no-cov` or `--cov-fail-under=0` when iterating locally.

## Workflow

Per-feature: brainstorming → spec in `planning/changes/active/YYYY-MM-DD.NN-<slug>/design.md` → writing-plans → plan in `planning/changes/active/YYYY-MM-DD.NN-<slug>/plan.md` → executing-plans / subagent-driven-development → requesting-code-review → finishing-a-development-branch. Each change is a folder bundle; `<slug>` is a kebab-case description, not a story ID; `.NN` is a zero-padded intra-day counter that breaks same-date ties so the timeline sorts stably. On merge, the bundle moves to `planning/changes/archive/` with `status: shipped`, `pr:`, and `outcome:` filled, **and the change promotes its conclusions into the affected `architecture/<capability>.md`** — that hand-edit is what keeps `architecture/` true. See [`planning/README.md`](planning/README.md) for the conventions + index and [`planning/_templates/`](planning/_templates/) for copy-and-fill starting points.

**Spec** (`design.md`) captures the *thinking* — why we are doing this, what the design is, what trade-offs were considered, what is out of scope. Written before code; rarely revised after merge. **Plan** (`plan.md`) captures the *sequencing* — the ordered checklist of tasks an executor (human or agent) walks. References the spec for the "why"; never re-explains it. **`architecture/`** captures the *invariants* of shipped systems — the living truth, promoted from a change on merge. A plan paragraph that would still read correctly with all task numbers and checkboxes removed is design content and belongs in the spec.

**Three lanes.** Scale the artifact to the change. **Full** — a `design.md` + `plan.md` bundle — for real design judgment, a new file/module, a public-API change, cross-cutting/multi-file work, or non-trivial test design. **Lightweight** — a single `change.md` — for small-but-real changes (≲30 LOC net, ≤2 files, no new file, no public-API change, a single straightforward test). **Tiny** — no bundle, just a conventional commit — for a typo fix, dep bump, linter/formatter/CI tweak, a mechanical rename to satisfy a just-landed convention, or a single-line config change. Heavier lane wins on ambiguity; a `change.md` that outgrows its lane splits into `design.md` + `plan.md`.

## Architecture

The package wires a FastStream `Broker`/`Registrator`/`Subscriber` trio whose transport is Postgres rows, not a message bus.

Deep-dives live in `architecture/`; this file holds the invariants Claude must not break, plus pointers.

### Producer side

`broker.publish(body, *, queue, session, headers=None, correlation_id=None, activate_in=None, activate_at=None, timer_id=None)` and `broker.publish_batch(*bodies, queue, session, headers=None, activate_in=None, activate_at=None)` insert outbox rows through the caller's `AsyncSession`. **They do not flush, commit, or open their own transaction** — the row must commit with the caller's domain writes. Both reject non-`AsyncSession` with `TypeError`. `publish` returns the row id (or `None` on `timer_id` conflict); `publish_batch` returns nothing and rejects `timer_id` (per-row dedup is meaningless in a batch). `broker.request` raises `NotImplementedError` (outbox is fire-and-forget).

`OutboxProducer` (`publisher/producer.py`) implements `ProducerProto[OutboxPublishCommand]` and is the canonical insert path. `broker.publish` / `publish_batch` / `OutboxPublisher.publish` all build an `OutboxPublishCommand` (`response.py`) and route through `_basic_publish(cmd, producer=self.config.producer)` — encode + insert + NOTIFY semantics live in one place. Session-type / queue / activate-args-mutex / tz validation lives in one shared `_validate_publish_args` (`response.py`), called by the `OutboxPublishCommand` constructor, `OutboxResponse.__init__`, and `broker.publish_batch`'s empty-batch branch — so every real publish entry point (including an empty batch) rejects the same misconfigurations identically and eagerly (order: activate-args → session → queue). `from_cmd` raises (relay chaining is unsupported here).

`broker.publisher(queue, *, headers=None, title=None, description=None, schema=None, include_in_schema=True)` returns an `OutboxPublisher` — a typed wrapper around `broker.publish` with the same transactional contract. Static decorator headers merge with per-call (per-call wins). The publisher exists for AsyncAPI / per-queue config — **not** decorator-relay chaining: `OutboxPublisher.__call__` raises `NotImplementedError` at decoration time. A relay decorator can't reach an `AsyncSession` without breaking the transactional contract.

For chained publishing, handlers can `return OutboxResponse(body=..., queue=..., session=session)`. `OutboxResponse.__init__` validates eagerly via the shared `_validate_publish_args` (so a misconfigured response raises at the `return` site, not at dispatch where it would masquerade as a handler failure); `as_publish_command()` re-runs the same validator, keeping `OutboxPublishCommand` the authoritative source. FastStream gates `_make_response_publisher` on truthy `message.reply_to`; `OutboxParser.parse_message` sets `reply_to=msg.queue` to trip it. The actual publisher is `OutboxFakePublisher` (`publisher/fake.py`), which gates on `isinstance(cmd, OutboxPublishCommand)` so plain returns (`None`, `dict`, …) become silent no-ops. `correlation_id` propagates via FastStream's `process_message` inheritance.

`_encode_payload` (`envelope.py`) is the internal helper that turns `body` into `(payload_bytes, headers_dict)`. Used by both producers; not exported.

### Relay to foreign broker

`OutboxSubscriber` can source a FastStream-native cross-broker chain: `@kafka_pub @broker_outbox.subscriber("q")` (Kafka/Rabbit/NATS/Redis/Confluent). Upstream's `SubscriberUsecase.process_message` walks the publisher chain — no dispatch override is needed for the chain itself. Three guardrails on top:

- **Bad chain composition is refused.** `OutboxResponse(...)` + a non-`OutboxFakePublisher` in the chain raises `_OutboxConfigError` (private `RuntimeError` subclass) via `process_message` / `consume()` / `dispatch_one` overrides; the worker loop catches it, logs it at ERROR, and leaves the row — the lease expires and another fetch reclaims it (retry via lease expiry, **not** the `retry_strategy`) until the config is fixed (P18).
- **WARNING for unstarted foreign brokers at `start()`** — one per broker, deduped via `_warned_foreign_config_ids: set[int]`.
- **`propagate_inbound_headers: bool = False`** — when True, inbound headers fill `Response.headers` only if the handler returned a `Response` with empty headers (user-set wins). Default False matches FastStream convention.

Deep dive: `architecture/relay.md`. User-facing: `docs/usage/relay.md`.

### Timers (delayed delivery)

`activate_in: timedelta` / `activate_at: datetime` (mutually exclusive) set `next_attempt_at`; the fetch CTE's `next_attempt_at <= now()` gates eligibility. `publish` computes server-side via `make_interval` (clock-skew-safe); `publish_batch` is client-side.

`timer_id` (single `publish` only) → partial unique index `(queue, timer_id) WHERE timer_id IS NOT NULL`. Producer uses `pg_insert(...).on_conflict_do_nothing(...)` — re-publishing the same id is a no-op (returns `None`). NOTIFY is skipped when future-dated OR the conflict suppressed the insert.

`broker.cancel_timer(*, queue, timer_id, session)` issues `DELETE WHERE queue=? AND timer_id=? AND acquired_token IS NULL` — **the `acquired_token IS NULL` guard is load-bearing** (preserves the lease-token invariant; returns `False` if a handler is in flight).

Deep dive: `architecture/timers.md`. User-facing: `docs/usage/timers.md`.

### User-owned schema

`make_outbox_table(metadata, table_name="outbox")` returns a `sqlalchemy.Table` on the user's `MetaData`. The package never creates or migrates — that's Alembic — but declares three partial indexes so autogenerate brings them up:

- `(queue, next_attempt_at) WHERE acquired_token IS NULL` — fetch CTE Branch A (unleased).
- `(queue, acquired_at) WHERE acquired_token IS NOT NULL` — fetch CTE Branch B (expired-lease reclaim).
- unique `(queue, timer_id) WHERE timer_id IS NOT NULL` — `timer_id` dedup.

Plus a `CHECK ((acquired_token IS NULL) = (acquired_at IS NULL))` (the `<table>_lease_ck` constraint) so a half-set lease is unrepresentable.

The fetch CTE's OR is written so each disjunct **explicitly carries its partial-index predicate as a conjunct** — Postgres only uses a partial index when the query implies its WHERE clause; the naive form falls back to seq-scan. Both fetch indexes pay write amplification on every claim. The index also satisfies the `ORDER BY next_attempt_at, id` **only for a single-queue subscriber** — a subscriber serving multiple queues (`queue = ANY(:queues)`), or the expired-lease branch (ordered by `next_attempt_at` while `_lease_idx` is keyed on `acquired_at`), adds a `LIMIT`-bounded sort node. Prefer one subscriber per queue when fetch ordering cost matters (same segregation pattern as lease TTLs).

There is **no `state` column**: a row is "available" iff `acquired_token IS NULL` or `acquired_at < now() - lease_ttl_seconds`. Terminal failures `DELETE` by default; opt in to audit via `dlq_table=make_dlq_table(metadata)`.

`validate_schema()` is **opt-in** (call from `/health` or startup hook, not `broker.start()`) so migrations can run against the same DB without a loop. Beyond the alembic column/index diff it also probes the live partial-index **predicates** (alembic ignores `postgresql_where`), catching a drifted or non-partial `timer_id_uq` that would otherwise break `ON CONFLICT` at publish time (S2), **and probes `pg_constraint` for the `<table>_lease_ck` CHECK** (alembic has no check-constraint comparator), catching a missing or drifted lease pairing. Alembic is optional (`faststream-outbox[validate]`); without it `validate_schema()` raises `ImportError` but every other path works.

### Opt-in DLQ on terminal failure

`make_dlq_table(metadata, table_name="outbox_dlq")` + `OutboxBroker(..., dlq_table=...)` archives terminal failures. With `dlq_table=None` every existing code path is **bit-for-bit identical**.

Atomicity: `delete_with_lease` switches to a single CTE `WITH deleted AS (DELETE … RETURNING …) INSERT INTO <dlq> SELECT …` — preserves writer-connection autocommit + lease-token guard. INSERT failure rolls back DELETE, so DLQ misconfiguration surfaces as outbox growth + `lease_lost` spikes, not silent audit loss.

`OutboxInnerMessage.terminal_failure_reason` is set on three paths: `allow_delivery` False → `"max_deliveries"`, `_nack` exhausted → `"retry_terminal"`, `_reject` → `"rejected"`. **Branch on `terminal_failure_reason` BEFORE `last_exception`** in `dispatch_one` so manual `await msg.reject()` (no exception raised) routes to `nacked_terminal(reason="rejected")`, not `acked`.

**The `DLQFailureReason` `Literal` (`message.py`) is the public contract** — operator queries / dashboards key off these values; changes are API-breaking.

`last_exception` is `repr()` bounded by `_LAST_EXCEPTION_MAX_CHARS=8192` (`subscriber/usecase.py`); truncation appends `…[truncated]`. DLQ `failure_reason` is `String(64)`. No built-in retention.

Deep dive: `architecture/dlq.md`. User-facing: `docs/usage/dlq.md`.

### Two-loop subscriber (`subscriber/usecase.py`)

Per subscriber:

1. **`_fetch_loop`** — long-lived `AsyncConnection` for the fetch CTE + separate raw asyncpg connection for `LISTEN outbox_<table>`. Single CTE: `SELECT … FOR UPDATE SKIP LOCKED → UPDATE acquired_token=:uuid, acquired_at=now() RETURNING *`. WHERE reclaims unleased rows **and** expired leases (`acquired_at < now() - make_interval(secs => :lease_ttl)`) — no separate reaper. NOTIFY shortcircuits idle sleep via `asyncio.Event` (idle latency from `max_fetch_interval` to ~10ms). LISTEN failures log once and fall back to polling. DB error → connections close, exponential backoff (`_BACKOFF_EXP_CAP=30`), reopen.
2. **`_worker_loop`** × `max_workers` — pulls from `asyncio.Queue(maxsize=fetch_batch_size)`, dispatches via `consume()`, flushes terminal state. Each worker owns a long-lived `AsyncConnection` (held across reconnect) and routes terminal writes through `delete_with_lease(conn, …)` / `mark_pending_with_lease(conn, …)` — drain of N rows costs O(workers) pool checkouts. Flush exceptions propagate (outer loop rebuilds the connection); inflight slot still releases in `finally`. Default `AckPolicy.NACK_ON_ERROR`; `REJECT_ON_ERROR` and `MANUAL` allowed. **`AckPolicy.ACK_FIRST` is rejected at registration with `ValueError`** — it would delete before the handler runs, defeating the outbox contract. `subscriber/factory.py` raises or warns on other footguns (`lease_ttl_seconds <= max_fetch_interval`, `max_deliveries` without retry, etc.).

`OutboxSubscriber.get_one()` and `__aiter__()` are explicit `NotImplementedError`s — point operators at `broker.fetch_unprocessed(session=..., queue=...)`. A peek that acquires a lease has surprising `deliveries_count` semantics; lease-free reads belong on `fetch_unprocessed`.

**Connection budget.** Each subscriber holds `max_workers + 1` SQLAlchemy pool connections steady-state + one raw asyncpg connection for LISTEN. Size the pool for `Σ subscribers × (max_workers + 1)` or startup blocks on checkout — the asyncpg LISTEN connection lives **outside** the pool, so it does not count toward pool sizing. **Per process** — Postgres `max_connections` must cover `replicas × Σ subscribers × (max_workers + 2)`: the `max_workers + 1` pool connections **plus** the out-of-pool asyncpg LISTEN connection. Undersize it and rolling deploys hit `FATAL: too many connections`.

**NOTIFY semantics.** `broker.publish` / `publish_batch` emit `SELECT pg_notify('outbox_<table>', queue)` on the caller's session right after the INSERT, **except** when future-dated or `timer_id` conflict no-op'd the insert. NOTIFY is transactional — atomicity with the row is automatic; rolled-back transactions silently drop it. Channel naming is `outbox_<table_name>`. Postgres limits identifiers to 63 bytes; `make_outbox_table` **raises `ValueError`** when the longest derived identifier — an index name like `<table>_pending_idx`, longer than the NOTIFY channel itself — would exceed it — so over-long table names (~>51 bytes) are rejected at construction, not silently degraded to polling.

### Lease-token invariant — load-bearing

Every terminal write (`delete_with_lease`, `mark_pending_with_lease`) filters on `acquired_token`. If a slow handler's lease expired and a newer fetch reclaimed the row, the slow handler's `DELETE`/`UPDATE` finds `rowcount == 0` and is silently dropped — preventing it from clobbering the new lease holder. **Any new fetch/terminal path must preserve this.**

`lease_ttl_seconds` (default `60.0`, per-subscriber) **must exceed handler P99 with margin** or healthy handlers race their own expiry. The lease cutoff uses server-side `make_interval(secs => :lease_ttl)` — immune to clock skew. **Sizing tip**: route occasional slow work onto its own subscriber with a tall TTL; keep the fast subscriber's tight. TTL is per-subscriber, so segregation costs only an extra `@broker.subscriber(...)`.

Lease-loss logs at WARNING with `extra={"event": "lease_lost", "phase": "terminal"|"retry", "row_id": …, "queue": …, "deliveries_count": …}`. Recurring `event=lease_lost` means `lease_ttl_seconds < handler P99`.

**Writer-connection autocommit.** `_open_worker_resources` sets the per-worker writer to `isolation_level="AUTOCOMMIT"`. Terminal writes are single statements; explicit BEGIN/COMMIT would add two round-trips per row. The `WHERE acquired_token = …` clause enforces the invariant, not the transaction wrapping. The fetch connection is **not** autocommit — it owns LISTEN/NOTIFY and amortizes BEGIN/COMMIT across the batch.

**Shutdown race in `dispatch_one`.** If `stop()` flips `running=False` between a worker pulling a row from `_inflight` and entering `consume()`, base `SubscriberUsecase.consume()` early-exits without running the handler. `dispatch_one` detects this (`not row.state_set and not self.running` after `consume()` returns without raising) and returns before `assert_state_set → reject() → _safe_flush` would silently DELETE. Lease lives until expiry; another replica reclaims. No metric fires. **Without this guard, busy subscribers leak rows on every rolling deploy.**

### Drain on stop (subscriber + broker)

Both `OutboxSubscriber.stop()` and `OutboxBroker.stop()` override FastStream parents. Override comments carry `# Upstream equivalent (replaced): …`.

- **Subscriber: two flags during drain.** `self.running` (FastStream's "actively dispatching") stays True for the duration of drain; `self._stopping` (new) signals "no new claims". `_fetch_inner` checks both; the worker loop only `running`. `stop()` flips `_stopping`, kicks `_notify_event`, waits up to `graceful_timeout` for `_inflight.join()`, then flips `running=False` and cancels tasks. `graceful_timeout=None` (unbounded for `ping()`) is **clamped to a finite fallback in the drain** so one wedged handler can't hang `stop()` forever. `super().stop()` is **not** called — its `MultiLock.wait_release` would re-wait stuck handlers for another full budget (2× shutdown regression).
- **Broker: parallel-gather subscriber stop** via `asyncio.gather(..., return_exceptions=True)` — sequential N × `graceful_timeout` exceeds K8s default `terminationGracePeriodSeconds=30s` once a service has 2+ subscribers. Exceptions logged via `_log_subscriber_stop_error`, never re-raised.
- **Phase interaction.** During drain `running` stays True so the `dispatch_one` guard is dormant; after drain `running=False` is set before `task.cancel()` so workers mid-`dispatch_one` benefit from the guard. The two changes are complementary.
- **Upstream divergence flag.** If FastStream adds cleanup to `BrokerUsecase.stop`, `SubscriberUsecase.stop`, or `TasksMixin.stop`, we silently miss it. **Re-check both overrides when touching shutdown.** Regression tests in `tests/test_fake.py` (`test_drain_finishes_inflight_rows_before_returning_in_fake_mode`, `test_broker_stop_cancels_wedged_handler_within_graceful_timeout_in_fake_mode`) and the Postgres-backed `tests/test_integration.py`.
- **Test-broker gotcha.** `_fake_close` sets `sub.running = False` and bypasses `subscriber.stop()` / `broker.stop()` entirely — drain tests must `await broker.stop()` explicitly inside the `async with` block.

Deep dive: `architecture/drain.md`.

### Test broker

`TestOutboxBroker` (`testing.py`) swaps in `FakeOutboxClient` (in-memory `_FakeRow` dicts). Two modes:

- **Sync (`run_loops=False`, default)** — `broker.publish` routes through `OutboxSubscriber.dispatch_one` synchronously; handler runs before `publish` returns. `producer` slot is swapped for `FakeOutboxProducer` so `broker.publisher("q").publish(...)` lands in the same store. Future-dated rows **fire immediately** in sync mode (sync dispatch ignores `next_attempt_at`).
- **Loop (`run_loops=True`)** — real `_fetch_loop` / `_worker_loop` against the fake client. Needed for retry rescheduling, lease-expiry reclaim, scheduled delivery firing.

`OutboxSubscriber.dispatch_one(row)` is the public per-row entry point — worker loop and test broker both call it. Caller must hold the row's lease.

`FakeOutboxClient.validate_schema()` raises `NotImplementedError` — a silent pass would let users ship broken schemas while tests stay green. Use a real `OutboxClient(real_engine, table)` for schema validation tests.

**Gotcha:** subscribers registered via `OutboxRouter` (then `broker.include_router(router)`) live on the router, not `broker._subscribers`. Walk `broker.subscribers` (the property) for full introspection.

Deep dive: `architecture/test-broker.md`. User-facing: `docs/usage/testing.md`.

### Annotations module (`annotations.py`)

Canonical home for `Annotated[..., Context(...)]` shortcuts — `OutboxMessage`, `OutboxBroker`, `OutboxProducer`, `OutboxClient`. Each shadows the underlying class via `from … import X as _X`. Producer path: `Context("broker._producer")` (via `BrokerUsecase._producer` property → `self.config.producer`). Client path: `Context("broker.config.broker_config.client")` (client lives only on the outbox-specific config layer). `faststream_outbox.fastapi` re-exports with FastAPI-aware `Context` (from `faststream._internal.fastapi.context`).

### FastAPI router (`fastapi/router.py`)

`OutboxRouter` subclasses FastStream's `StreamRouter` (which subclasses `APIRouter`). `app.include_router(router)` auto-starts the inner `OutboxBroker` via FastAPI lifespan.

Critical for the transactional contract: `wrap_callable_to_fastapi_compatible` (FastStream internals) bridges FastAPI's dependency resolver into the consume pipeline, so `Depends(get_session)` inside a handler resolves the same `AsyncSession` it would in an HTTP route — and `OutboxResponse(session=...)` commits the follow-on row with the handler's domain writes.

`subscriber()` and `publisher()` are overridden to pin defaults for FastAPI-specific kwargs (`response_model=Default(None)`, etc.) that the base declares keyword-only without defaults. Outbox kwargs flow through unchanged. `apply_types` and broker `dependencies` are intentionally **not exposed**: `StreamRouter` forces `apply_types=False` (FastDepends takes over), and the broker's `Dependant` list isn't useful in this flow.

`fastapi` is an optional dependency (`faststream-outbox[fastapi]`).

### Engine ownership

Caller owns the `AsyncEngine` — the broker never disposes it. The engine lives on `OutboxBrokerConfig` (set by the broker constructor) and may be `None` until wired, so the broker can be constructed before the engine exists (used by the test broker).

### Metrics + native middleware

Two complementary seams — **don't collapse them.**

- **Recorder seam** (`OutboxBroker(..., metrics_recorder=...)`): `Callable[[str, Mapping[str, Any]], None]`. Subscriber emits `fetched`, `dispatched`, `acked`, `nacked_retried`, `nacked_terminal`, `lease_lost`, plus `dlq_written` when `dlq_table` is set. Producer emits `published`. Default `_noop_recorder` lets sites fire unconditionally. Every call site is wrapped in `try/except` + DEBUG log. **Recorder must not block** (sync `Counter.inc()` fine; HTTP/StatsD not). `dlq_written` vs `nacked_terminal` divergence detects DLQ misconfiguration.
- **Native middleware** (`opentelemetry/`, `prometheus/`): thin subclasses of upstream's `TelemetryMiddleware[OutboxPublishCommand]` and `PrometheusMiddleware[OutboxInnerMessage, OutboxPublishCommand]`. Register via the public `OutboxBroker(..., middlewares=[...])` constructor kwarg (forwarded internally as `broker_middlewares`). Fire on `consume_scope` (via `dispatch_one → self.consume(row)`) and `publish_scope` (via `_basic_publish`).

Why two: middleware owns `consume_scope` / `publish_scope` (spans, durations, status, size). Recorder owns events **outside** the bus — `fetched` (no `StreamMessage` at fetch time), `lease_lost` (after `consume_scope` exits), `nacked_terminal(reason="max_deliveries")` (before consume opens). Each fires for events the other physically cannot observe.

Bundled adapters are optional extras (`[prometheus]` / `[opentelemetry]`). Canonical `messaging.system` / `broker` label is `"outbox"` (shared by both seams). Prometheus tags consume by `handler`, publish by `destination` (mirrors upstream). OTel adapter is meter-only; spans go via native middleware.

Deep dive: `architecture/metrics.md`. User-facing: `docs/usage/observability.md`.

### Retry strategies (`retry.py`)

`get_next_attempt_delay(*, first_attempt_at, last_attempt_at, attempts_count, exception=None)` returns the **delay in seconds** before the next attempt (the DB computes `next_attempt_at` from it server-side, so timing is skew-immune), or `None` for terminal failure. It receives the raised exception so subclasses can retry only on transient errors. `_RetryStrategyTemplate` enforces `max_attempts` and `max_total_delay_seconds`. `ExponentialRetry` has optional jitter and `max_delay_seconds`.

**Default**: a subscriber with no explicit `retry_strategy` resolves to `ExponentialRetry(initial_delay_seconds=1.0, multiplier=2.0, max_delay_seconds=300.0, max_attempts=10, jitter_factor=0.2)` (`_default_retry_strategy()` in `registrator.py`). "Delete on first error" is the wrong default for an outbox; opt in with `NoRetry()`.

## Conventions

- Python 3.13+.
- **Never use local/inline imports.** All imports at module top — no `import` inside functions/methods/`if TYPE_CHECKING` exception aside. Tests included. If `# noqa: PLC0415` is the only way to keep an import inline, hoist it instead.
- `ruff` runs `select = ["ALL"]` with documented ignores in `pyproject.toml`; many `# noqa` are intentional.
- Type checker is `ty`. Use `# ty: ignore[<rule>]` for intentional escapes.
- Suppressions audit (PLR0913, ARG002, `invalid-method-override`, `BrokerUsecase` invariance, etc.) → `planning/lint-suppressions.md`. Consult before removing one.
