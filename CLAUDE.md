# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`faststream-outbox` is a FastStream broker integration that uses a Postgres table as the message queue (transactional outbox pattern). Postgres-only at v0. Subscribers poll the table and use LISTEN/NOTIFY to short-circuit idle waits.

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

`broker.publish(body, *, queue, session, headers=None, correlation_id=None, activate_in=None, activate_at=None, timer_id=None)` and `broker.publish_batch(*bodies, queue, session, headers=None, activate_in=None, activate_at=None)` insert outbox rows through the caller's `AsyncSession`. They do **not** flush, commit, or open their own transaction — the row must commit with the caller's domain writes. Both reject anything that is not an `AsyncSession` with `TypeError`. `publish` returns the inserted row's id (or `None` on `timer_id` conflict); `publish_batch` returns nothing and does not accept `timer_id` (per-row dedup makes no sense in a batch).

`broker.request` raises `NotImplementedError` (outbox is fire-and-forget).

`OutboxProducer` (in `publisher/producer.py`) is the canonical insert path — implements FastStream's `ProducerProto[OutboxPublishCommand]`. Both `broker.publish`/`publish_batch` and `OutboxPublisher.publish` build an `OutboxPublishCommand` (in `response.py`) and route through `_basic_publish(cmd, producer=self.config.producer)`, so encode + insert + NOTIFY semantics live in one place. The command's constructor enforces the session-type check + activate-args mutex + tz check; `from_cmd` raises (relay chaining is unsupported — see the publisher section below).

`broker.publisher(queue, *, headers=None, middlewares=(), title=None, description=None, schema=None, include_in_schema=True)` returns an `OutboxPublisher` — a typed, queue-scoped wrapper around `broker.publish` with the same transactional contract: `await pub.publish(body, *, session, headers=None, correlation_id=None, activate_in=None, activate_at=None, timer_id=None)`. Static headers passed to the decorator are merged with per-call headers (per-call wins). `middlewares=` wrap every `publisher.publish(...)` call. The publisher exists for AsyncAPI spec coverage and per-queue config — **not** for decorator-relay chaining. `OutboxPublisher.__call__` raises `NotImplementedError` at decoration time so `@pub @broker.subscriber(...)` fails fast with a message pointing at the manual `broker.publish(...)` pattern. Rationale: the dispatch loop has no reachable `AsyncSession` without breaking the outbox transactional contract (row commits with caller's domain writes), so a relay decorator would either silently open its own session (defeating the point) or require contextvar plumbing (over-engineered for the use case).

For chained publishing without the relay-decorator footgun, handlers can `return OutboxResponse(body=..., queue=..., session=session)`. The `OutboxResponse` (in `response.py`) is the response counterpart to `OutboxPublishCommand` and delegates validation to it via `as_publish_command()`. FastStream's `_make_response_publisher` is gated on a truthy `message.reply_to`; `OutboxParser.parse_message` sets `reply_to=msg.queue` to trip the gate. The actual response publisher is `OutboxFakePublisher` (in `publisher/fake.py`), which gates internally on `isinstance(cmd, OutboxPublishCommand)` so plain handler returns (`None`, `dict`, etc.) become silent no-ops. `correlation_id` propagates from the inbound message via FastStream's `process_message` inheritance — no extra wiring needed for trace stitching.

`_encode_payload` (in `envelope.py`) is the internal helper that turns `body` into `(payload_bytes, headers_dict)`. Used by `OutboxProducer` and the fake producer; not exported.

### Timers (delayed delivery)

`activate_in: timedelta` / `activate_at: datetime` (mutually exclusive) set `next_attempt_at` so the row is invisible to fetch until the gate opens — the `next_attempt_at <= now()` predicate in the fetch CTE is what gates eligibility, so no subscriber-side change is needed for scheduling. For `publish`, `next_attempt_at` is computed server-side via `now() + make_interval(secs => :s)` to stay clock-skew-safe; for `publish_batch` it's client-side (`datetime.now(UTC) + activate_in`) because executemany doesn't compose cleanly with column-level SQL expressions, and the few-ms drift is harmless for user-supplied scheduling.

`timer_id` (single `publish` only) flows into a `String(255)` column with a partial unique index on `(queue, timer_id) WHERE timer_id IS NOT NULL`. The producer switches to `pg_insert(...).on_conflict_do_nothing(index_elements=[queue, timer_id], index_where=timer_id IS NOT NULL)` so re-publishing the same id is a silent no-op (returns `None`). NOTIFY is skipped when `activate_in`/`activate_at` is set OR the conflict path returned no row — both cases would either wake listeners that find nothing, or wake them prematurely.

`broker.cancel_timer(*, queue, timer_id, session)` issues `DELETE WHERE queue=? AND timer_id=? AND acquired_token IS NULL` on the caller's session — the `acquired_token IS NULL` guard is load-bearing: it preserves the lease-token invariant by refusing to clobber a row whose handler is already in flight (returns `False` in that case; the delivery completes normally).

Latency floor: timer firing latency is bounded by `max_fetch_interval` (default 10s) after `next_attempt_at` elapses. NOTIFY does not help here — listeners can't act on a future row. Sub-second precision is not a goal of this broker.

### User-owned schema

`make_outbox_table(metadata, table_name="outbox")` returns a `sqlalchemy.Table` attached to the user's `MetaData`. The package never creates or migrates the table — that's Alembic's job — but it **does** declare three indexes on the table itself so Alembic autogenerate brings them up: the partial `(queue, next_attempt_at) WHERE acquired_token IS NULL` backs the fetch CTE's Branch A (unleased rows); the partial `(queue, acquired_at) WHERE acquired_token IS NOT NULL` backs Branch B (expired-lease reclaim); and the partial unique `(queue, timer_id) WHERE timer_id IS NOT NULL` enforces `timer_id` dedup. The fetch CTE's OR is written so each disjunct explicitly carries the matching partial-index predicate as a conjunct — Postgres only uses a partial index when the query implies its WHERE clause, so the naive `acquired_at < cutoff` form would not engage `_lease_idx` and would fall back to seq-scan. Both fetch-side indexes pay write amplification on every claim (the fetch UPDATE writes `acquired_token` and `acquired_at`), traded for bounded scan cost under sustained lease expiry. `validate_schema()` is **opt-in** (call from `/health` or a startup hook, not `broker.start()`) so migrations can run against the same DB without a startup loop. There is **no** `state` column: a row is "available" iff its lease is unset (`acquired_token IS NULL`) or expired (`acquired_at < now() - lease_ttl_seconds`). Terminal failures `DELETE` (no archive, no DLQ).

`validate_schema()` delegates to `alembic.autogenerate.compare_metadata` against a throwaway `MetaData` populated by `make_outbox_table(...)` — so the canonical `Table` is the single source of truth and the validator never duplicates the schema declaration. It only flags **missing** schema (`add_*` / `modify_*` ops); `remove_*` ops are intentionally ignored so users may attach extras (audit columns, their own indexes). Alembic is an **optional dependency** (`faststream-outbox[validate]`); without it, `validate_schema()` raises `ImportError`, but every other code path works (the import lives at the top of `client.py` inside a try/except, with module-level sentinels `_alembic_compare_metadata` / `_AlembicMigrationContext` set to `None` on failure).

### Two-loop subscriber (`subscriber/usecase.py`)

Per subscriber:
1. **`_fetch_loop`** — owns a long-lived `AsyncConnection` for the fetch CTE and a separate raw asyncpg connection for `LISTEN outbox_<table>`. Single CTE: `SELECT … FOR UPDATE SKIP LOCKED → UPDATE acquired_token=:uuid, acquired_at=now() RETURNING *`. The CTE's WHERE reclaims both unleased rows AND rows whose lease has expired (`acquired_at < now() - make_interval(secs => :lease_ttl)`), so there is no separate stuck-row reaper. The idle-sleep is short-circuited by NOTIFY via an `asyncio.Event` — idle dispatch latency drops from up to `max_fetch_interval` (default 10s) to ~10ms. If LISTEN setup fails (asyncpg missing, non-asyncpg driver, permission error), the loop logs once and falls back to polling. On any DB error the connections are closed, the loop backs off exponentially (capped by `_BACKOFF_EXP_CAP=30`), and reopens. Test broker (no real engine) skips the persistent-connection / LISTEN path entirely and uses `client.fetch(...)` per iteration.
2. **`_worker_loop`** × `max_workers` — pulls from an in-process `asyncio.Queue(maxsize=fetch_batch_size)`, dispatches via `consume()`, then flushes the row's terminal state. Each worker owns a long-lived `AsyncConnection` (held across the outer reconnect boundary) and routes terminal writes through `delete_with_lease_with_conn` / `mark_pending_with_lease_with_conn`, so a drain of N rows costs O(workers) pool checkouts, not O(rows). A flush exception propagates so the outer loop can close & rebuild the (presumed-poisoned) connection; the inflight slot is still released in `finally`. Default `AckPolicy.NACK_ON_ERROR`; users may pass `ack_policy=AckPolicy.REJECT_ON_ERROR` or `MANUAL`. `AckPolicy.ACK_FIRST` is **rejected at registration with `ValueError`** — it deletes before the handler runs, so a crash silently drops the row and defeats the outbox contract. The factory in `subscriber/factory.py` also raises or warns on other footgun combinations (e.g. `lease_ttl_seconds <= max_fetch_interval`, `max_deliveries` without retry, `min_fetch_interval > max_fetch_interval`).

`OutboxSubscriber.get_one()` and `__aiter__()` are explicit `NotImplementedError`s that point operators at `broker.fetch_unprocessed(session=..., queue=...)` — a peek that would acquire a lease has surprising `deliveries_count` semantics; lease-free reads belong on `broker.fetch_unprocessed`. The `__aiter__` override matches the base's no-yield signature (coroutine returning `AsyncIterator`) so the override stays Liskov-compatible without becoming an async generator.

**Connection budget**: each subscriber holds `max_workers + 1` SQLAlchemy pool connections steady-state (one writer per worker + one fetch), plus one raw asyncpg connection for LISTEN when available. Size the engine pool for `Σ subscribers × (max_workers + 1)` or startup will block on checkout. SQLAlchemy's default `pool_size=5, max_overflow=10` is enough for a handful of single-worker subscribers; bump it for larger fleets. The pool formula is **per process** — Postgres `max_connections` must cover `replicas × Σ subscribers × (max_workers + 1)`, or new replicas (or rolling deploys) hit `FATAL: too many connections` at startup.

Producer side: `broker.publish` and `publish_batch` emit `SELECT pg_notify('outbox_<table>', queue)` on the caller's session right after the INSERT, **except** when the row is future-dated (`activate_in`/`activate_at` set) or a `timer_id` conflict made the insert a no-op — both cases skip NOTIFY since listeners can't act on the result. NOTIFY is transactional: listeners only see it after the user's transaction commits, so atomicity with the row insert is automatic. Rolled-back transactions silently drop the NOTIFY.

Channel naming convention: `outbox_<table_name>`. Postgres limits identifiers to 63 chars, so users with table names longer than ~56 chars will silently lose the NOTIFY wake-up and degrade to polling.

### Lease-token invariant — load-bearing

Every terminal write (`delete_with_lease`, `mark_pending_with_lease`) filters on `acquired_token`. If a slow handler's lease expired and a newer fetch reclaimed the row with a fresh token, the slow handler's `DELETE`/`UPDATE` finds `rowcount == 0` and is silently dropped — preventing it from clobbering the new lease holder. Any new fetch/terminal path must preserve this.

`lease_ttl_seconds` (default `60.0`, per-subscriber) **must exceed the P99 handler duration with margin**, otherwise healthy in-flight handlers race their own lease expiry and trigger duplicate deliveries. The lease cutoff is computed server-side via `make_interval(secs => :lease_ttl)` to be immune to worker/DB clock skew.

**Sizing tip — slow handlers.** When a small fraction of work is much slower than the rest (e.g. an occasional 5-minute job among 100ms typicals), don't crank `lease_ttl_seconds` globally — that delays reclaim of *actually* stuck rows on the fast path. Route slow work onto its own subscriber with a tall `lease_ttl_seconds`; keep the fast subscriber's TTL tight. `lease_ttl_seconds` is already per-subscriber, so the segregation costs nothing beyond an extra `@broker.subscriber(...)` decorator and routing producers to the right queue name.

Lease-loss is logged at WARNING with `extra={"event": "lease_lost", "phase": "terminal" | "retry", "row_id": ..., "queue": ..., "deliveries_count": ...}` (in `_flush_terminal` / `_flush_retry`). Recurring `event=lease_lost` records mean `lease_ttl_seconds < handler P99` — that's the operator playbook signal. Log-pipeline aggregators (Datadog, CloudWatch, Loki) can count these records via the `event` field without parsing the message.

### Test broker

`TestOutboxBroker` (in `testing.py`) swaps in a `FakeOutboxClient` (in-memory list of `_FakeRow` dicts). Two dispatch modes:

- **Sync (default, `run_loops=False`)**: `broker.publish` synchronously routes through `OutboxSubscriber.dispatch_one` — matches the FastStream test-broker idiom (`TestKafkaBroker` / `TestRabbitBroker`). The handler runs before `publish` returns; no background loops. `broker.publish_batch`, `cancel_timer`, and `fetch_unprocessed` are also patched to operate on the fake client (the `session` argument is ignored). The broker's `producer` slot is swapped for a `FakeOutboxProducer` (in `testing.py`) so `publisher.publish()` lands rows in the same fake store via the FastStream `_basic_publish` flow — tests using `broker.publisher("q").publish(...)` work identically to `broker.publish(queue="q", ...)`. Future-dated rows (`activate_in`/`activate_at`) fire **immediately** in sync mode — sync dispatch ignores `next_attempt_at`. This trades production parity for test ergonomics: tests can assert handler effects without time travel. `next_attempt_at` is still recorded on the fake row for inspection. Use `run_loops=True` if you need scheduled delivery to actually wait.
- **Loop (`run_loops=True`)**: spins up the real `_fetch_loop` / `_worker_loop` against the fake client. Required for tests that exercise retry rescheduling, lease-expiry reclaim, fetch-loop error recovery, or scheduled delivery firing. Subscribers without registered handlers are skipped in `_fake_start` (mirrors `OutboxSubscriber.start`'s `if not self.calls: return`).

`OutboxSubscriber.dispatch_one(row)` is the public per-row dispatch entry point. The worker loop calls it; the test broker calls it directly. Caller must have already acquired the row's lease.

`FakeOutboxClient.validate_schema()` raises `NotImplementedError` — there is no real DB to validate against, and a silent pass would let users ship broken schemas while their `TestOutboxBroker`-backed tests stay green. Tests that need real schema validation must construct an `OutboxClient(real_engine, table)` against the same DSN the migrations ran against.

`TestOutboxBroker._fake_start` deliberately **skips the parent's publisher-iteration loop** (the one that calls `create_publisher_fake_subscriber`). Reason: FastStream's publisher-spy infrastructure mocks the registered handler to forward `publisher.publish()` calls — which conflicts with the outbox's real dispatch path (the fake producer already lands rows in the fake client *and* drives the real handler via `_sync_dispatch`). The required abstract `create_publisher_fake_subscriber` is therefore implemented as `raise NotImplementedError(...)` — unreachable in normal use. If you ever need FastStream's publisher mock for outbox tests, swap that override out before re-using the parent's `_fake_start`.

**Gotcha:** subscribers registered via `OutboxRouter` (then `broker.include_router(router)`) live on the router, not on `broker._subscribers`. Walk `broker.subscribers` (the property) — it iterates `[*self._subscribers, *(s for r in self.routers for s in r.subscribers)]` — when you need to introspect every subscriber.

### Annotations module (`annotations.py`)

`faststream_outbox.annotations` is the canonical home for `Annotated[..., Context(...)]` shortcuts — `OutboxMessage`, `OutboxBroker`, `OutboxProducer`, `OutboxClient`. Each shadows the underlying class via `from … import X as _X`. The producer path is `Context("broker._producer")` (via `BrokerUsecase._producer` property, which returns `self.config.producer`); the client path is `Context("broker.config.broker_config.client")` because the client lives only on the outbox-specific config layer, not as a property on the broker.

`faststream_outbox.fastapi` re-exports the same names with FastAPI-aware `Context` (from `faststream._internal.fastapi.context`) so they cooperate with `Depends(...)`.

### FastAPI router (`fastapi/router.py`)

`faststream_outbox.fastapi.OutboxRouter` subclasses FastStream's `StreamRouter` (which itself subclasses `APIRouter`). Mounting via `app.include_router(router)` auto-starts the inner `OutboxBroker` through FastAPI's lifespan machinery — user code never calls `broker.start()`.

Critical property for the outbox transactional contract: `wrap_callable_to_fastapi_compatible` (in FastStream's FastAPI internals) bridges FastAPI's dependency resolver into the FastStream consume pipeline, so `Depends(get_session)` inside a subscriber handler resolves the same way it does for HTTP endpoints. A handler can therefore receive the same `AsyncSession` it would in an HTTP route — and `OutboxResponse(session=...)` commits the follow-on row with the handler's domain writes.

`subscriber()` and `publisher()` are overridden on `OutboxRouter` to pin defaults for FastAPI-specific kwargs (`response_model=Default(None)`, etc.) that the base `StreamRouter.subscriber()` declares as keyword-only without defaults. Outbox-broker kwargs (`max_workers`, `retry_strategy`, `fetch_batch_size`, …) flow through to `self.broker.subscriber(...)` unchanged.

`apply_types` and the broker's FastStream `dependencies` are intentionally **not exposed** on `OutboxRouter.__init__`: `StreamRouter` forces `apply_types=False` (FastAPI's FastDepends takes over), and `dependencies` on the router signature means FastAPI `Depends(...)` only — the broker's FastStream `Dependant` list isn't useful in this flow.

`fastapi` is an optional dependency (`faststream-outbox[fastapi]`); users who don't import `faststream_outbox.fastapi` never need it installed.

### Engine ownership

The caller owns the `AsyncEngine`. `OutboxBrokerConfig.disconnect()` deliberately does nothing; `EngineState` is just a lazy holder so the broker can be constructed before the engine is wired (used by the test broker).

### Metrics seam (`metrics/__init__.py`)

`OutboxBroker(..., metrics_recorder=...)` accepts a `MetricsRecorder = Callable[[str, Mapping[str, Any]], None]`. The default (`_noop_recorder`) lets instrumentation sites call unconditionally. The recorder threads through `OutboxBrokerConfig.metrics_recorder` to two places: the subscriber's six emission points (`fetched`, `dispatched`, `acked`, `nacked_retried`, `nacked_terminal`, `lease_lost`) via `OutboxSubscriber._emit_metric`, and the producer's single emission point (`published`) via `OutboxProducer._emit_metric`. The producer reads the recorder from its own constructor kwarg (passed in alongside the config field) so the canonical insert path doesn't have to reach through the broker config at call time.

Every call site wraps the recorder in `try/except` and logs at DEBUG — a broken recorder never poisons the dispatch loop. The recorder is called from the event loop and **must not block**; sync `Counter.inc()` is fine, blocking HTTP/StatsD calls are not. The library does not wrap user recorders in `asyncio.to_thread` — that would destroy ordering and create per-event task explosion.

Bundled adapters in `metrics/prometheus.py` and `metrics/opentelemetry.py` are optional extras (`pip install faststream-outbox[prometheus]` / `[opentelemetry]`) — both modules guard their imports so importing `faststream_outbox` without the extras stays clean. Metric names, status values (`acked, nacked, error`), histogram buckets, and constructor argument names mirror upstream FastStream's `PrometheusMiddleware` / `TelemetryMiddleware` so users running other brokers see consistent dashboards. **The Prometheus adapter uses a different label set for consume vs publish, matching upstream verbatim: consume tags by `handler` (the subscriber); publish tags by `destination` (the queue). The canonical `messaging.system` / `broker` label value is `"outbox"` — shared by `PrometheusRecorder`, `OpenTelemetryRecorder`, and the native middleware providers below.** OTel adapter is meter-only — the callable seam can't bracket span lifecycles; spans land via the native middleware path instead.

Test broker (`testing.py`) mirrors the producer-side `published` emission in `_build_fake_publish` / `_build_fake_publish_batch` (and `FakeOutboxProducer.publish` / `.publish_batch`) so test code can assert on publish-side metrics without exercising the real producer path; the synthetic events use `duration_seconds=0.0` since the in-memory client has no real write to time.

### Native middleware integration (`opentelemetry/`, `prometheus/`)

Thin subclasses of upstream FastStream's `TelemetryMiddleware[OutboxPublishCommand]` and `PrometheusMiddleware[OutboxInnerMessage, OutboxPublishCommand]` register via `broker_middlewares=[...]`. They fire on `consume_scope` (via `OutboxSubscriber.dispatch_one → self.consume(row)`) and `publish_scope` (via `OutboxBroker.publish → _basic_publish` in `faststream/_internal/broker/pub_base.py:39-51`) — both work without modifying `dispatch_one` or `OutboxProducer`. Empirically verified.

Providers (`opentelemetry/provider.py`, `prometheus/provider.py`) set `messaging_system = "outbox"` — the canonical value shared with the recorder-seam adapters above. The OTel provider maps `row.id → messaging.message.id`, `row.queue → messaging.destination_publish.name`, `correlation_id → messaging.message.conversation_id`, `len(payload) → messaging.message.payload_size_bytes`, and `len(cmd.batch_bodies) → messaging.batch.message_count` when >1. Attribute keys are baked as string literals to avoid the deprecated `SpanAttributes` enum from upstream `opentelemetry.semconv.trace`.

**Test broker quirk**: `TestOutboxBroker._patch_broker` replaces `broker.publish` directly via `mock.patch.object`, bypassing `_basic_publish` — so middleware-registered publish metrics do **not** fire in test mode. Consume metrics still fire (`dispatch_one` walks middleware normally). The recorder-seam `published` event provides synthetic publish-side coverage in test mode via the fake producer.

**Two-seam layering**: middleware and recorder are complementary, not redundant. Middleware owns `consume_scope` / `publish_scope` (spans, durations, status, message size). Recorder owns events outside the bus: `fetched` (no `StreamMessage` exists at fetch time), `lease_lost` (after `consume_scope` exits), `nacked_terminal(reason="max_deliveries")` (fires before consume opens). Each fires for events the other physically cannot observe.

### Retry strategies (`retry.py`)

`get_next_attempt_at(...)` receives the raised `exception` so subclasses can retry only on transient errors (return `None` for terminal). `_RetryStrategyTemplate` enforces `max_attempts` and `max_total_delay_seconds`. `ExponentialRetry` has optional jitter and `max_delay_seconds`.

**Default**: a subscriber with no explicit `retry_strategy` resolves to `ExponentialRetry(initial_delay_seconds=1.0, multiplier=2.0, max_delay_seconds=300.0, max_attempts=10, jitter_factor=0.2)` (built by `_default_retry_strategy()` in `registrator.py`). Defaulting to "delete on first error" is the wrong contract for an outbox; users wanting that behavior must explicitly pass `NoRetry()`.

## Conventions

- Python 3.13+.
- **Never use local/inline imports.** All imports go at the top of the module — no `import` statements inside functions, methods, or `if TYPE_CHECKING` exception aside. This applies to test files too. If a `# noqa: PLC0415` is the only way to keep an import inline, hoist it instead.
- `ruff` is set to `select = ["ALL"]` with a documented ignore list in `pyproject.toml`; many `# noqa: XXX` comments are intentional and align with that list.
- Type checker is `ty`. Use `# ty: ignore[<rule>]` for intentional escapes (matches existing usage in `broker.py`, `registrator.py`).

### `ty: ignore` / `noqa` justifications (publisher + producer)

Audit of the suppressions added by the publisher feature. Each is intentional; before removing one, re-run `just lint` and verify the suppression is still flagged.

| Pattern | Locations | Why it stays |
|---|---|---|
| `# noqa: PLR0913` on the widest factories | `fastapi/router.py:OutboxRouter.__init__`, `fastapi/router.py:OutboxRouter.subscriber`, `router.py:OutboxRoute.__init__` | `[tool.ruff.lint.pylint] max-args = 15` in `pyproject.toml` covers the common factories. These three carry localized noqas because mirroring the underlying `APIRouter` / `SubscriberRoute` kwargs ladder pushes the count past the ceiling. |
| `# noqa: ARG002` on `connection`/`conn` params | `publisher/producer.py:OutboxProducer.connect`, several `FakeOutboxClient` / `FakeOutboxProducer` methods | The arg is part of an external protocol (FastStream `ProducerProto.connect`, `AbstractOutboxClient.fetch`, etc.). We accept it for signature compatibility but the outbox is session-driven, not connection-driven. |
| `# ty: ignore[invalid-argument-type]` on `super().__init__(config, specification)` | `publisher/usecase.py:OutboxPublisher.__init__` | Passing `OutboxPublisherSpecification` where the base `PublisherUsecase` expects `PublisherSpecification[BrokerConfig, ...]`. Generic invariance — identical pattern in `faststream-sqlbroker` and `faststream-redis-timers`. |
| `# ty: ignore[invalid-method-override]` on `.publish` | `publisher/usecase.py:OutboxPublisher.publish`, `broker.py:OutboxBroker.publish` / `publish_batch` | Overriding `PublisherProto.publish(message, /, *, correlation_id)` with outbox-specific kwargs (`session`, `activate_in`, `activate_at`, `timer_id`). The outbox contract diverges from the FastStream default — that's the whole point. |
| `# ty: ignore[invalid-argument-type]` on `patch_broker_calls(broker)` | `testing.py:_fake_start` | `BrokerUsecase[Msg, Conn, BrokerConfig]` is invariant on the config type; `OutboxBroker`'s `OutboxBrokerConfig` triggers the diagnostic. The call only iterates `broker.subscribers` — runtime-safe. |
| `# noqa: SLF001` on private-attr access | `testing.py` (`fd_config._serializer`, `sub._config.*`, `sub._worker_loop`, `sub._fetch_loop`), `broker.py:fd_config._serializer`, `__init__.py:_get_broker_registry` | Test broker and broker init reach into FastStream internals to thread serializers and start the real subscriber loops against the fake client. No public surface for these. |
| `# ty: ignore[missing-argument]` / `[invalid-argument-type]` on `broker.publish(...)` in tests | `tests/test_fake.py` | `TestOutboxBroker` patches `broker.publish` to make `session` optional in tests. `ty` sees the original signature. |
| `# pragma: no cover` on `TestOutboxBroker.create_publisher_fake_subscriber` | `testing.py` | `_fake_start` deliberately bypasses FastStream's publisher fake-subscriber loop (would mock the real handler). The abstract method must exist but is genuinely unreachable in normal flows. |
