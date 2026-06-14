# Deep Audit — faststream-outbox (2026-06-14)

> Multi-agent audit: 7 parallel finders (concurrency, data-layer, security, API/UX,
> semantics, refactor, tests) → adversarial verification (default-refute) per finding
> → synthesis. 31 agents, ~1.46M tokens. 23 raw findings, **12 confirmed**, 11 refuted.
> Scoped to go deeper than the closed 2026-06-12 sweep; the known FastAPI-router
> forwarding gap (`planning/deferred.md`) was excluded by design.

## Executive summary

This audit went beyond the closed 2026-06-12 sweep and found **no critical or high-severity
code defects** in the shipped runtime paths — the load-bearing invariants (lease-token
filtering, no-flush publish, AUTOCOMMIT writer, drain budgeting) hold up under scrutiny. Of
12 verified findings, the distribution after adversarial severity adjustment is: **0 Critical,
0 High, 2 Medium, 10 Low**. Both Medium findings are **test-coverage gaps**, not bugs: the two
most consequential interleavings (lease expiry mid-flight; the relay `_OutboxConfigError`
worker-loop catch path) are protected only by unit-level mocks, never driven end-to-end through
the live worker loop. The single most important item is the **lease-expiry-during-in-flight-handler
test gap** — the outbox's subtlest failure mode (a slow handler's lease stolen, its terminal
write racing the new holder) is exercised only with synthetic wrong-tokens, so a regression in
how the worker threads `acquired_token` from fetch into the terminal write would pass every test
yet cause double-processing in production. The remaining Lows are real but narrow: a
`graceful_timeout=None` drain footgun, a `validate_schema()` blind spot for the `_lease_ck` CHECK
constraint (plus a flatly false docstring), and a cluster of cosmetic API/consistency/comment nits.

## Findings by severity

### Critical
None.

### High
None.

### [MEDIUM][test] No end-to-end test of lease expiry DURING an in-flight handler

> **RESOLVED (2026-06-14)** — `tests/test_integration.py::test_lease_expiry_during_inflight_handler_redelivers_without_clobber` races a real running handler past its `lease_ttl_seconds` through the live `_fetch_loop`/`_worker_loop`, asserts exactly one `acked` (new holder) and a `lease_lost(phase=terminal)` (stale holder dropped), and the row deleted exactly once.

`tests/test_integration.py` (lease tests at 159-262, 297-315; duplicate-delivery assert at 1045) — production token threading at `faststream_outbox/subscriber/usecase.py:674-747`.

**Problem.** The load-bearing lease-token invariant is tested only as isolated units (`delete_with_lease`/`mark_pending_with_lease` fed a synthetic `uuid.uuid4()`) and lease reclaim only by backdating `acquired_at` on an *idle* row with no handler running. No test drives the full live interleaving: a real running handler outlives `lease_ttl_seconds`, a second fetch reclaims + redelivers the same row mid-flight, and the test asserts the slow handler's terminal DELETE is correctly dropped (rowcount 0 / `lease_lost` fires) so the new holder is not clobbered. The one duplicate-delivery assertion lives in the 8-worker drain where all handlers finish in microseconds and no lease ever expires.

**Impact.** The most consequential outbox failure mode is protected only by unit mocks. A regression in how the worker loop threads `acquired_token` from fetch into the terminal write (stale-token caching, token reuse) would pass every test yet cause double-processing or clobbered leases on production rolling deploys. (Verifier adjusted High→Medium: the SQL-level no-op, CTE atomicity, and `lease_lost` logging *are* covered against real Postgres, and the token is a plain attribute pass-through, so the named regression vectors are narrow — this is a coverage-completeness gap, not an unprotected invariant.)

**Fix.** Add an integration test with small `lease_ttl_seconds` (~0.3s) and `max_workers>=2`: a handler gated on an `asyncio.Event` that sleeps past the TTL; publish one row; let worker A claim it; wait past TTL so a second fetch reclaims+redelivers; release both; assert the row is processed exactly once (new holder wins), exactly one terminal DELETE lands, and `lease_lost(phase=terminal)` fired for the stale holder.

### [MEDIUM][test] Relay-chain guardrails only tested in fake-sync mode, never through the real worker loop

> **RESOLVED (2026-06-14)** — `tests/test_integration.py::test_relay_dual_fire_guard_through_worker_loop_leaves_row_and_logs` drives a foreign-publisher-decorated subscriber returning `OutboxResponse` through the real worker loop against live Postgres, asserting the `_worker_inner` catch path: foreign publish never fires, the row is left in place (not deleted), and the `_OutboxConfigError` is logged at ERROR.

`tests/test_relay.py:208-258` — production paths at `faststream_outbox/subscriber/usecase.py:588-595` (dispatch_one re-raises) vs `529-539` (`_worker_inner` catches, logs ERROR, leaves row for lease-expiry).

**Problem.** Two production paths handle `_OutboxConfigError` from the dual-fire guard with *different* contracts. Every relay-chain test with a real foreign-publisher chain runs against `TestOutboxBroker(run_loops=False)`, where the handler executes synchronously inside `broker.publish` and the error raises out of `publish()` (the dispatch_one path). The worker-loop catch path — which swallows the error, logs ERROR, and relies on lease expiry — is only unit-tested with a hand-fed `_OutboxConfigError` (`test_worker_inner_swallows_config_error_without_reconnect` via `patch.object(sub, 'dispatch_one', ...)`), never end-to-end with a real chain in loop mode. The integration relay test covers foreign-publish *failure* (a generic RuntimeError on the retry path), a different code path.

**Impact.** A regression in how `dispatch_one` re-raises vs how `_worker_inner` swallows `_OutboxConfigError` (or a change in `process_message` chain-walking) could break one contract while the other stays green, leaving a misconfigured relay either crash-looping or silently dropping rows.

**Fix.** Add a loop-mode/integration test: register a foreign-publisher-decorated subscriber returning `OutboxResponse`, publish a row, run the real worker loop, and assert the row is NOT deleted (left for lease expiry), an ERROR is logged, and the foreign publish never fired.

### Low

### [LOW][design] `graceful_timeout=None` makes drain wait forever on a wedged handler

`faststream_outbox/subscriber/usecase.py:244-245`; accepted at `broker.py:141` and `fastapi/router.py:79`, forwarded unchanged.

**Problem.** `OutboxBroker`/`OutboxRouter` accept `graceful_timeout: float | None = 15.0` and forward `None` straight through. In subscriber `stop()`, `with anyio.move_on_after(self._outer_config.graceful_timeout): await self._inflight.join()` — `anyio.move_on_after(None)` produces an *unbounded* scope (deadline `inf`). With `graceful_timeout=None` and a wedged handler, `_inflight.join()` never returns, so `running` is never flipped to False, tasks are never cancelled, and `stop()` (hence `broker.stop()`) hangs. This silently defeats the documented "strict-bound drain" contract (architecture/drain.md:15-17).

**Impact.** An operator who sets `graceful_timeout=None` intending "wait for a clean drain" turns a single wedged handler into a pod that never terminates (until K8s SIGKILL; under bare-process/systemd with no external kill, `stop()` truly never returns). Latent API footgun: the type annotation invites `None`. Default path (15.0) is unaffected.

**Fix.** Either reject `None` at the broker/subscriber boundary for the drain path, or fall back to a finite default (e.g. 15.0) inside `stop()` while keeping `None` valid for `ping()`'s unbounded semantics. At minimum, document that `None` means "block forever on a stuck handler."

### [LOW][bug] `validate_schema()` cannot detect a missing/wrong `_lease_ck` CHECK constraint; docstring falsely claims the table declares no CHECK

`faststream_outbox/client.py:587-621` (docstring at 597-599); constraint declared at `schema.py:92-95`.

**Problem.** `validate_schema()` never verifies the load-bearing `<table>_lease_ck` CHECK `(acquired_token IS NULL) = (acquired_at IS NULL)`. Alembic's `compare_metadata` registers no check-constraint comparator (verified against installed alembic 1.18.4: only indexes/uniques, FKs, nullability), so `_run_validate` emits no op for a missing/wrong CHECK; `_drift_entry_to_error` has no `add_constraint` branch anyway; and `_validate_index_predicates_sync` probes only the three partial *indexes*. So a DB lacking (or with a disabled/wrong) `_lease_ck` passes `validate_schema()` clean. The `_drift_entry_to_error` docstring compounds this: it asserts "The canonical outbox table declares no CHECK / FK / UNIQUE constraints (only the autoincrement PK)" — flatly false (`schema.py` declares both the CHECK and a unique index). The 0.9.0 release note (`planning/releases/0.9.0.md:93`) similarly over-claims that autogenerate will add the CHECK.

**Impact.** An operator running `validate_schema()` from `/health` to confirm schema correctness gets a false all-clear when the lease CHECK is absent — and the package markets this method as going *beyond* alembic's diff (the S2 index-predicate probe), so users reasonably expect constraint coverage. Without `_lease_ck`, a half-set lease becomes representable (manual UPDATE setting one of the pair), producing a row permanently invisible to fetch, `cancel_timer`, and metrics. (Verifier adjusted Medium→Low: blast radius is narrow — package code always sets/clears the pair together, so a half-set lease requires both an external buggy UPDATE *and* the operator omitting the CHECK from their migration. The certain defects are the false docstring and the coverage gap.)

**Fix.** Add a `pg_constraint` (contype='c') catalog probe for `<table>_lease_ck` parallel to `_validate_index_predicates_sync`, comparing `pg_get_constraintdef` against the expected normalized predicate. Correct the `_drift_entry_to_error` docstring. Optionally revisit the 0.9.0 release-note claim.

### [LOW][design] Handler returning `OutboxResponse` with a naive/invalid `activate_at` retries the inbound message to exhaustion

`faststream_outbox/subscriber/usecase.py:905-919` (publish loop), `response.py:134-161` (deferred validation).

**Problem.** `OutboxResponse.__init__` does zero validation; all checks (session type, activate-args mutex, tz-awareness) defer to `OutboxPublishCommand` via `as_publish_command()`, called at `usecase.py:915` *after* the handler returned. A handler returning `OutboxResponse(..., activate_at=<naive datetime>)` raises an ordinary `ValueError` there, which propagates through the middleware stack → `_CaptureExceptionMiddleware` stashes it onto `row.last_exception` → the inbound row is nacked under `NACK_ON_ERROR` (or via `assert_state_set` under MANUAL). Because the error is deterministic, every reclaim re-raises it: the inbound message walks its full retry budget (default 10 attempts), then DLQs as `retry_terminal`.

**Impact.** A deterministic, never-recoverable misconfiguration of the *returned* response manifests as the *inbound* message exhausting its retry strategy, with a confusing `ValueError` unrelated to the handler's actual work. Notably inconsistent: the analogous `OutboxResponse` + foreign-publisher misconfig *is* given a distinguishable `_OutboxConfigError` (ERROR-logged, lease-expiry retry), but `activate_at`/`session` errors masquerade as handler failures. Narrow programmer-error path; no data loss.

**Fix.** Validate `OutboxResponse` construction args in `__init__` (mirror the activate mutex + tz-aware check) so the error raises at the `return OutboxResponse(...)` site, or special-case `as_publish_command()` `ValueError`s in `process_message` to raise `_OutboxConfigError`.

### [LOW][refactor] DLQ INSERT column list is a hardcoded SQL string that can silently drift from `make_dlq_table`

`faststream_outbox/client.py:318-331` (`_build_dlq_cte_stmt`) vs `schema.py:148-166` (`make_dlq_table`).

**Problem.** The DLQ CTE hardcodes the column list (`original_id, queue, payload, headers, deliveries_count, created_at, failure_reason, last_exception, timer_id`) as an f-string. `make_dlq_table()` independently declares those columns plus `id` (autoincrement PK) and `failed_at` (NOT NULL, server-default). Nothing programmatically links the two — the P9 `timer_id` comment in both files confirms they were already hand-edited in lockstep once. The only unit test asserts on schema-qualified table *names*, never the column list.

**Impact.** Correct *today* (the two omitted columns both have defaults). The risk is latent: a future change adding a NOT-NULL-without-default column to `make_dlq_table` would make every terminal DLQ write fail, rolling back the DELETE and creating poison rows that retry forever (the B10 failure mode); a nullable addition silently drops audit data. Integration tests against real Postgres *would* catch a broken INSERT, but only when Postgres is reachable. (Verifier adjusted High→Low: a maintainability hazard with zero current impact and a conditional trigger is not High.)

**Fix.** Derive the INSERT/SELECT column list from `self._dlq_table.c` (excluding the autoincrement PK and server-default-only columns), or add a unit test asserting the CTE column list equals `make_dlq_table(...).columns` minus `id`/`failed_at`. At minimum, cross-reference comments on both sites flagging the drift hazard.

### [LOW][refactor] Recorder swallow-and-log logic duplicated instead of reusing `_safe_emit`

`faststream_outbox/publisher/producer.py:64-68` vs `metrics/__init__.py:74-85`.

**Problem.** `OutboxProducer._emit_metric` is byte-for-byte identical to `metrics/_safe_emit` (same module `_logger`, DEBUG level, message, `exc_info`) and could delegate via `_safe_emit(self._metrics_recorder, event, tags)`. (Note: the subscriber's `_emit_metric` at `usecase.py:195-206` is *not* a third interchangeable copy — it deliberately routes through `self._log` for handler-scoped context, with an explaining comment. And the finder's secondary claim that the `_safe_emit` docstring is "factually wrong about being the single shared site" does not hold: the docstring says "shared by every call site that emits metrics *from the test broker*," which matches reality.)

**Impact.** Two places encode the recorder-isolation contract (swallow, DEBUG-log, never poison dispatch). A change to that contract on the module-logger shape must be applied twice or the seams diverge. Minor.

**Fix.** Have `OutboxProducer._emit_metric` delegate to `_safe_emit`.

### [LOW][design] Inconsistent `body` type hint across the three publish entry points

`broker.py:373` (`body: typing.Any`), `publisher/usecase.py:70` (`body: SendableMessage`), `response.py:136` (`body: typing.Any`); `publish_batch` uses `*bodies: typing.Any`.

**Problem.** The same logical `body` parameter is typed three ways, with the *narrowest* (`SendableMessage`) on the publisher — even though all three converge on `OutboxPublishCommand.__init__(body: typing.Any)` and the identical `_encode_payload` path. Both publish methods already carry `# ty: ignore[invalid-method-override]`, so the publisher is not forced to match an upstream type. No documented rationale; CLAUDE.md calls the publisher a "typed wrapper around `broker.publish` with the same transactional contract," implying parity.

**Impact.** A body legal at `broker.publish` could be flagged by `ty` at `pub.publish` despite identical runtime behavior, pushing users toward unnecessary casts. Minor least-surprise wart. (Medium confidence; likely-but-undocumented origin is mirroring upstream FastStream's `SendableMessage`.)

**Fix.** Use `typing.Any` consistently on `broker.publish`, `OutboxPublisher.publish`, and `OutboxResponse.__init__`.

### [LOW][design] `broker.request` override is `*args, **kwargs` — drops the typed contract and IDE help

`faststream_outbox/broker.py:521-523` vs the typed sibling `OutboxPublisher.request` at `publisher/usecase.py:118-127`.

**Problem.** `OutboxBroker.request` is `async def request(self, *args: typing.Any, **kwargs: typing.Any) -> typing.NoReturn`, discarding the parameter names/types that upstream `BrokerUsecase.request` and the sibling `OutboxPublisher.request` advertise. Internal inconsistency: the publisher models the same "unsupported" state with a real signature.

**Impact.** Cosmetic. The method always raises `NotImplementedError` (documented fire-and-forget); callers get no IDE signal that it is unsupported until runtime. No correctness consequence.

**Fix.** Mirror upstream's (or the publisher's) `request` signature so `NotImplementedError` is the only surprise, not the parameter list.

### [LOW][test] Worker-loop reconnect on a poisoned writer connection only mock-tested

`tests/test_unit.py:2164-2330`; production path `_run_with_reconnect` at `subscriber/usecase.py:471-516`, AUTOCOMMIT re-applied at `468`.

**Problem.** The "flush exception propagates → worker rebuilds its cached AUTOCOMMIT writer connection" path is exercised only with MagicMock engines (`test_worker_loop_reconnects_after_error` asserts `connect.call_count == 2`). No integration test injects a real DB error/connection drop mid-flush and verifies the worker rebuilds the writer conn and resumes draining against live Postgres.

**Impact.** Narrow. (Verifier adjusted Medium→Low: the constituents *are* integration-tested individually — `test_writer_connection_autocommit_round_trip` proves AUTOCOMMIT works end-to-end via the same code path, and writer-conn reuse is integration-tested; the "poisoned conn reused" worry is structurally impossible since `async with engine.connect()` opens a fresh conn each cycle. Only a regression specific to the reconnect-then-reapply sequence would slip through.)

**Fix.** Add an integration test forcing one flush failure (e.g. patch `delete_with_lease` to raise once, or terminate the backend pid), then assert the worker drains remaining rows successfully against real Postgres.

### [LOW][test] LISTEN-failure fallback to polling never verified end-to-end against real Postgres

`tests/test_unit.py:1682-1745, 2333-2352`; fallback path `subscriber/usecase.py:282-298, 363-369`.

**Problem.** "LISTEN failures log once and fall back to polling" is tested only by mocking `asyncpg.connect`/`add_listener` to fail. No integration test runs a real subscriber whose LISTEN connection can't be established and confirms it still delivers rows via the polling interval against live Postgres.

**Impact.** Low. (Verifier note: the finder overstated the impact — unit tests at `test_unit.py:2855, 2876` *do* drive `_fetch_inner(fetch_conn=None, listen_conn=None)` against the fake and assert the loop still fetches, so a "failed LISTEN wedges the fetch loop" regression would fail those tests. The residual gap is only the real-asyncpg/real-Postgres dimension.)

**Fix.** Add an integration test that patches `_open_listen_connection` to return `None` while running against real Postgres, publishes a row, and asserts delivery within ~`max_fetch_interval`.

### [LOW][refactor] Stale comment references a renamed method (`_build_dlq_cte`)

`faststream_outbox/client.py:541`.

**Problem.** The comment reads "matching the schema-qualified DLQ CTE in `_build_dlq_cte` (B10)" but the method is `_build_dlq_cte_stmt` (defined at `client.py:288`). A grep for `_build_dlq_cte` finds only this dangling reference plus the two real `_build_dlq_cte_stmt` sites — the bare name exists nowhere.

**Impact.** Cosmetic; a grep-driven reader following the cross-reference lands on a non-existent name.

**Fix.** Update the comment to `_build_dlq_cte_stmt`.

## Themes

**1. Fake-vs-real / mock-vs-Postgres coverage drift (4 of 12 findings).** The most consequential paths — lease expiry mid-flight, the relay `_OutboxConfigError` worker-loop catch, writer-conn reconnect, LISTEN fallback — are protected by unit mocks or fake-sync mode but never driven end-to-end through the live `_worker_loop` against real Postgres. The pattern is consistent: the *constituent SQL/logic* is well covered, but the *interleaving through the real loop* is not. This is the dominant residual risk surface and where the two Medium findings sit.

**2. Late / deferred validation surfaces errors far from the author.** `OutboxResponse` defers all validation to dispatch time (a documented single-source-of-truth choice — see the refuted appendix), but the consequence is real: an invalid returned `OutboxResponse` masquerades as a handler failure and exhausts the retry budget, and the inconsistency with the eager `_OutboxConfigError` relay guard is jarring. The `graceful_timeout=None` footgun is the same shape — a permissive type accepted at the boundary defers a "this can't work" condition to a far-away hang.

**3. Hand-maintained parallel sources of truth.** Two findings (DLQ CTE column list vs `make_dlq_table`; `validate_schema`'s docstring vs the actual `schema.py` constraints) stem from a SQL/string representation that must be kept in lockstep with the schema declaration by hand. The stale `_build_dlq_cte` comment is the cosmetic tail of the same pattern.

## Refuted / out of scope (appendix)

| Finding | Why dismissed |
|---|---|
| Fetch loop leases a batch after `_inflight.join()` returns | No `await` between `join()` returning and `task.cancel()`; the suspended fetch is cancelled at its await point before the put loop. Residual window is the universal "cancel during commit round-trip," covered by lease-expiry. |
| Upstream supervisor re-spawns a loop task during `stop()` | Trigger essentially unreachable (`_run_with_reconnect` swallows all `Exception`; CancelledError ignored by supervisor). Even if re-spawned, pre-set `running=False`/`_stopping=True` make the loop exit immediately. |
| `ping()` leaves a crashed task's exception unretrieved | `TasksMixin.add_task` attaches a supervisor callback that *retrieves* the exception and logs the full traceback at ERROR; no GC warning, no lost root cause. |
| `OutboxResponse` defers ALL validation (as a *bug*) | Documented intentional single-source-of-truth design; surfaces as a loud exception, not silent loss; test-broker sync mode hits it pre-prod. (The narrower deterministic-retry-exhaustion consequence is kept as a Low above.) |
| `fetch_unprocessed` silently truncates at `limit` | Documented OOM safety guard; caller controls `limit`; hitting exactly 1000 is itself a signal. |
| `activate_in` accepts negative/zero `timedelta` | Documented + tested as "immediately eligible" (recovered idempotency-token use case). |
| `LinearRetry`/`ConstantRetry` lack the `_MAX_DELAY_SECONDS` backstop | Backstop deliberately scoped to `ExponentialRetry` (overflow + interval limit are exponential-specific); claimed cross-strategy invariant does not exist; scenario physically unreachable. |
| `jitter_factor=2.0` permits a zero-second delay | Intentional "full jitter" semantics; exact-zero has measure zero; `max_attempts`/`max_total_delay_seconds` still terminate. |
| Fake DLQ dict carries `failed_at` the real write never returns | Real DLQ row genuinely has `failed_at` (server default); no test asserts on it; inert test-helper observation. |
| Loop-mode fake always wakes on publish (including future-dated) | Documented test-ergonomics choice; fetch re-checks eligibility on wake, so end behavior matches production; `_notify_event` is a wakeup hint, not the pg_notify seam. |
| Fetch-CTE partial-index usage never EXPLAIN-tested | `test_fetch_cte_carries_partial_index_predicates_as_conjuncts` (test_unit.py:3113) pins the compiled WHERE shape and catches the exact naive-collapse regression; an EXPLAIN test would be flakier on small tables. |

## Coverage notes

**Examined:** subscriber two-loop machinery (fetch/worker, reconnect, backoff), drain/`stop()` overrides and the `graceful_timeout` boundary, lease-token invariant threading, terminal-write paths (`delete_with_lease`/`mark_pending_with_lease`, DLQ CTE), `validate_schema()` and its alembic/index-predicate probes, the publish surface (`broker.publish`/`publish_batch`/`OutboxPublisher`/`OutboxResponse` and their validation timing), timers/`activate_*`/`timer_id`, retry strategies, relay-chain guardrails and `_OutboxConfigError` routing, the metrics recorder seam, the fake test broker (sync + loop modes), and the existing integration/unit/fake/relay test suites.

**NOT examined (or examined only shallowly):** the OpenTelemetry/Prometheus native middleware adapters beyond their seam description; the FastAPI router beyond the already-known (and deliberately excluded) `dlq_table`/`metrics_recorder`/`routers` forwarding gap; AsyncAPI schema generation; the `annotations.py` / `fastapi` Context wiring; actual Alembic migration-generation behavior against a live autogenerate run (reasoned from the installed comparator set, not executed); and performance/load characteristics (connection-budget sizing was read as documented, not measured). Confidence is high on all retained findings (the two `medium`-confidence caveats — the `body` type-hint origin and the precise blast radius of the lease test gap — are flagged inline).
