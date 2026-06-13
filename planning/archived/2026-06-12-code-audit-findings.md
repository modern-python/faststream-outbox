---
status: shipped
date: 2026-06-12
slug: 2026-06-12-code-audit-findings
scope: faststream_outbox/ (package) + tests/ (test quality)
prs: [61, 66, 67, 68, 69, 70]
findings_doc_prs: [62, 65]
releases: ["0.9.0", "0.9.1"]
outcome: >
  All findings remediated, nothing deferred. Bugs B1–B16 (#61) + test-holes
  T1–T8 (#66) + improvements P1–P35 (#67) shipped in 0.9.0; suspected S1–S5
  (#68; S3 was already resolved by P17) + warning-attribution P27 (#69) +
  test-broker dedup/NOTIFY P29/P30 (#70) shipped in 0.9.1.
---

# Code audit findings — 2026-06-12

Audit of the package (~37 source files) and test suite on four lenses:
correctness & concurrency, data integrity & SQL, API design & code
quality, test quality. Eight parallel subsystem reviews (fetch/LISTEN
loop, worker/dispatch/drain, SQL layer, producer side, wiring/API,
test-broker + metrics adapters, core test quality, periphery test
quality) plus an inline pass: `ruff`/`ty` clean, full dockerized suite
**401 passed** against Postgres 17 with the 100%-coverage gate, and a
cross-cutting invariant sweep (lease filters, recorder wrapping,
isolation modes — all sound).

**Verification gate:** every bug-severity claim was re-verified by me —
by runnable repro where feasible (marked **[repro]**), otherwise by
direct source read (marked **[read]**). Unproven interleavings stayed
`suspected`. Three findings were independently converged on by 2–3
reviewers, noted inline.

Severity tiers: **bug** (confirmed wrong behavior), **suspected**
(plausible, not proven end-to-end), **test-hole** (a load-bearing
invariant the 100%-coverage suite does not actually pin — named by the
mutation that survives), **improvement**.

## Remediation status (2026-06-13)

**All 16 confirmed bugs (B1–B16) are fixed in PR #61** (branch `worktree-fix-audit-bugs-b1-b16`, commit `74f8554`). Each fix ships a
regression test verified red on the pre-fix code where mechanically feasible.
Verification: full dockerized suite (Postgres 17) **423 passed** (was 401 — 22 new
tests), the **100% coverage** gate met, `ruff format`/`ruff check`/`ty check` clean.

Implementation followed the audit's suggested directions: B5/B6/B7 accept-and-ignore
`**options` on ack/nack/reject, the manual-fallback nacks (honors retry) when
`last_exception` is set, a raising retry strategy degrades to `retry_terminal`, and
`ExponentialRetry` clamps the exponent (`_MAX_DELAY_SECONDS` ceiling). B8 overrides
`OutboxPublishCommand.batch_bodies`. B10 uses `format_table` + `MetaData(schema=...)`.

Two notes worth recording:

* The first B5 regression test passed for the **wrong reason** — a handler typed
  `msg: OutboxMessage` never had the message injected, so the body never ran and a
  DI exception (not the intended `raise`) drove the retry. Coverage caught it; the
  handler is now body-based and genuinely exercises "MANUAL + handler exception".
* B13's missing-extra import path can't be exercised under the all-extras CI (the
  audit itself flagged this); the fix is structural — the new `else`/`except`
  branches carry `# pragma: no cover`.

**Still open** (out of scope for PR #61): suspected S1–S5, test-holes T1–T8, the
additional test findings, improvements P1–P35, and the doc / CLAUDE.md drift items.

## Summary — bugs

| ID | Where | One-liner | Verified |
|---|---|---|---|
| B1 | usecase.py:295,433-439 | Drain-window fetch spin: `_run_with_reconnect` loops on `running` (stays True during drain) with no sleep on clean return — connection-churn storm in production; **livelock** in test-broker loop mode (no await point; `stop()` never returns) | [repro] exit 137 |
| B2 | usecase.py:164,226 | `_stopping` never reset — after `stop()` → `start()` the subscriber never fetches again while hot-spinning connect/close; `ping()` stays green (3 reviewers converged) | [read] |
| B3 | usecase.py:432-447 | `error_attempt` never resets after recovery — lifetime error count accrues; after 7 cumulative blips every future blip costs a flat 30 s outage, and `min(…, cap)` annihilates jitter → lockstep reconnect herd | [read] |
| B4 | usecase.py:362-372 | LISTEN connection leaks when `connect()` succeeds but `add_listener()` fails (PgBouncer txn pooling, drop between awaits, cancellation) — one raw TCP conn leaked per reconnect cycle | [read] |
| B5 | message.py:140-148 + usecase.py:527 | `AckPolicy.MANUAL` + handler **exception** → fallback reject → row permanently DELETEd (DLQ `"rejected"`); opposite of every native FastStream broker's MANUAL semantics and of the project's own no-delete-on-first-error stance | [repro] |
| B6 | message.py:77-84 | `raise NackMessage(delay=5)` (FastStream's documented idiom) → `TypeError` inside ack middleware (our `nack()` takes no kwargs) → swallowed → fallback reject → **row deleted, even under default NACK_ON_ERROR** | [repro] |
| B7 | retry.py:107 + reject-fallback path | Any exception from a retry strategy converts the nack into terminal reject (row deleted, reason `"rejected"`). Deterministic trigger ships in-box: `ExponentialRetry` with `max_attempts=None` raises `OverflowError` at attempt 1025 (`2.0 ** 1024`) — ~3.5 days of a persistently failing row | [repro] |
| B8 | producer.py:165 + broker.py:379-388 | `publish_batch` drops a leading `None` body — upstream `batch_bodies` excludes `body is None`: `(None, x)` inserts 1 row; `(None,)` inserts nothing, no error, no metric. `publish(None)` meanwhile inserts `b""` | [repro] |
| B9 | metrics/prometheus.py:253-257 + usecase.py:487-501 | `faststream_received_messages_in_process` goes **negative**: max_deliveries path emits `nacked_terminal` without a preceding `dispatched`; adapter unconditionally `.dec()`s. Inverse leak (inc without dec) on consume-escape/shutdown paths (2 reviewers converged) | [read] |
| B10 | client.py:291-309 | DLQ CTE built from `preparer.quote(table.name)` — ignores `Table.schema`. `MetaData(schema="app")` + DLQ → `UndefinedTable` on every terminal failure (poison rows retry forever, outbox grows) or silent write to a same-named search_path table. `validate_schema()` is schema-naive too | [read] |
| B11 | broker.py:306 | `ping()` iterates `self._subscribers` — misses router-registered subscribers (the pattern the FastAPI docs promote); a dead worker task on a router subscriber never fails the health check | [read] |
| B12 | broker.py:299-310 | `ping(timeout)` accepts and **ignores** `timeout` — upstream brokers wrap in `move_on_after`; ours can hang on pool checkout / black-holed TCP for minutes during the exact partition ping exists to detect | [read] |
| B13 | metrics/prometheus.py:51 et al. | Friendly missing-extra `ImportError`s are unreachable: the modules unconditionally import `faststream.prometheus`/`.opentelemetry`, which import the third-party packages at module top — users without the extra get a raw `ModuleNotFoundError` from a faststream frame; the curated guards are dead code (only `metrics/opentelemetry.py` is correctly guarded) | [read] |
| B14 | testing.py:621-643 | Loop-mode `TestOutboxBroker` spawns every fetch/worker loop **twice** (upstream harness calls patched `start()` twice; spawn has no guard) — `max_workers=1` tests actually run 2 workers | [repro] (2 `_fetch_inner` entries) |
| B15 | testing.py + upstream `_fake_close` | Loop-mode worker tasks leak on context exit: `_fake_close` only flips `running=False`; workers parked on `_inflight.get()` are never cancelled — "Task was destroyed but it is pending!" noise, stale workers wake on re-entry | [read] |
| B16 | testing.py:504-518 | Patched `fetch_unprocessed` lacks the `limit` parameter — production-valid `broker.fetch_unprocessed(..., limit=10)` raises `TypeError` under the test broker | [read] |

## Summary — suspected

| ID | Where | One-liner |
|---|---|---|
| S1 | usecase.py:274-275 | Teardown's unbounded `listen_conn.close()` can hang on the same half-dead socket the health probe just detected — fetch loop wedged with no log until process exit (asyncpg close-on-dead-socket behavior unproven) |
| S2 | client.py:416-469 | `validate_schema()` cannot detect partial-index **predicate** drift — alembic's `compare_indexes` never compares `postgresql_where`; a wrong `timer_id_uq` predicate passes validation then breaks `ON CONFLICT` arbiter inference at publish time |
| S3 | metrics/prometheus.py:268-273 | `processed_total` double-counts every lease-lost message (`acked`/`nacked` emitted pre-flush, then `lease_lost` → `status="error"` inc for the same row) — sum exceeds true throughput |
| S4 | __init__.py:4,42-57 | The try-it-out resilience guard protects the attribute access but the module import itself sits unguarded at top level — an upstream module move breaks `import faststream_outbox` entirely, defeating the guard's stated purpose |
| S5 | testing.py:341-364,461-484 | Sync-mode batch publish dispatches handlers per-body mid-feed (handlers observe half-inserted batches — impossible in production) and emits `published` after the handlers ran (inverted event order) |

### Suspected — resolution (2026-06-13)

All five investigated against current `main`; each confirmed or dismissed (no longer "unproven"):

- **S1 — confirmed, fixed.** The teardown `listen_conn.close()` was unbounded. Now bounded
  via `asyncio.wait_for(..., _LISTEN_CLOSE_TIMEOUT)` with a `terminate()` fallback, so a
  half-dead socket can't wedge the fetch loop on shutdown.
- **S2 — confirmed, fixed.** A docker probe showed `validate_schema()` passes with a wrong
  `timer_id_uq` predicate (alembic's diff ignores `postgresql_where`). Added
  `_validate_index_predicates_sync` — a `pg_get_expr(indpred, …)` probe comparing each
  partial index's predicate against the expected value.
- **S3 — dismissed (already resolved by P17).** Emitting `acked`/`nacked_*` only after a
  successful flush means a lease-lost row emits `lease_lost` *instead*, never a paired
  ack/nack — so `processed_total` no longer double-counts. Pinned by
  `test_dispatch_one_lease_lost_emits_only_lease_lost_not_acked`.
- **S4 — confirmed, fixed.** The `try_it_out` import sat outside the resilience guard.
  Moved inside the `try/except (AttributeError, ImportError)` so an upstream module
  move/rename no longer breaks `import faststream_outbox`.
- **S5 — confirmed, fixed.** Sync-mode batch publish dispatched each handler mid-feed and
  emitted `published` last. Now inserts the whole batch, emits `published`, then dispatches
  — mirroring production (atomic batch INSERT → published → subscriber fetch).

Fixes shipped together; see PR for tests.

## Bug details (what the table can't carry)

**B1 + B2 (drain spin / dead restart).** The two-flag drain design keeps
`running=True` while `_stopping` gates new claims — but only
`_fetch_inner` checks `_stopping`; the reconnect wrapper re-enters it in
a tight loop with full connection setup/teardown per iteration. Repro:
loop-mode broker, in-flight handler, real `sub.stop()` → the process had
to be SIGKILLed (watchdog, exit 137) because the spin contains no await
point and starves the event loop, so `_inflight.join()` never resumes.
In production the same shape is a Postgres connection-churn storm
lasting the entire drain (seconds; the wedged-handler case burns the
full `graceful_timeout`). Because `_stopping` is also never cleared,
`stop()` → `start()` leaves the subscriber spinning forever and
consuming nothing — while `ping()` reports healthy. Fix is two lines:
thread `not self._stopping` into the fetch loop's reconnect predicate,
and reset `_stopping = False` in `start()`. Note `broker.stop` is
**mocked** inside `TestOutboxBroker` contexts (upstream `_patch_broker`),
which is why no fake-mode test ever hit this — and contradicts
CLAUDE.md's "drain tests must `await broker.stop()` inside the
`async with`" advice (those calls hit the mock).

**B5/B6/B7 — the reject-fallback trap (one root cause, three doors).**
`dispatch_one`'s `assert_state_set` fallback exists for "MANUAL handler
*returned* without acking → reject". But three other paths land in the
same fallback with state unset, and all three end in permanent DELETE
(or DLQ `"rejected"`):
- MANUAL + raised exception (consume swallows it; ack middleware is
  disabled under MANUAL) — a transient DB blip before `msg.ack()`
  destroys the message [repro: `rows_left=0, reason=rejected`].
- `raise NackMessage(delay=5)` / `AckMessage(**opts)` under **any**
  policy — upstream's ack middleware calls `message.nack(**extra_options)`,
  our overrides accept no kwargs, the `TypeError` is caught and logged
  CRITICAL upstream, state stays unset [repro under default policy:
  `rows_left=0, terminal=['rejected'], retried=[]`].
- A retry strategy that raises — including the in-box `ExponentialRetry`
  with default `max_attempts=None`… except the *default subscriber*
  strategy sets `max_attempts=10`, so the overflow needs an explicit
  `ExponentialRetry(..., max_attempts=None)` (which the docs present as
  a legitimate config) [repro: `rows_left=0, terminal=['rejected']`].
Suggested direction: accept-and-ignore `**kwargs` on ack/nack/reject;
in the fallback, branch on `row.last_exception` — exception present →
route through nack (honor retry strategy) or return without flushing
(lease-expiry redelivery); wrap strategy invocation so a strategy error
degrades to `retry_terminal` with an ERROR log, not `"rejected"`; clamp
the exponent in `_delay_seconds`. Also fix the fallback's ERROR log text
("state not set after handler returned") which is wrong for the raise
paths.

**B8 (batch `None`).** Root cause is inheriting upstream's
`batch_bodies` property (`(self.body,) if self.body is not None else ()`).
Read `(cmd.body, *cmd.extra_bodies)` in `OutboxProducer.publish_batch`,
or reject `None` bodies uniformly in the command constructor.

**B9 (gauge).** Cleanest fix at the adapter: only `.dec()` when the
event carries `duration_seconds` (present exactly for post-`dispatched`
terminals per `metrics/__init__.py`'s vocabulary), or skip the dec for
`reason="max_deliveries"`. The emitter-side alternative (emit
`dispatched` before the max-deliveries check) would lie about handler
invocation.

**B10 (DLQ schema).** `preparer.format_table(table)` handles
schema+quoting; `validate_schema` needs `MetaData(schema=table.schema)`
for its canonical copy; or explicitly reject `table.schema is not None`
at `OutboxClient.__init__` until supported.

**B13 (import guards).** Either guard the upstream
`faststream.prometheus`/`.opentelemetry` imports the same way the
third-party imports are guarded (the `metrics/opentelemetry.py` pattern),
or delete the dead constructor guards and document that these modules
require the extra at import time. The `*_raises_friendly_error*` tests
pass only because the dev env has the extras and they monkeypatch the
probe flag — they test the guard, not the import path.

## Test-quality holes (mutations that survive the 401-test suite)

| ID | Invariant | Surviving mutation |
|---|---|---|
| T1 | Lease filter, **retry** path | Delete `acquired_token == :token` from `mark_pending_with_lease`'s WHERE (client.py:350) — every test passes the correct token; the DELETE half is pinned (`test_delete_with_wrong_token_is_noop`), the UPDATE half is not |
| T2 | MANUAL fallback reject | Delete `await row.assert_state_set(logger)` (usecase.py:527) — both existing tests call the method directly, none through dispatch; a forgetful MANUAL handler then emits a false `acked` and the row redelivers forever |
| T3 | Consume-escape row preservation | Replace the `return` at usecase.py:519 with fall-through — the only test asserts "doesn't raise"; the mutation silently DELETEs the row on any middleware-bypassing failure (copy the shape of `test_dispatch_one_preserves_row_when_consume_early_exits_on_shutdown`, which gets this right) |
| T4 | Drain "no new claims" | Change the fetch guard to `while self.running:` — both drain tests pre-claim all rows before `stop()`, so continued claiming is unobservable |
| T5 | Relay dual-fire guard **ordering** | Move the guard below the publisher chain (usecase.py:802 vs 804-811) — `pytest.raises` still passes, but Kafka publish + outbox insert both happened; also nothing asserts the row survives the `_OutboxConfigError` |
| T6 | Index-implying fetch CTE shape | Replace the two-armed OR with the naive form — the fake/real parity test compares result sets (identical); nothing compiles the SQL or checks the partial-index predicates beyond `where is not None` |
| T7 | Recorder event **sequencing** | The Prometheus tests hand-feed `dispatched` before `nacked_terminal` — an order the max-deliveries path never produces; this is what masked B9 |
| T8 | Drain, off-Postgres | `tests/test_fake.py` contains **zero** drain tests; CLAUDE.md's "grep `test_drain_timeout` / `test_broker_stop` in test_fake.py" matches nothing (the real tests live in test_integration.py and skip silently without Postgres) |

Additional test findings: fake `None`-token terminal writes succeed
where real SQL no-ops (`None == None`; testing.py:157) and the two
loop-mode tests covering that guard assert nothing it does; default
retry-strategy parameters unpinned; lease-lost fake tests assert only
that handlers ran; drain timing bounds (`elapsed < 0.7`) are
CI-load-sensitive; FastAPI tests never exercise the transactional
contract (session-per-delivery, `OutboxResponse` chaining through the
FastAPI wrapper); OTel middleware has zero span assertions; two OTel
recorder tests are tautological; "started foreign broker does not warn"
untested; `OutboxRouter` kwarg forwarding untested end-to-end; six
local imports in test_unit.py violate the project's no-inline-imports
convention; stale plan references in test_relay.py:33-37.

## Improvements (grouped, one decision each)

**Producer / command validation**
- P1 — empty `publish_batch` skips the session-type check
  (broker.py:374-378) — add it to the early path (2 reviewers).
- P2 — explicit `correlation_id` kwarg silently loses to
  `headers["correlation_id"]` (envelope.py:41); raise like the
  content-type conflict or let the kwarg win.
- P3 — encode failures bypass the `published` error metric
  (producer.py:82-91,178-186).
- P4 — `OutboxPublishCommand` accepts batch-meaningless `timer_id` /
  `correlation_id` that the batch producer silently drops; reject in
  the constructor (the advertised single source of truth).
- P5 — no `queue` validation (empty string, non-str, >255 chars all
  reach SQL); add a constructor check.
- P6 — timer-conflict publishes are only distinguishable by `count=0`;
  tag explicitly or document the convention.

**SQL layer / schema**
- P7 — 63-byte guard covers the NOTIFY channel but not derived index
  names (`{table}_timer_id_uq` adds 13 bytes; names 51-56 bytes pass
  then fail at DDL time).
- P8 — no CHECK pairing `acquired_token`/`acquired_at`; a half-set
  lease (manual UPDATE) is permanently invisible to fetch, cancel, and
  metrics. `CheckConstraint("(acquired_token IS NULL) = (acquired_at IS NULL)")`.
- P9 — `timer_id` missing from `OutboxInnerMessage` and the DLQ copy —
  a terminally-failed timer loses its business dedup key in the audit
  trail.
- P10 — `delete_with_lease(dlq_payload=...)` with no `dlq_table` on the
  client silently degrades to plain DELETE; raise instead (public API,
  silent audit loss if wiring ever desyncs).
- P11 — fetch statement rebuilt per poll tick; build once per
  `(table, queues)` and use `queue = ANY(:queues)`.

**Fetch loop**
- P12 — no positivity validation for `min_fetch_interval` /
  `max_fetch_interval` / `lease_ttl_seconds` (0 or negative →
  busy-poll).
- P13 — jitter sleeps below the documented `min_fetch_interval` floor
  (`base × U(0.5, 1.5)`); clamp or reword.
- P14 — `_on_notify` ignores the payload (queue name); filtering on
  `payload in self._queues` would stop cross-queue wakeup storms on
  busy multi-queue tables.
- P15 — comment at usecase.py:349-352 claims queries on the LISTEN
  connection break delivery — contradicted by the health probe itself;
  reword before someone "fixes" the probe.
- P16 — `stop()` cancels tasks without awaiting them; a caller's
  immediate `engine.dispose()` races the cleanup (upstream parity, but
  a `gather` after cancel is cheap).

**Worker / dispatch**
- P17 — terminal metrics emitted before the flush: lease-lost rows
  count twice (`acked` then redelivered + counted again; overlaps S3);
  `dlq_written` already demonstrates emit-after-write. Also
  `dispatched` fires before the shutdown-guard return, leaving an
  unclosed event.
- P18 — `_OutboxConfigError` re-raise rides the worker reconnect path:
  each occurrence tears down the writer connection and backs off (up to
  30 s), throttling unrelated rows; the "…; reconnecting" ERROR text is
  misleading for a config error.
- P19 — identical log string "Outbox worker error" for two different
  failures (usecase.py:518 vs 571); dead `or` in `_nack`
  (message.py:100); client-clock `first/last_attempt_at` vs server-clock
  `next_attempt_at` skew worth a comment.

**Wiring / API surface**
- P20 — `stop()` re-evaluates the `subscribers` property across an
  `await` and zips `strict=True` — a mid-shutdown registration raises
  out of stop(), defeating its never-raise contract; snapshot once.
- P21 — foreign-broker warning lists queues from **all** subscribers,
  not the decorated one (broker.py:253).
- P22 — duplicate-queue warning is blind across routers; re-check in
  `start()` over `self.subscribers`.
- P23 — retry dataclasses accept invalid knobs (`jitter_factor > 2` →
  negative delays → hot retry); add `__post_init__` validation.
- P24 — `OutboxBrokerConfig.connect()/disconnect()` are dead code
  (nothing in upstream 0.7.1 or this package calls them).
- P25 — `_subscribers: list[...]` annotation lies (runtime is
  `WeakSet`); relevant to B11's `_subscribers` vs `subscribers` trap.
- P26 — `__aenter__` upgrade to full `start()` lacks the
  `# Upstream equivalent (replaced):` marker the project's own
  divergence discipline requires.
- P27 — factory warning `stacklevel=4` is wrong through the FastAPI
  router path (two extra frames).

**Test broker / metrics adapters**
- P28 — `published_messages_total{status="error"}` can never fire
  (`if count > 0` gate; errors carry `count=0`) — the "drop-in parity"
  claim is wrong for the exact label dashboards alert on.
- P29 — ~80 duplicated lines between `FakeOutboxProducer` and
  `_build_fake_publish*`; extract a shared helper.
- P30 — loop mode never simulates NOTIFY; every loop test compensates
  with `min_fetch_interval=0.01`; setting `_notify_event` on feed would
  mirror production and speed tests up.
- P31 — `feed()` accepts naive `next_attempt_at` → `TypeError` inside
  the patched-away logger → test hangs with zero diagnostic; validate
  tz-awareness like the publish path.
- P32 — `headers` dict shared by reference across fake row, inner
  message, and DLQ copy — handler mutation corrupts "persisted" state;
  copy at boundaries.
- P33 — patched `broker.publish` accepts any `session` while
  `publisher.publish`/`OutboxResponse` stay production-strict; document
  or align.
- P34 — `dlq_written` emits `exception_type: None` instead of omitting
  the key, contradicting the vocabulary doc (`nacked_terminal` already
  omits correctly).
- P35 — misleading comment: fake's "first matching subscriber wins —
  mirrors production" (production competes via SKIP LOCKED,
  nondeterministically).

## Doc / CLAUDE.md drift surfaced by this audit

- CLAUDE.md names `delete_with_lease_with_conn` /
  `mark_pending_with_lease_with_conn` — actual API is
  `delete_with_lease(conn, …)` / `mark_pending_with_lease(conn, …)`.
- CLAUDE.md still claims long table names "silently lose NOTIFY and
  degrade to polling" — `make_outbox_table` raises `ValueError` now
  (same stale claim the docs audit flagged in checklist.md).
- CLAUDE.md describes an `EngineState` lazy holder that doesn't exist.
- CLAUDE.md + `architecture/drain.md` point at regression tests
  (`test_drain_timeout*`, `test_broker_stop*` in test_fake.py) that
  don't exist there.
- `architecture/relay.md` says `_OutboxConfigError` "rides
  AcknowledgementMiddleware to the normal nack path so the row is
  retried" — in code the nack is never flushed; retry actually happens
  via lease expiry, without `retry_strategy` delays (see P18).
- `architecture/timers.md:18` has the stale NOTIFY-skip wording
  (pre-"genuinely future-dated"); same issue the docs audit found in
  the user docs (I2).

## Verified sound (highlights)

- **Lease-token invariant:** all three terminal writes filter on
  `acquired_token` (client.py:268, :300, :350); the DLQ CTE carries the
  guard inside the `deleted` CTE; `cancel_timer` keeps the
  `IS NULL` guard; the fake mirrors all of it.
- **Fetch CTE concurrency:** CTEs with `FOR UPDATE SKIP LOCKED` are
  never inlined by Postgres (materialized once); the outer UPDATE
  operates on rows we hold — atomic claim. EvalPlanQual interactions
  (cancel_timer vs in-flight fetch) resolve invariant-preservingly.
- **Index alignment:** both OR disjuncts carry their partial-index
  predicates; `cancel_timer`'s equality implies `IS NOT NULL`;
  `make_interval` typing and `ORDER BY (next_attempt_at, id)` check out.
- **Transactional contract:** producer never flushes/commits/begins;
  NOTIFY is parameterized (injection-safe), transactional, and skipped
  exactly when future-dated or conflict-suppressed.
- **Notify-event ordering:** no lost wakeups (clear-after-wait + fetch
  always follows); `_on_notify` runs on the same loop, no
  thread-safety issue; inflight puts can never block.
- **Shutdown-race guard in `dispatch_one`** is correct and well-tested
  (`test_dispatch_one_preserves_row_when_consume_early_exits_on_shutdown`
  is the model for fixing T3).
- **`_CaptureExceptionMiddleware` ordering** (capture before ack-nack)
  verified against upstream's LIFO exit; exception-type-aware retry
  works.
- **`broker.stop()` parallel-gather** override faithfully reproduces +
  improves upstream 0.7.1's body; annotations' `Context` paths resolve;
  FastAPI router kwarg plumbing matches upstream's contract (beyond the
  known dlq/recorder gap).
- **Retry math at the boundaries** (attempts=0, `>=` on max_attempts,
  jitter-before-clamp ceiling) is correct and property-tested.
- **Cancellation safety in dispatch:** `CancelledError` passes through
  consume/flush untouched; mid-handler cancel leaves the lease for
  reclaim (at-least-once preserved).

## One disagreement worth recording

The fetch-side reviewer called the stop-vs-fetch-batch race a bug
(rows claimed by an in-flight fetch CTE can be queued after
`_inflight.join()` already returned, then cancelled — stranded under
lease until TTL); the worker-side reviewer judged the same interleaving
acceptable (at-least-once holds via TTL reclaim). Both agree on the
mechanics. I've left it **out of the bug table**: impact is bounded to
one fetch batch per shutdown and the drain promise is explicitly
best-effort within `graceful_timeout`. If you want it closed, the fix
is ordering: stop/await the fetch task before `_inflight.join()` in
`OutboxSubscriber.stop()`.
