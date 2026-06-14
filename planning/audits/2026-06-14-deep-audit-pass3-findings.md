# Deep Audit — Pass 3 (fresh full re-sweep + deferred FastAPI gap) — 2026-06-14

> Third-pass multi-agent audit, run after the same-day pass-1 (7-finder code sweep) and
> pass-2 (5-finder middleware/AsyncAPI/FastAPI/alembic/perf sweep) whose findings shipped as
> PRs #81–#84. Method: **8 blind finders** in parallel (concurrency/error-paths, data-layer,
> security, API/contract, outbox-semantics, refactor/inconsistency, **test-correctness**,
> **FastAPI + the deferred forwarding gap**) — each given the codebase with *no* prior-findings
> list so framing wasn't anchored → **6 adversarial verifiers** (default-refute) on the
> defect-claiming subset → synthesis with cross-reference against the two prior 2026-06-14 docs.
> 14 agents, ~1.3M tokens. ~78 raw findings; defect-claiming subset adversarially verified,
> refactor/doc/test-hardening tail triaged by direct inspection.

## Executive summary

The fresh re-sweep paid off: it surfaced **one genuine HIGH-severity runtime defect that both prior
same-day passes missed** — `propagate_inbound_headers=True` poisons a *successful* handler's inbound
row whenever the relayed response body encodes to a different content-type than the inbound message
(F5-01). Two prior passes examined the relay/response path and called it clean; the header-propagation
content-type/correlation-id conflict (F5-01, F5-02) is new and reproducible. The second-most-important
result is that **PR #83's eager-validation fix on `OutboxResponse` is incomplete**: it added eager
`activate_*`/tz checks (closing the prior LOW), but `queue` and `session` validation are still deferred
to dispatch, so the exact "misconfig masquerades as a handler failure and burns the inbound row's
retry budget" footgun the fix was meant to kill still fires for a bad `queue`/`session` (F4-01, F4-02).

After adversarial severity adjustment the distribution is **1 High, 4 Medium, ~30 Low, 1 Refuted**.
The High and two of the Mediums (F5-02, F5-03) are one root cause — the response-publish step runs
*inside* the consume try-scope, so any failure there nacks the inbound row with no signal
distinguishing "handler failed" from "handler succeeded, relay publish failed." The other Mediums are
**F8-01** (the FastAPI `OutboxRouter` still cannot enable DLQ or the metrics-recorder seam — the known
deferred item, now confirmed mechanically forwardable for `dlq_table`/`metrics_recorder`) and **F7-02**
(the relay's central transactional-commit contract has no real-Postgres test). The Low tail is the
expected mix: bounded concurrency/observability nits, validation-symmetry gaps, doc/code drift, and
DRY clusters — none touching the load-bearing invariants (lease-token filtering, no-flush publish,
AUTOCOMMIT writer, drain budgeting), all of which held up under scrutiny again.

The single most actionable item is **F5-01 + F5-03**: strip envelope-managed keys (`content-type`,
`correlation_id`) before propagating inbound headers, and emit a distinct signal when the relay publish
(not the handler) is what failed.

## Findings by severity

### Critical
None.

### High

#### [F5-01][bug] `propagate_inbound_headers=True` poisons a successful inbound row on a cross-content-type relay
- **Location:** `subscriber/usecase.py:916-928` × `envelope.py:32-38`. **Verdict:** CONFIRMED (reproduced). **NEW** (relay path was called clean in passes 1–2).
- > **RESOLVED (2026-06-14)** — `_maybe_propagate_inbound_headers` now strips `content-type` (and `correlation_id`, see F5-02) from the propagated headers when the result is an `OutboxResponse`; foreign-publisher relays keep forwarding them verbatim. Tests: `tests/test_fake.py::test_propagate_inbound_headers_does_not_poison_cross_content_type_outbox_relay`. Doc: `architecture/relay.md`.
- **Problem.** When `propagate_inbound_headers=True` and the handler returns an `OutboxResponse` with no headers (the default — `Response.headers` is `headers or {}`), `if ... not result_msg.headers: result_msg.headers = dict(message.headers)` copies the inbound row's **full** header dict, including the envelope-managed `content-type`. Inbound rows always carry `content-type` for non-bytes bodies (`_encode_payload`, `envelope.py:39-40`). `as_publish_command()` then forwards that header into `_encode_payload` for the *new* body; `envelope.py:32-38` raises `ValueError` when the propagated `content-type` disagrees with the encoder's output for the new body.
- **Reproduction.** Inbound `str` body (`text/plain`) → handler returns `OutboxResponse(body={"x": 1}, ...)` (`application/json`) → `text/plain != application/json` → `ValueError`.
- **Impact.** The response-publish runs inside `process_message`'s `AsyncExitStack`, *after* the AckMiddleware `__aexit__` is pushed. The `ValueError` unwinds the stack; under the default `NACK_ON_ERROR` the AckMiddleware nacks the **successful** inbound row, which then walks its entire retry budget (default 10) and DLQs as `retry_terminal` — purely because of header propagation, for any relay that changes body type. The exception is then swallowed by `consume()`.
- **Fix.** Exclude envelope-managed keys (`content-type`, `correlation_id`) when propagating inbound headers — propagation should carry business/user headers, not envelope plumbing. The response body's own encoder owns `content-type`.

### Medium

#### [F5-02][bug] `propagate_inbound_headers=True` + explicit `OutboxResponse(correlation_id=...)` raises a correlation-id conflict
- **Location:** `subscriber/usecase.py:913-917, 926` × `envelope.py:45-51`. **Verdict:** CONFIRMED. **NEW.**
- > **RESOLVED (2026-06-14)** — same fix as F5-01 (`correlation_id` is stripped from the propagated headers for an `OutboxResponse`; the dedicated field still carries it). Test: `tests/test_fake.py::test_propagate_inbound_headers_does_not_poison_custom_correlation_id_outbox_relay`.
- **Problem.** Same propagation mechanism as F5-01: a handler returning `OutboxResponse(..., correlation_id="custom")` keeps `"custom"`, but the propagated inbound headers also contain the inbound `correlation_id`. `_encode_payload` raises `ValueError` when a passed `correlation_id` and a `headers["correlation_id"]` disagree (`envelope.py:46-51`), poisoning the inbound row exactly as in F5-01. Correctly scoped: it only fires when the handler sets a *custom* correlation id (omitting it inherits the inbound one → no conflict).
- **Fix.** Same as F5-01 — drop envelope-managed keys before propagating.

#### [F5-03][bug] A relay-publish failure nacks the successful inbound row with no *distinguishing* signal
- **Location:** `subscriber/usecase.py:919-928, 597-617, 835-837`. **Verdict:** PARTIAL (control-flow confirmed; "no log at all" overstated). **NEW.**
- > **PARTIALLY MITIGATED (2026-06-14)** — the F5-01/F5-02 fix removes the common trigger (header-propagation encode conflicts). The residual gap stands: a relay-publish failure from another cause (e.g. a DB error on the follow-on insert) still nacks the inbound row with no signal distinguishing it from a handler failure. Left open for a follow-up.
- **Problem.** Any exception while publishing the follow-on `OutboxResponse` row (the F5-01/02 encode conflicts, or a DB error on the follow-on insert/NOTIFY) unwinds the consume try-scope and nacks the inbound row even though the handler logic completed. `row.last_exception` is **not** set for a relay-publish failure (the capture middleware only stashes *handler* exceptions), so the resulting `nacked_retried` metric carries no `exception_type` tag — indistinguishable from an ordinary handler-driven retry.
- **Correction (from verification):** there *is* a generic ERROR log (CriticalLogMiddleware logs the exception with `exc_info`). It just doesn't *distinguish* "handler OK, relay failed" from "handler raised" — the log line is identical in both cases. So the finding's "no log" wording is wrong; the *no distinguishing signal* core holds.
- **Fix.** Mostly resolved by fixing F5-01/02 (removes the common trigger). Additionally consider validating content-type/correlation feasibility eagerly at the `return OutboxResponse(...)` site (mirroring the eager `activate_*` checks), or emit a distinct error log/metric when the relay publish — not the handler — failed.

#### [F8-01][gap] FastAPI `OutboxRouter` cannot enable DLQ or the metrics-recorder seam
- **Location:** `fastapi/router.py:67-149` vs `broker.py:154,160,162`. **Verdict:** CONFIRMED. **KNOWN/deferred** (`planning/deferred.md`; excluded from passes 1–2 by design — this pass audited it per request).
- > **RESOLVED (2026-06-14, PR #88)** — `OutboxRouter.__init__` now accepts `dlq_table` + `metrics_recorder` and forwards them to the inner broker (via `StreamRouter`'s `**connection_kwars`). DLQ archival and the recorder seam (`fetched`/`lease_lost`/`nacked_terminal`/`dlq_written`) are now reachable for FastAPI users. Test: `tests/test_fastapi.py::test_outbox_router_forwards_dlq_table_and_metrics_recorder_to_inner_broker`. `routers` remains unforwarded pending a design call (now the sole open item in `planning/deferred.md`). Docs updated (`docs/usage/fastapi.md`), which also clears **F8-02** (it wrongly said observability was unavailable via the router).
- **Problem.** `OutboxRouter.__init__` omits `dlq_table`, `metrics_recorder`, and `routers` from its signature and `super().__init__` passthrough. The inner broker is built during `super().__init__` with constructor-frozen config and no post-hoc injection handle, so a FastAPI deployment **cannot** enable dead-letter archival or the recorder-based signals (`fetched`, `lease_lost`, `nacked_terminal`, `dlq_written`) at all — exactly the operability features a production service needs.
- **Verification refinement.** The fix is *more mechanical* than `deferred.md` frames it: `dlq_table` and `metrics_recorder` are plain `OutboxBroker` kwargs that already flow through `StreamRouter`'s `**connection_kwars` (this is how `outbox_table` reaches the broker today). Adding them to the router signature + passthrough would just work. Only `routers` warrants a design decision (its semantics through the FastAPI lifespan / AsyncAPI are questionable).
- **Doc correction (see F8-02):** native Prometheus/OTel **middleware** observability *does* work via `OutboxRouter(middlewares=[...])`; only the *recorder* seam is blocked. The docs conflate the two.

#### [F7-02][test-gap] The relay's transactional-commit contract has no real-Postgres test
- **Location:** integration relay tests; `testing.py` fake producer path. **Verdict:** PARTIAL (the finder's framing is wrong; the gap is real). **NEW.**
- > **RESOLVED (2026-06-14, PR #87)** — `tests/test_integration.py::test_outbox_response_followon_row_commits_with_handler_transaction` drives the exact dispatch publish path (`OutboxFakePublisher(producer)._publish(OutboxResponse(...).as_publish_command())`) on a real session inside `session.begin()` against live Postgres, asserting the follow-on row is invisible to a separate connection mid-transaction and committed after — mirroring `test_publish_inserts_in_caller_transaction` for the direct path. Verified non-vacuous by temporarily injecting an early `session.commit()` in the producer (test failed), then reverting.
- **Problem.** There is **no test** that an `OutboxResponse(session=<real AsyncSession>)` follow-on row commits with the handler's domain writes against live Postgres. The only integration `OutboxResponse` usage is the dual-fire-guard test, where the row is deliberately never published. The fake-mode relay tests use an `AsyncMock(spec=AsyncSession)` against a transaction-less fake client, so they cannot catch a regression where the follow-on row commits on a *fresh connection* instead of the handler's session — a silent violation of the outbox's central guarantee.
- **Correction (from verification):** the finder claimed "relay tests pass `session=None`" and "`del session` is the relevant path" — both wrong (fake relay tests pass an `AsyncMock`; the `del session` leniency is on `broker.publish`, not the `OutboxResponse` producer path). The transactional-contract gap stands regardless.
- **Fix.** Add one integration relay test that returns `OutboxResponse` inside a real `session.begin()` and asserts mid-transaction invisibility + commit-with-domain-writes, mirroring `test_publish_inserts_in_caller_transaction`.

### Low

> **Behavior/robustness batch RESOLVED (2026-06-14, PR #92)** — **F2-10** (`validate_schema` now probes `indisunique`; a non-unique `timer_id_uq` is flagged — integration test recreates the index non-unique), **F1-03** (`OutboxBroker.stop` sets `running=False` *before* the subscriber-stop gather), **F1-04** (a timed-out drain emits a WARNING + `drain_timeout` metric instead of silently abandoning rows), **F1-06** (`_run_with_reconnect` captures `started` only after `open_resources` succeeds, so a slow-failing open no longer resets the backoff), **F3-02** (`validate_table_identifiers` extracted in `schema.py` and called from `OutboxClient.__init__`, so a directly-constructed/reflected over-long `Table` is rejected). `just test` → 514 passed, 100% coverage.

> **Safe-Lows batch RESOLVED (2026-06-14, PR #89)** — code: **F4-04** (`fetch_unprocessed` rejects `limit < 1`, real + fake), **F2-12** (`ping()` bounded by `asyncio.timeout(_PING_TIMEOUT_SECONDS)`), **F6-02** (Prometheus `published` count default unified to `0`, matching OTel), **F6-17** (removed the dead `outer is self.config` branch). Docs/comments: **F6-01** (stale "Task 5"), **F6-04** (`_run_validate` phrasing), **F6-13** (CLAUDE.md `make_interval`/`activate_at`), **F6-15** (`fetch_unprocessed` docstring), **F1-05** (`cancel_timer` commit-contingent return), **F2-06** (`timer_id` dedup window). Still open after this batch: **F2-10** (validate `indisunique` — needs a drifted-schema integration test, own change), **F6-05** (`_utcnow` dedup — needs a shared-module decision), **F5-04** (parser correlation_id fallback — subtle), and the larger refactor / test-hardening Lows below.

**Validation symmetry (gaps left by PR #83's eager-validation fix):**
> **F4-01/F4-02/F4-06/F4-10 RESOLVED (2026-06-14, PR #86)** — collapsed into one shared `_validate_publish_args(context, *, queue, session, activate_in, activate_at)` in `response.py` (order: activate-args → session → queue), called by the `OutboxPublishCommand` constructor, `OutboxResponse.__init__`, and `broker.publish_batch`'s empty branch. Tests: `tests/test_unit.py::test_outbox_response_rejects_{empty_queue,non_str_queue,non_async_session}_eagerly` + `::test_broker_publish_batch_empty_rejects_empty_queue`. Three dual-fire tests that passed `session=None` to `OutboxResponse` as a shortcut were corrected to a valid session — exactly the latent misuse F4-02 surfaces. **F4-04 (negative-limit `fetch_unprocessed`) remains open.**
- **[F4-01]** `OutboxResponse.__init__` validated `activate_*`/tz eagerly but **not `queue`** (non-str/empty/>255) — a bad `queue` deferred to `as_publish_command()` at dispatch, masquerading as a handler failure and burning the inbound row's retry budget. The exact footgun #83 closed for `activate_*`, left open for `queue`. CONFIRMED. (`response.py:139-162`)
- **[F4-02]** Same for `session` — `OutboxResponse.__init__` does no `isinstance(session, AsyncSession)` check, unlike every `broker.*` entry point. CONFIRMED. (`response.py:139-162`)
- **[F4-06]** `publish_batch` empty-batch early-return validates `session` + `activate_*` but skips `queue` validation, so `publish_batch(queue="", ...)` with no bodies returns silently while with bodies it raises. CONFIRMED. (`broker.py:452-456`)
- **[F4-04]** `fetch_unprocessed(limit=...)` has no `limit >= 1` guard; `limit=-1` raises a DB error on the real path but the fake does `rows[:-1]` (drops the last row) — a silent real/fake divergence. CONFIRMED. (`broker.py:511-537`, `testing.py:545-558`)
- **Root cause [F4-10/F6-07/F6-18]:** `OutboxResponse` re-implements a *partial* copy of `OutboxPublishCommand`'s validation/field set; the "fail-fast mirror" can't help but drift. A single shared `_validate_publish_args(...)` helper called by both would make F4-01/02/06 structurally impossible.

**Data layer (all CONFIRMED):**
- **[F2-01]** `max_total_delay_seconds` is measured `last_attempt_at - first_attempt_at`; the first attempt sets both equal, so the effective horizon is extended by ~one delay interval beyond the configured budget. (`retry.py:77-80`, `message.py:148-153`)
- **[F2-07]** `deliveries_count` counts *claims*, not handler runs (incremented in the fetch CTE). Under lease churn (`lease_ttl < handler P99`), reclaims bump it without a completed delivery, so `max_deliveries` can fire after fewer than N real handler runs. Inherent to "deliveries"; worth documenting that it counts claims. (`client.py:216`, `message.py:155-167`)
- **[F2-09]** The fetch CTE's `ORDER BY next_attempt_at, id` is only on the inner `ready`/LIMIT select; the outer `UPDATE ... RETURNING *` is unordered, so within-batch dispatch order is undefined (FIFO is "which rows picked," not "dispatch order"). (`client.py:205, 210-223`)
- **[F2-10]** `_validate_index_predicates_sync` checks the partial WHERE predicate by index name but never `indisunique`; a same-named **non-unique** `timer_id_uq` passes validation yet breaks `ON CONFLICT` at publish time. Add `i.indisunique` to the probe. (`client.py:454-497`)
- **[F2-12]** `ping()` runs `SELECT 1` with no `wait_for`/timeout — against a half-dead TCP socket it can hang for hours (kernel keepalive), defeating its use as a liveness probe. The LISTEN health probe deliberately wraps the same query in `asyncio.wait_for` and documents exactly this hazard; `ping()` should match. **NEW.** (`client.py:410-416`)
- **[F2-02]** *(downgraded Medium→Low)* `validate_schema()` disables `compare_server_default`, so a missing `next_attempt_at` default isn't directly probed. But the only *silent* outage variant (column nullable **and** default dropped) is caught by the nullability comparator; the default-only drift fails loudly at publish (NOT NULL violation). Residual gap is a narrow self-inflicted misconfig. PARTIAL. (`client.py:582-612`)

**Concurrency / error paths / shutdown (all CONFIRMED):**
- **[F1-01]** `free = _inflight.maxsize - _inflight.qsize()` counts only queued rows, not the up-to-`max_workers` in-flight (checked-out) rows, so total simultaneous leases can reach `fetch_batch_size + max_workers`. **Extends pass-2's lease-vs-queue-depth finding** from a different angle. (`subscriber/usecase.py:337`)
- **[F1-06]** `_run_with_reconnect` captures `started` *before* `open_resources`, so time spent blocked in a slow pool checkout that then fails counts as "healthy" and resets the backoff — defeating exponential escalation under a connection storm. Capture `started` after open succeeds. (`subscriber/usecase.py:509-527`)
- **[F1-02]** The LISTEN health probe isn't woken by the drain signal (`_stopping`/`_notify_event`); a mid-probe `stop()` waits for cancellation rather than the signal, adding bounded latency. (`subscriber/usecase.py:331-336`)
- **[F1-03]** `OutboxBroker.stop()` sets `self.running = False` only *after* `await asyncio.gather(...)`; if `stop()` is cancelled during the gather, `running` stays True over half-stopped subscribers. Set it before the gather (or `try/finally`). (`broker.py:338-346`)
- **[F1-04]** When `_inflight.join()` exceeds the drain timeout, `move_on_after` silently abandons in-flight rows (left to lease expiry) with **no metric and no WARNING** — operators can't distinguish a clean drain from a timed-out one. Inspect `cancel_scope.cancelled_caught` / unfinished count and log it. (`subscriber/usecase.py:255-265`)
- **[F1-07]** NOTIFYs emitted during a fetch-loop reconnect/backoff window are lost (LISTEN isn't durable); latency degrades to the poll interval until the next tick. Backstopped by polling — document it. (`subscriber/usecase.py:302-308`)
- **[F1-05]** `cancel_timer` returns a `bool` from an **uncommitted** DELETE on the caller's session; a `True` is only durable once the caller commits. Document that the return is contingent on commit. (`broker.py:499-509`)

**Semantics:**
- **[F5-04]** *(CONFIRMED)* `parser.py:17` falls back to `correlation_id=str(msg.id)`, but the canonical producer path always writes a `correlation_id` header — the fallback is dead for normal rows and yields a non-UUID id for out-of-band inserts. (`parser/parser.py:17` vs `envelope.py:52`)
- **[F2-04/F2-05]** `activate_at` (single publish) and *all* of `publish_batch`'s timing are computed on the **worker clock** (literal datetime), unlike `activate_in`'s server-side `make_interval`. The NOTIFY `is_future` decision for `activate_at` is therefore worker-clock-relative. Documented for `publish_batch`; the `activate_at` asymmetry is not. (`producer.py:131-189`)
- **[F2-06]** `timer_id` dedup is "one *live* row per `(queue, timer_id)`" — it resets after delivery/terminal failure (the DLQ keeps `timer_id` non-unique). Document so operators don't treat it as a global idempotency key. (`producer.py:142-151`, `schema.py:116-122`)

**Security / robustness (all CONFIRMED, all Low):**
- **[F3-01]** *(RESOLVED 2026-06-14, PR #93)* added `OutboxBroker(..., last_exception_renderer=...)` (also on the FastAPI router) — a `Callable[[BaseException], str | None]` that redacts (e.g. `type(exc).__name__`) or drops (`None`) the DLQ exception text; default keeps `repr`. Rendering centralized in `_render_last_exception`. Tests: `test_fake_dlq_{redacts_last_exception_with_renderer,drops_last_exception_when_renderer_returns_none}`. *(downgraded Medium→Low)* `last_exception` stores `repr(exc)` (≤8 KiB) verbatim into the DLQ; pydantic/asyncpg reprs can embed payloads/PII/secrets. Inherent + documented design (the payload is *already* in the operator's outbox/DLQ by contract), so not a boundary-crossing leak — but offer a redaction hook / `store_exception=False` for PII deployments. (`subscriber/usecase.py:109-117`, `client.py:324-337`)
- **[F3-02]** The 63-byte identifier guard lives only in `make_outbox_table`; a directly-constructed or reflected `Table` bypasses it, re-introducing the over-long-identifier failure. Move the check into `OutboxClient.__init__` / a shared validator. (`schema.py:63-78`)
- **[F3-03]** Persistent DLQ misconfig or a permanent `_OutboxConfigError` grows the outbox table without bound (rows cycle forever via lease expiry) — a config error degrading into storage-exhaustion DoS. Documented trade-off; recommend an operational alert on row count / `lease_lost` rate. (`client.py:259-266`, `subscriber/usecase.py:540-552`)
- **[F3-05]** The DLQ CTE is the one raw-SQL identifier-interpolation site; it's safe today (dialect `format_table` quoting + bindparams) but one refactor from an injection sink. Add a regression test with adversarial table names (`"`, `;`). (`client.py:288-339`)
- **[F3-04]** Several ERROR/WARNING logs interpolate queue/exception data via f-strings; `!r` neutralizes the direct CRLF vector but prefer structured `extra=` logging. (`subscriber/usecase.py` log sites)

**Refactor / DRY / dead code:**
> **DRY batch RESOLVED (2026-06-14, PR #90)** — **F6-11** (index/constraint suffixes → shared `_PENDING_IDX_SUFFIX`/`_TIMER_ID_UQ_SUFFIX`/`_LEASE_IDX_SUFFIX`/`_LEASE_CK_SUFFIX`/`_CHANNEL_PREFIX` constants in `schema.py`, imported by `client.py`'s validation dicts), **F6-05** (`_utcnow` → one `faststream_outbox/_time.py::utcnow`, 5 sites), **F6-03** (the conditional-`exception_type` tag idiom → `_with_exception_type` helper), **F6-12** (`is_future` → one `_is_future_dated` in `producer.py`, used by `_do_publish` + `publish_batch`), **F4-05/F6-09** (`_REQUEST_UNSUPPORTED_MSG` constant in `response.py`, 4 sites unified). All behavior-preserving; `just test` → 507 passed, 100% coverage. (F6-02, F6-17 were PR #89.)
- **[F6-11]** *(PARTIAL — actually 4 copies, not 3)* Derived index/constraint suffixes (`_pending_idx`, `_timer_id_uq`, `_lease_idx`, `_lease_ck`) are hand-duplicated across the `schema.py` length guard, the `schema.py` `Index/CheckConstraint` names, `client._EXPECTED_INDEX_PREDICATES`, and `client._EXPECTED_CHECK_CONSTRAINTS`. Hoist one shared suffix-constant tuple. (`schema.py:63-135`, `client.py:444-514`)
- **[F6-05]** `_utcnow()` duplicated in `message.py` + `testing.py` and inlined in `producer.py`/`broker.py` (5 sites). Hoist to a shared util.
- **[F6-03]** The "flush, and emit the metric only if the write landed" idiom + conditional `exception_type` tag is hand-inlined 3× in `dispatch_one`/`_flush_terminal`. Extract a `_terminal_tags(...)` helper.
- **[F6-12]** The `is_future` / activate→next_at resolution exists in 3 forms (`_do_publish`, `publish_batch`, `broker._compute_next_at_client_side`). Extract `_is_future_dated(...)`.
- **[F6-02]** `published` `count` default differs between adapters — OTel `tags.get("count", 0)` vs Prometheus `tags.get("count", 1)`. Dormant (producer always sets `count`) but a latent trap. Pick one. (`metrics/opentelemetry.py:217`, `metrics/prometheus.py:294`)
- **[F6-17]** `_warn_on_unstarted_foreign_publishers` keeps an unreachable `if outer is self.config` branch (`# pragma: no cover`) that the following `getattr(outer, "producer", None)` check already covers — dead code. (`broker.py:294-300`)
- **[F4-05/F6-09]** The "request unsupported" `NotImplementedError` message has 3 different spellings across broker/publisher/producer/fake; hoist a `_REQUEST_UNSUPPORTED_MSG` constant. `request()`'s dead `timeout` param + `# noqa: ASYNC109` is defensible (signature parity).
- **[F4-03]** `subscriber()` uses `title_`/`description_` while `publisher()` uses `title`/`description` — likely upstream-parity, but document the asymmetry. (`registrator.py:59-60` vs `106-107`)
- **[F4-09]** The fake `cancel_timer`/`fetch_unprocessed` drop the `session` type check (like `publish`'s documented P33 leniency) but aren't covered by the P33 note. Extend the note or enforce the check.

**Docs / comments (drift):**
- **[F6-01]** Shipped `process_message` docstring still says "Task 5 adds…" (a dev-plan artifact; the guard already exists) + dangling `G1`/`G3` plan IDs. (`subscriber/usecase.py:859`)
- **[F6-13]** CLAUDE.md says `publish` "computes server-side via `make_interval`" without qualification — only `activate_in` does; `activate_at` is a client literal. Tighten the line.
- **[F6-15]** `fetch_unprocessed` docstring says "Intended for test assertions" but `get_one()`/`__aiter__()` point operators to it as the canonical lease-free read and it has an OOM guard "for backlogged production tables." Reword to include operator inspection.
- **[F6-04]** `_validate_index_predicates_sync` docstrings phrase the predicate probes as if they run "through `_run_validate`"; they're siblings. Reword to "the alembic diff (`_run_validate`) doesn't catch these." (`client.py:468-557`)
- **[F8-02]** *(RESOLVED 2026-06-14, PR #88)* `docs/usage/fastapi.md` said FastAPI users can't get observability; corrected as part of F8-01 (the recorder seam now forwards, and native Prometheus/OTel middleware was always available via `OutboxRouter(middlewares=[...])`).
- **[F6-06]** *(downgraded Medium→Low)* Two public classes named `OutboxRouter` (`router.py` include-router vs `fastapi/router.py` APIRouter subclass). Namespaced (different importable modules, standard Python convention), so no runtime shadowing — a docstring cross-reference suffices. (`router.py:65`, `fastapi/router.py:61`)

**Tests:**
> **Test-hardening batch RESOLVED (2026-06-14, PR #91)** — **F7-04** (new `test_fetch_skips_rows_locked_by_another_transaction`: locks 20 rows in an open txn, asserts a concurrent fetch promptly claims the disjoint 10 — a regression to plain `FOR UPDATE` fails via `wait_for` timeout), **F7-06** (assert `id` + `acquired_token` threaded into `delete_with_lease`, not just the conn), **F7-11** (capture + assert the relayed body on the successful retry), **F7-10** (identity-check the injected producer/client against the swapped fakes; added `TestOutboxBroker.fake_producer`), **F7-05** (assert DLQ `headers` round-trips), **F3-05** (new `test_dlq_cte_quotes_adversarial_table_names` for the raw-SQL identifier site). `just test` → 509 passed, 100% coverage. **Deferred to a follow-up:** F7-07 (NOTIFY-wakeup determinism — fiddly), F7-09 (telemetry exact bounds — spans 4 files), F1-08 (route sync dispatch through `fake_client.fetch` — touches sync-dispatch, higher risk).
- **[F7-04]** *(PARTIAL, downgraded Medium→Low)* `test_two_concurrent_fetches_dont_double_claim` (20 rows, two LIMIT-10 fetches) would pass on plain `FOR UPDATE` (or no SKIP LOCKED) — it doesn't isolate SKIP-LOCKED behavior. It *does* still guard the no-double-claim invariant it's named for. (The finder's "not actually concurrent" mechanism claim is wrong — the two executes can overlap on distinct pooled connections.) To exercise contention: seed >2×limit rows with overlapping limits, or hold A's txn open while B fetches. (`test_integration.py:128-142`)
- **[F7-05]** Fake DLQ tests never assert the `headers` audit column round-trips (only the PG-gated `test_dlq_atomic_insert` does). Add `assert row["headers"] == {...}` to one fake DLQ test.
- **[F7-06]** `test_dispatch_one_threads_writer_conn_into_delete` (+ retry twin) assert only `args[0] is conn`, not that `row.id`/`row.acquired_token` (the load-bearing lease guard) are threaded. Also assert the token arg.
- **[F7-07]** The loop-mode "wakes via NOTIFY" tests can pass on the polling path (the feed may land before the loop enters its idle wait). Spy `_wait_for_notify_or_timeout` to confirm the wait was interrupted by the event.
- **[F7-09]** Telemetry tests use loose bounds (`>= 1`, `max(...)`, `any()`) that survive double-counting / duplicate-series regressions, and the acked-histogram test doesn't assert the `status="acked"` attribute. Tighten to exact `== 1` + sum, assert attributes.
- **[F7-10]** The FastAPI annotation-injection test `isinstance`-checks the injected `client`/`producer` instead of identity-checking the swapped fake (the non-FastAPI test asserts `is`). The annotation *string* is the load-bearing artifact — assert identity.
- **[F7-11]** `test_relay_at_least_once_under_foreign_publish_failure` asserts `call_count >= 2` but never checks the successful publish carried the right body — a retry with an empty/wrong payload would pass. Capture and assert `cmd.body`.
- **[F1-08]** The sync test broker hand-rolls lease+`deliveries_count` increment in `_sync_dispatch` instead of sharing `FakeOutboxClient.fetch`, so `max_deliveries` boundary behavior is exercised on a different path than loop mode. Route sync dispatch through the fake fetch. (`testing.py:285-288`)

## Refuted / corrected

- **[F7-01] REFUTED** — claim that sync-mode's ignore-`next_attempt_at` leaves timer eligibility with *no off-Postgres guard*. False: `test_fake.py::test_loop_mode_delays_delivery_by_next_attempt_at` and `::test_fake_client_future_next_attempt_is_invisible_to_fetch` both run off-Postgres (loop mode / direct fake fetch) and would fail if the `next_attempt_at <= now()` gate were removed.
- **Downgrades on verification:** F2-02 (Medium→Low, the silent variant is caught by the nullability probe), F3-01 (Medium→Low, inherent/documented, no new trust-boundary crossing), F4-01 (Medium→Low, first-delivery programmer error, no data loss), F6-06 (Medium→Low, namespaced), F6-11 (Medium→Low, DRY smell caught by existing tests, no runtime corruption), F7-04 (Medium→Low, named invariant still covered).
- **Verification-only / no-defect (confirmed clean):** lease-token guard on all terminal writes (F1-09); `terminal_failure_reason`-before-`last_exception` branch ordering; the `OutboxFakePublisher` isinstance gate + `reply_to` trick; `_OutboxConfigError` relay routing; no-flush/no-commit publish contract on every producer path; FastAPI transactional session wiring + lifespan auto-start + `apply_types=False` interaction + `dependency_overrides` (F8-03/04/05); no SQL/identifier injection, no `eval`/`exec`/`pickle`/ReDoS; the fetch-CTE partial-index conjunct shape (guarded by `test_fetch_cte_carries_partial_index_predicates_as_conjuncts`, F7-03).

## Coverage

**Finders (blind):** concurrency & error paths · data-layer/SQL · security · public API/contract · outbox semantics · refactor/inconsistency · test correctness · FastAPI + deferred forwarding gap.
**Verifiers (default-refute):** F5 cluster (reproduced) · F2 data-layer cluster · F4 API cluster · F1 concurrency cluster · F3-01 + F8-01 + structural (F6-06/F6-11) · F7 test-correctness cluster.
**Examined and clean** (beyond the verification-only list): envelope encode/decode round-trip, header merge precedence, batch-bodies leading-`None` handling, future-dated NOTIFY-skip logic, the retry-vs-lease-expiry distinction, and the metrics recorder-vs-middleware seam separation.

**Net vs prior passes:** the fresh re-sweep found a real HIGH (F5-01) and an incomplete prior fix (F4-01/02) that two same-day passes missed — validating the blind, no-anchor approach. No new findings touched the core invariants. No code changes made (findings-doc-only, per request).
