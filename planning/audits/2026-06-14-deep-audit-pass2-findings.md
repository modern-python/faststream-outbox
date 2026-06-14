# Deep Audit — Pass 2 (previously-uncovered surfaces) — 2026-06-14

> Second-pass multi-agent audit of the surfaces the main 2026-06-14 audit deliberately
> skipped or skimmed: native telemetry **middleware** adapters, **AsyncAPI** schema
> generation, **FastAPI / Context** wiring, **alembic autogenerate**, and **performance**.
> 5 finders → adversarial verification → synthesis. 11 agents, ~544K tokens. 5 findings,
> all 5 confirmed, 0 refuted. The HIGH finding was additionally **reproduced by hand** (see note).

## Executive summary

The second pass surfaced **5 findings**: 1 high, 0 medium, 4 low (two perf items were
downgraded from medium to low by the verifier). The single most important item is the
**AsyncAPI document being structurally empty** for every outbox broker — `BrokerSpec(url=[])`
short-circuits upstream's channel/operation generation, so the publisher's headline documented
purpose ("exists primarily for AsyncAPI spec coverage") produces nothing observable across the
CLI, the FastAPI `/asyncapi` endpoint, and `AsyncAPI(broker).to_specification()`, for both
schema versions. The remaining four are low-severity: one docstring overstatement on alembic
autogenerate coverage, and three bounded perf/guardrail nuances (lease-vs-queue-depth sizing
gap, multi-queue sort node, NOTIFY churn at saturation). The **middleware** and
**fastapi-context** surfaces came back **clean**. Consistent with the first pass, this is mostly
a low-severity tail — no critical, no data-correctness, no invariant violations.

> **Hand-verification of the HIGH finding (2026-06-14):** reproduced directly — a broker with
> one subscriber + one publisher yields `channels={}, operations={}, servers={}`; injecting a
> non-empty `specification.url` immediately yields `channels=['orders:Handle','events:Publisher']`,
> populated operations, and `servers=['development']`. The bug and the fix direction both hold.

## Findings by severity

### [HIGH][bug] `url=[]` makes the entire AsyncAPI document empty (no servers, channels, or operations) for every outbox broker

> **RESOLVED (2026-06-14)** — `_spec_url(engine, outbox_table)` now supplies a non-empty url (password-masked engine DSN, or a `postgresql://outbox/<table>` placeholder when the engine is None), so the assembled document populates. Tests: `tests/test_unit.py::test_asyncapi_document_populates_channels_and_operations` (+ `..._include_in_schema_false_excludes_publisher_channel`).

**File:** `faststream_outbox/broker.py:189-196`

**Problem:** `OutboxBroker` constructs its `BrokerSpec` with `url=[]`. Upstream's AsyncAPI generator (v3 `generate.py:95-97`, v2.6 `generate.py:90-95`) produces channels **and** operations only for brokers present in `broker_servers`, and that map is populated solely inside `for url in specification.url:` (`generate.py:181`). With an empty url list, the loop body never runs, `broker_servers` stays empty, and the channel/operation comprehension iterates nothing. Runtime reproduction confirmed: a broker with one subscriber and one publisher yields `servers={}, channels=[], operations=[]` for both schema 3.0.0 and 2.6.0; setting a non-empty url immediately populates all three. Every upstream broker passes a non-empty url (e.g. `redis/broker/broker.py:121`, `nats/broker/broker.py:428`); the outbox broker is the deviation.

**Impact:** Every operator running AsyncAPI generation against an `OutboxBroker` — CLI `faststream docs gen`, the FastAPI router's `/asyncapi` endpoint (`fastapi/router.py:90`, default `schema_url="/asyncapi"`), or `AsyncAPI(broker).to_specification()` — gets a blank document. All correct per-publisher/per-subscriber spec work (`pub.schema()` / `sub.schema()`, title/description/schema/include_in_schema) is silently discarded at document assembly. This directly nullifies the publisher's documented headline purpose (`docs/usage/publisher.md:8,119`; CLAUDE.md). Not data-correctness or runtime-safety, hence not critical, but it fully defeats a documented feature across all three generation paths and both schema versions.

**Suggested fix:** Pass a non-empty url to `BrokerSpec` so the broker enters `broker_servers`. Derive from the engine when available (`engine.url.render_as_string(hide_password=True)`), falling back to a stable placeholder like `f"postgresql://outbox/{outbox_table.name}"` when engine is `None` (test-broker / pre-connect). Add a regression test asserting `AsyncAPI(broker).to_specification().to_jsonable()` has non-empty channels for a registered subscriber and publisher, and that `include_in_schema=False` removes a channel — the bug exists today precisely because no test exercises the assembled document.

### [LOW][docs] `schema.py` docstring overstates autogenerate coverage of partial indexes / CHECK on the incremental-migration path

> **RESOLVED (2026-06-14)** — the module docstring now scopes the autogenerate guarantee to fresh `create_table` and points at `validate_schema()` as the incremental-migration/drift backstop.

**File:** `faststream_outbox/schema.py:4-11, 97-105`

**Problem:** The module docstring claims the partial indexes "are declared on the table, so Alembic autogenerate picks them up and users can't forget them." This is true only for fresh `create_table` (rendering `CreateTableOp.from_table` emits the CHECK and each partial index with its `postgresql_where`). On an **incremental** migration onto a pre-existing table, alembic's Postgres index comparator never reads `indpred`/`postgresql_where`, and there is no CHECK-constraint comparator at all — so a drifted/non-partial predicate or a missing `<table>_lease_ck` ships silently. The codebase's own code corroborates the gap precisely: `client.py:395-396`, `client.py:468-474` (`_validate_index_predicates_sync`), `client.py:521-523` (`_validate_check_constraints_sync`).

**Impact:** A reader trusting the docstring may assume autogenerate fully protects the schema on every migration and skip the opt-in `validate_schema()`. Runtime is correctly backstopped via the dedicated `pg_catalog` probes; only the docstring overstates the guarantee. Low severity — docstring-only, behavior is sound.

**Suggested fix:** Scope the docstring's autogenerate guarantee to fresh `create_table` and point at `validate_schema()` (and its `pg_catalog` probes) as the backstop for incremental migrations / drift, mirroring the accurate wording already in `client.py:395-403` and the CLAUDE.md `validate_schema` section.

### [LOW][perf] Leased rows can expire while queued in `_inflight` when `fetch_batch_size` outruns `max_workers` × handler throughput

> **RESOLVED — docs (2026-06-14)** — `docs/usage/subscriber.md` now documents the `(fetch_batch_size / max_workers) × P99(handler) ≪ lease_ttl_seconds` sizing invariant. Left as guidance (not a code guard) since the lease-token invariant already makes the duplicate self-correcting; tightening the factory warning is optional.

**File:** `faststream_outbox/subscriber/usecase.py:337-363`

**Problem:** The fetch loop claims up to `fetch_batch_size` rows (default 10), each acquiring a lease at fetch time (`acquired_at=now()`), and pushes them all onto `_inflight` (`maxsize=fetch_batch_size`, `usecase.py:167-169`). With `max_workers=1` (default) the single worker processes serially; the lease clock on queued-but-unstarted rows runs while they sit idle. The worker dispatches with no lease-validity re-check (`usecase.py:536-539`), so if queue-depth × per-row handler time exceeds `lease_ttl_seconds`, a queued row's lease expires before dispatch and a competing fetch reclaims it → duplicate delivery / wasted work. The factory only warns on `lease_ttl_seconds <= max_fetch_interval` (`factory.py:165-174`); even its recommended formula `2*max_fetch_interval + P99(handler)` omits the in-queue serialization term.

**Impact:** Wasted/duplicate work that self-corrects (the lease-token invariant on terminal writes prevents state corruption). This is a variant of the already-documented at-least-once / idempotent-handler hazard (`subscriber.md:114-137`), with a guardrail gap rather than a new bug class. Downgraded to low by the verifier.

**Suggested fix:** Document the invariant `(fetch_batch_size / max_workers) × P99(handler) << lease_ttl_seconds`, and extend the factory warning to add the queuing term or warn when `fetch_batch_size > max_workers` with a tight TTL. Alternatively cap the in-flight queue depth nearer `max_workers`, or re-fetch the lease timestamp per row at dispatch.

### [LOW][perf] Multi-queue fetch loses the partial-index sort and forces a sort node

> **RESOLVED — docs (2026-06-14)** — CLAUDE.md's fetch-CTE note now records that the index-backed ordering holds only for single-queue subscribers; multi-queue / expired-lease fetches incur a `LIMIT`-bounded sort, with one-subscriber-per-queue as the mitigation.

**File:** `faststream_outbox/client.py:184-209`

**Problem:** The fetch CTE filters `queue = ANY(:queues)` and `ORDER BY next_attempt_at, id`. `_pending_idx (queue, next_attempt_at)` satisfies both predicate and ordering for a **single** queue (streamable index scan). For a subscriber serving multiple queues, the index yields rows ordered per-queue, not globally by `next_attempt_at`, so the planner adds a Sort/merge node. Branch B (expired-lease reclaim) orders by `next_attempt_at` while `_lease_idx` is ordered by `acquired_at`, so a sort is needed there for any queue count. Multi-queue is first-class (`config.queues` plural; `usecase.py:191-192, 347`).

**Impact:** Single-queue subscribers (documented default) unaffected. Multi-queue or expired-lease-dominated fetches pay a `LIMIT`-bounded sort each tick — real work not reflected in the "single round-trip, index-backed" framing. The P11 `= ANY` change was documented only for prepared-statement reuse; the sort consequence is undocumented. (Confidence medium: no live Postgres was reachable to confirm the planner chooses Sort vs. incremental/merge-append.)

**Suggested fix:** Note in `architecture/` that the index-only ordering guarantee holds for single-queue subscribers and that multi-queue / expired-lease-dominated fetches incur a bounded sort; if multi-queue ordering fairness matters, prefer one subscriber per queue (already the recommended segregation pattern for lease TTLs).

### [LOW][perf] NOTIFY during a saturated `_inflight` queue causes repeated wake/check/re-sleep cycles

> **WON'T FIX (2026-06-14)** — verifier called it "borderline none": CPU-only, bounded (idempotent `Event.set()` collapses bursts to ≤1 extra wakeup/cycle), no DB I/O, only while the queue is full. Not worth the added complexity of a separate drain-signal. Left documented here.

**File:** `faststream_outbox/subscriber/usecase.py:337-339, 368-372`

**Problem:** When `free <= 0` (in-flight queue full) the loop waits on `_notify_event` then `continue`s without fetching; `_wait_for_notify_or_timeout` unconditionally clears the event on wakeup. `_on_notify` sets the event for every NOTIFY whose payload matches a served queue, so during saturation each matching NOTIFY can wake the loop, recompute `free`, find it still 0, clear, and re-await — a wakeup + `qsize()` + `clear()` + re-await with no DB round-trip.

**Impact:** Pure CPU churn, no DB I/O, only while the queue is full, and bounded — `Event.set()` is idempotent so bursts collapse into at most one extra wakeup per loop cycle, and the exactly-zero-free window across multiple NOTIFYs is narrow. Negligible micro-inefficiency inherent to the edge-triggered Event design; arguably borderline none.

**Suggested fix:** When `free <= 0`, skip waiting on `_notify_event` (no slot to fill) and wait on a worker-drain signal instead; or sleep `base` without clearing the event so a single later NOTIFY triggers exactly one fetch once a slot frees. Low priority.

## Per-surface verdict

- **middleware** (opentelemetry/prometheus adapters, recorder seam): **CLEAN** — no findings. Recorder-vs-middleware two-seam split and shared `"outbox"` label are intentional per design.
- **asyncapi**: **1 finding** (HIGH) — empty assembled document via `url=[]`.
- **fastapi-context** (router overrides, `Annotated[..., Context(...)]` wiring): **CLEAN** — no findings.
- **alembic** (autogenerate / schema declaration): **1 finding** (LOW, docs) — docstring overstates incremental-migration coverage; runtime backstop is correct.
- **perf**: **3 findings** (all LOW) — lease-vs-queue-depth sizing gap, multi-queue sort node, NOTIFY churn at saturation.

## Refuted / out of scope (appendix)

| Item | Disposition |
|------|-------------|
| — | No findings were refuted in this pass. |
