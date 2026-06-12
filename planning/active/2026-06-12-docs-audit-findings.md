---
date: 2026-06-12
scope: docs/ (22 user-facing mkdocs pages)
status: triage
---

# Docs audit findings — 2026-06-12

Static audit of all 22 pages under `docs/` on four lenses: code-sample
correctness, factual drift vs the implementation, internal consistency +
link integrity, and writing quality / gaps. Six parallel batch reviews
plus an inline mechanical pass (`mkdocs build --strict`, full
link/anchor sweep, cross-page consistency of defaults / event names /
formulas). **Every bug-severity finding below was independently
re-verified against source before inclusion** — none are unconfirmed
agent assertions.

Severity tiers:

- **bug** — the doc says something false: a snippet that raises if
  copied, a metric/kwarg/attribute that doesn't exist, a claim the code
  contradicts.
- **inaccuracy** — stale or misleading, but not flatly broken.
- **improvement** — clarity, gaps, structure.

## Summary table (bugs + inaccuracies)

| ID | Sev | Page(s) | One-liner |
|---|---|---|---|
| B1 | bug | relay.md:31,89 | `OutboxBroker(engine=engine)` missing required `outbox_table` → `TypeError` |
| B2 | bug | relay.md:62 | `OutboxRouter(engine=engine)` missing required `outbox_table` → `TypeError` |
| B3 | bug | testing.md:40,89; timers.md:123 | `broker.fake_client` doesn't exist — it's a `TestOutboxBroker` harness attribute, and the snippets never bind the harness |
| B4 | bug | router.md:59 | `from faststream_outbox import OutboxRoute` → `ImportError` (not exported from root) |
| B5 | bug | fastapi.md:53-61 | `broker: OutboxBroker` annotation in an HTTP route → 422 (FastAPI-aware `Context` needs the internal `context__` header, present only in subscriber dispatch) |
| B6 | bug | publisher.md:131-137 | Handler annotated with plain `OutboxMessage` class instead of the `annotations` alias — not context-resolved |
| B7 | bug | subscriber.md:185-195 | Retry-subclass example overrides nonexistent `get_next_attempt_at`; protocol method is `get_next_attempt_delay` — override is dead code |
| B8 | bug | subscriber.md:136-144 | MANUAL-ack example's `msg` param unannotated — message never injected, `msg.ack()` can't work |
| B9 | bug | add-kafka-relay.md:233-234 | "row stays in the table until Kafka acks" — false under the tutorial's own config; default bounded retry deletes the row after ~13-14 min of outage |
| B10 | bug | setup-prometheus-opentelemetry.md:192-203 | Both-seams recipe puts middleware + recorder on one registry → `Duplicated timeseries` raises at broker construction |
| B11 | bug | observability.md:100 | PromQL alert uses nonexistent `faststream_outbox_nacked_terminal_total` (real: `faststream_outbox_terminal_total`) — alert never fires |
| B12 | bug | observability.md:57,60 | Event-catalog rows wrong: `nacked_retried` lists nonexistent `attempts_count`, omits `next_delay_seconds`; `published` lists nonexistent `destination`/`payload_size_bytes`, fires pre-commit and on error too |
| B13 | bug | checklist.md:66-69 | Claims Postgres "silently truncates" long channel names and degrades to polling; `make_outbox_table` actually raises `ValueError` at build time (and limit is bytes, not chars) |
| B14 | bug | troubleshooting.md:95-105; checklist.md:79-83 | Claims a WARNING is logged when LISTEN falls back for missing driver / non-asyncpg URL — those two causes return silently with no log |
| I1 | inacc | how-it-works.md:62-78 | Illustrative fetch CTE diverges from the real query: naive OR form (the exact shape the code avoids for partial-index use), `ORDER BY id` vs `(next_attempt_at, id)`, missing `deliveries_count` increment |
| I2 | inacc | how-it-works.md:47-50; timers.md:76-78 | "NOTIFY skipped when `activate_in`/`activate_at` set" — actually skipped only when *genuinely future-dated*; past `activate_at` still notifies |
| I3 | inacc | installation.md:43-48 | Base install ships no async Postgres driver at all; quickstart DSNs hit `ModuleNotFoundError`, not the implied graceful polling fallback |
| I4 | inacc | add-kafka-relay.md:29-31 vs 199-200 | Step 1 says the DOCKER listener is for the Step 5 console consumer, but Step 5 connects via the HOST listener (`kafka:9092`); works only because the consumer is exec'd inside the broker container |
| I5 | inacc | add-kafka-relay.md:17-24 | "Postgres is already running from Tutorial 1" — Tutorial 1's cleanup stops a `--rm` container, destroying it and its data; reader must redo T1 steps 2+4 |
| I6 | inacc | subscriber.md:67 | `min_fetch_interval` described as "floor used when the queue has work" — actually the idle-backoff base (jittered ±50%, can land below it) and the queue-full wait; no sleep at all while fetches return rows |
| I7 | inacc | subscriber.md:201-202; troubleshooting.md:111-119 | "`broker.start()` blocks on pool checkout" — `start()` only schedules tasks; an undersized pool stalls the loops with repeating ERROR logs, dispatch silently starves |
| I8 | inacc | dlq.md:144 | `exception_type` on `dlq_written` is always emitted (value `None` when no exception), not "omitted"; custom recorders see the key |
| I9 | inacc | dlq.md:188-210 | Self-contained-looking snippet calls `MetaData()` without `from sqlalchemy import MetaData` → `NameError` |
| I10 | inacc | schema-validation.md:7-10 | Omits the second validation pass over the DLQ table when `dlq_table` is configured — contradicts dlq.md:124-128 |
| I11 | inacc | publisher.md:117-118 | "same session that owns the inbound row's terminal write" — terminal write runs on the worker's autocommit connection, not any user session |
| I12 | inacc | testing.md:39-40 | "`_FakeRow` dicts" — `_FakeRow` is a dataclass (attribute access is why the later snippets work) |
| I13 | inacc | fastapi.md:120-122 | "set middlewares on the broker before mounting" not actionable — the router builds the broker itself; `OutboxRouter(middlewares=[...])` is the real path |
| I14 | inacc | setup-prometheus-opentelemetry.md:140-144 | Prose says `broker_middlewares=[...]`; the constructor kwarg is `middlewares` (the page's own code block is correct) |
| I15 | inacc | observability.md:54-59 | Remaining catalog rows drift: `dispatched` omits `deliveries_count`+`size_bytes`; `acked` omits `deliveries_count`, `duration_seconds` is always present; `nacked_terminal` missing situational `duration_seconds`; `lease_lost` omits `subscriber` and the retry-phase emission |
| I16 | inacc | instrumentation-seams.md:42-64,90,93 | "Four events" double-counts: the "empty-fetch idle counter" is the same `fetched` event with `count=0`; real count is three |
| I17 | inacc | comparison.md:105-107 | Lists "channel name too long" as a NOTIFY-loss mode — unreachable through this package (`make_outbox_table` raises `ValueError` first) |
| I18 | inacc | subscriber.md:207; troubleshooting.md:127-129; checklist.md:14-16 | `max_connections ≥ replicas × Σ subs × (max_workers + 1)` omits the per-subscriber raw asyncpg LISTEN connection — true server-side footprint is `max_workers + 2`; checklist contradicts its own bullet two lines up |
| I19 | inacc | alembic.md:23,86 | Snippets render `astext_type=Text()` unqualified — real autogenerate emits `sa.Text()`; pasted as-is → `NameError` |
| I20 | inacc | alembic.md:68-70 | "`validate_schema()` will refuse to start a service" — it never runs at `broker.start()`; it raises only where *you* call it (health probe / CI) |
| I21 | inacc | observability.md:20,30-31 | "the subscriber's six emission points" — seven with `dlq_written` (instrumentation-seams.md words it correctly) |
| I22 | inacc | setup-prometheus-opentelemetry.md:126-130 | OTel instrument list omits `messaging.publish.messages` and all three outbox-specific instruments (`fetch.batches`, `lease_lost`, `dlq_written`) |

## Bug details

### B1/B2 — relay.md constructs brokers without `outbox_table`

`relay.md:31` and `:89` have `OutboxBroker(engine=engine)`; `:62` has
`OutboxRouter(engine=engine)`. Both signatures declare keyword-only
`outbox_table: Table` with no default (`broker.py:125-129`,
`fastapi/router.py:67-73`). Copying any of the three samples raises
`TypeError` at construction. Every other docs page passes it.
**Fix:** `OutboxBroker(engine, outbox_table=outbox_table)` (declare via
`make_outbox_table` or mark elided, as other pages do).

### B3 — `broker.fake_client` doesn't exist

`testing.py:541` sets `self.fake_client` on the **`TestOutboxBroker`
harness**; `_patch_broker` only swaps
`broker.config.broker_config.client`, never attaches `fake_client` to
the broker. Upstream `TestBroker.__aenter__` returns the *broker*, so
`async with TestOutboxBroker(broker):` (the docs' shape) leaves no name
bound to the harness at all — `broker.fake_client.rows` raises
`AttributeError`. The repo's own tests bind the harness
(`tests/test_fake.py:103` — `test_broker.fake_client.rows`).
Sites: `testing.md:40-41`, `testing.md:88-89`, `timers.md:123-124`.
**Fix:** bind the harness (`tb = TestOutboxBroker(broker)` /
`async with tb: ...`) and inspect `tb.fake_client.rows`. Also fix the
stale source comment `testing.py:71` ("``broker.fake_client.dlq_rows``").

### B4 — `OutboxRoute` not importable from package root

`router.md:59`: `from faststream_outbox import OutboxRoute, OutboxRouter`.
Root `__init__.py` imports/exports only `OutboxRouter`; no `OutboxRoute`
in `__all__`. `ImportError` as written.
**Fix:** `from faststream_outbox.router import OutboxRoute` — or export
it from the root (it's presented as public API).

### B5 — FastAPI quickstart injects broker annotation into an HTTP route

`fastapi.md:53-61` uses `broker: OutboxBroker` in `@router.post("/orders")`.
The FastAPI-aware `Context` resolves through a **required** header
`params.Header(alias="context__")` (faststream
`_internal/fastapi/context.py`), supplied only by FastStream's fake
request during *subscriber* dispatch. A real HTTP POST has no such
header → FastAPI returns 422. The annotation shortcuts work only inside
`@router.subscriber` handlers.
**Fix:** obtain the broker in HTTP routes via `router.broker` (closure
or small `Depends` provider); keep annotation shortcuts to subscriber
handlers and say so explicitly.

### B6 — publisher.md chained example uses the plain `OutboxMessage` class

`publisher.md:131`: `from faststream_outbox import OutboxMessage` then
`msg: OutboxMessage` as a handler param. The root export is the plain
`StreamMessage` subclass (`message.py:154`); the context-resolved alias
lives in `annotations.py:41`. A plain-class annotation is treated by
FastDepends as the expected body type — the message is never injected.
The repo's tests import the alias for handler params (`test_unit.py:35`).
**Fix:** `from faststream_outbox.annotations import OutboxMessage` (the
page's own "Annotated handler params" section already does this).

### B7 — retry-subclass example overrides a method that doesn't exist

`subscriber.md:185-195` defines `get_next_attempt_at` and calls
`super().get_next_attempt_at(...)`; the prose repeats the name. The
protocol method is `get_next_attempt_delay` (`retry.py:20,38,60`;
dispatcher calls it at `message.py:99`). As written, the override never
runs — the inherited strategy retries *every* exception, silently
defeating the example's stated purpose ("retry only on transient
errors"); if the misnamed method were called, `super()` would
`AttributeError`.
**Fix:** rename method, `super()` call, and prose to
`get_next_attempt_delay`.

### B8 — MANUAL-ack example's `msg` is unannotated

`subscriber.md:136-144`: `async def handle(msg, body: dict)`. FastStream
injects the message only via the `Context("message")` annotation; a bare
`msg` is resolved as a decoded-body field. The project's own MANUAL test
annotates it (`test_unit.py:2738-2741`).
**Fix:** `async def handle(msg: OutboxMessage, body: dict)` with
`from faststream_outbox.annotations import OutboxMessage`.

### B9 — tutorial durability claim false under its own config

`add-kafka-relay.md:233-234`: the row "stays in the table until Kafka
actually acks the publish." The tutorial configures no `retry_strategy`
and no `dlq_table`, so the default
`ExponentialRetry(..., max_attempts=10)` applies
(`registrator.py:32-37`); exhaustion returns `None` (`retry.py:46`) →
`to_delete=True`, `terminal_failure_reason="retry_terminal"`
(`message.py:107-110`) → `delete_with_lease` removes the row with no
archive. With the default schedule a Kafka outage longer than ~13-14
minutes deletes the row permanently.
**Fix:** qualify — "stays in the table for the duration of the retry
budget (10 attempts by default); configure `retry_strategy` /
`dlq_table` for longer outages."

### B10 — both-seams recipe shares one Prometheus registry

`setup-prometheus-opentelemetry.md:192-203` registers
`OutboxPrometheusMiddleware(registry=REGISTRY, ...)` **and**
`PrometheusRecorder(registry=REGISTRY, ...)`. Both create identically
named `faststream_*` collectors; `PrometheusRecorder.__init__` registers
unconditionally (`metrics/prometheus.py:126-131`), so `prometheus_client`
raises "Duplicated timeseries in CollectorRegistry" at broker
construction. The project's own test states it:
`test_middleware_prometheus.py:195-198` — "middleware and recorder must
use *separate* registries … Sharing one registry would raise on
construction."
**Fix:** two registries (or distinct `metrics_prefix`) + how to expose
both; warn that pairing both seams double-counts consume/publish series.

### B11 — alert query targets a metric that doesn't exist

`observability.md:100`: `rate(faststream_outbox_nacked_terminal_total[5m])`.
The adapter's counter is `f"{p}_outbox_terminal_total"` →
`faststream_outbox_terminal_total` (`metrics/prometheus.py:194-199`).
The DLQ-divergence alert as written matches no series and never fires —
worst kind of monitoring bug.
**Fix:** `rate(faststream_outbox_terminal_total[5m]) - rate(faststream_outbox_dlq_written_total[5m]) > 0`.

### B12 — event-catalog rows for `nacked_retried` and `published` are wrong

`observability.md:57`: `nacked_retried` lists always-tag
`attempts_count` — never emitted. Actual: `queue`, `subscriber`,
`deliveries_count`, `duration_seconds`, `next_delay_seconds` (the tag
the row exists to convey, missing from the doc), situational
`exception_type` (`usecase.py:529,541-545`).
`observability.md:60`: `published` lists `destination` and
`payload_size_bytes` — neither exists; actual tags are `queue`,
`status`, `count`, `size_bytes`, `duration_seconds` (+ `exception_type`
on error) (`producer.py:90-115`). `destination` is a Prometheus *label*
the adapter derives from `queue`. "Fired by: Producer INSERT committed"
is also wrong twice: fires after the INSERT *executes* (pre-commit, on
the caller's session) and also on the error path with `status="error"`.
**Fix:** correct both rows; the accurate catalog already exists at
`metrics/__init__.py:11-36` — sync the table to it.

### B13 — checklist invents a silent-truncation failure mode

`checklist.md:66-69` says long table names are "silently truncated" and
NOTIFY "degrades to plain polling". `make_outbox_table` raises
`ValueError` when `outbox_<table_name>` exceeds 63 **bytes**
(`schema.py:50-57`) — there is no silent path, and the limit is bytes,
not chars.
**Fix:** reword to "checked at table-build time — `make_outbox_table`
raises `ValueError` past 63 bytes" (or drop the item; the guard makes it
un-forgettable). Related: I17 (comparison.md lists the same unreachable
scenario).

### B14 — "check logs for a WARNING" for failure modes that log nothing

`troubleshooting.md:95-105` (and `checklist.md:79-83`) claim the LISTEN
fallback logs a WARNING for: missing asyncpg, non-asyncpg engine URL, or
permission failure. The first two return silently —
`usecase.py:353-354`: `if _asyncpg is None or "asyncpg" not in
(engine.url.drivername or ""): return None` — no log. The WARNING fires
only when `asyncpg.connect`/`add_listener` raises (`usecase.py:363-371`).
The diagnose step sends operators hunting for a log line that doesn't
exist in two of the three listed scenarios.
**Fix:** split causes — connection/permission errors log a WARNING;
missing driver / non-asyncpg URL fall back silently (diagnose via the
engine URL). Alternatively (code change): log a one-time INFO on the
silent path.

## Inaccuracy details (abridged — evidence in summary table)

- **I1** `how-it-works.md:62-78` — illustrative CTE shows the naive OR
  (`client.py:190-196` documents why the real query carries each
  partial-index predicate explicitly), orders by `id` instead of
  `(next_attempt_at, id)` (`client.py:198`), omits the
  `deliveries_count` increment (`client.py:209`) that `max_deliveries`
  rests on. Either mirror the real query or label it "simplified" and
  note the index-implying OR shape — the docs elsewhere lean on the
  partial-index story.
- **I2** `how-it-works.md:47-50`, `timers.md:76-78` — NOTIFY skip
  condition is "genuinely future-dated" (`producer.py:136-141,160`);
  a past `activate_at` (recovered idempotency token) still notifies.
- **I3** `installation.md:43-48` — base deps are only `faststream` +
  `sqlalchemy[asyncio]` (`pyproject.toml:11-14`); all example DSNs are
  `postgresql+asyncpg://`. State that an async driver is required;
  scope "falls back to polling" to setups with another async driver.
- **I4/I5** `add-kafka-relay.md` — listener-rationale vs Step 5 command
  mismatch; cross-tutorial continuity break (T1 cleanup destroys the
  `--rm` container T2 assumes). Add a "Before you start" recovery note.
- **I6** `subscriber.md:67` — `min_fetch_interval` is the idle-backoff
  base (jittered ±50%, `usecase.py:93`) and queue-full wait
  (`usecase.py:306`); no sleep while fetches return rows
  (`usecase.py:327-330`).
- **I7** `subscriber.md:201-202`, `troubleshooting.md:111-119` —
  `start()` only schedules tasks (`usecase.py:198-205`); pool exhaustion
  surfaces as repeating reconnect ERRORs + stalled dispatch
  (`usecase.py:440-447`), not a blocked `start()`.
- **I8** `dlq.md:144` — `dlq_written` always emits `exception_type`
  (`None` when absent, `usecase.py:628`); only `nacked_terminal`
  conditionally omits the key (`usecase.py:537-539`).
- **I9** `dlq.md:188-210` — add `from sqlalchemy import MetaData`.
- **I10** `schema-validation.md:7-10` — mention the DLQ pass
  (`client.py:373-375`); currently contradicts dlq.md.
- **I11** `publisher.md:117-118` — terminal write runs on the worker's
  autocommit connection (`usecase.py:413,593`); reword to "same session
  as your domain writes". Same stale phrasing in source
  `_REJECT_RELAY_MSG` (`publisher/usecase.py:36-39`).
- **I12** `testing.md:39-40` — `_FakeRow` is a `@dataclass`
  (`testing.py:46-47`); module docstring `testing.py:4` has the same
  drift.
- **I13** `fastapi.md:120-122` — `OutboxRouter` accepts `middlewares`
  (`fastapi/router.py:78`); the user never constructs the broker, so
  "set them on the broker" is impossible advice.
- **I14** `setup-prometheus-opentelemetry.md:140-144` — kwarg is
  `middlewares` (`broker.py:134`); prose contradicts the page's own
  code block.
- **I15** `observability.md:54-59` — remaining catalog rows drift from
  emission sites (`usecase.py:498-501,529,536-547,611-619,661-668`);
  sync to `metrics/__init__.py:11-36`.
- **I16** `instrumentation-seams.md:42-64,90,93` — "four events" counts
  `fetched` twice (empty fetch = `fetched` with `count=0`,
  `usecase.py:323-326`); merge bullets, drop the duplicate table row.
- **I17** `comparison.md:105-107` — drop the 63-char clause or scope it
  to hand-rolled LISTEN/NOTIFY (unreachable here; see B13).
- **I18** `subscriber.md:207`, `troubleshooting.md:127-129`,
  `checklist.md:14-16` — server-side budget is `max_workers + 2` per
  subscriber when LISTEN is active (raw asyncpg conn,
  `usecase.py:341-372`); checklist counts the LISTEN conn in one bullet
  and omits it from the formula two bullets later. The *pool* formula
  (`max_workers + 1`) is correct.
- **I19** `alembic.md:23,86` — `astext_type=sa.Text()`.
- **I20** `alembic.md:68-70` — validate_schema is opt-in; reword to
  "will fail your `/health` probe / CI gate" (`broker.py:312-314`,
  `client.py:376-378`).
- **I21** `observability.md:20,30-31` — seven emission points with
  `dlq_written` (`usecase.py:621-630`).
- **I22** `setup-prometheus-opentelemetry.md:126-130` — add
  `messaging.publish.messages` and the outbox-specific instruments
  (`metrics/opentelemetry.py:121-151`).

## Improvements

Grouped by page; each is one decision for triage.

**index.md**
- P1 — Documentation section omits several nav pages (instrumentation
  seams, setup how-to, troubleshooting, alembic, both tutorials); sync
  the on-page index with `mkdocs.yml` nav.

**installation.md**
- P2 — document the `all` extra (`pyproject.toml:22`) in the
  combine-extras section.

**how-it-works.md**
- P3 — DLQ snippet uses `MetaData()` without import and undefined
  `engine`; make it a clean fragment or self-contained.
- P4 — "Relay tutorial" link text points at the relay *guide*
  (`usage/relay.md`); rename or retarget to
  `tutorials/add-kafka-relay.md`.

**basic.md**
- P5 — "Run with `faststream run app:app`" never says to save the file
  as `app.py`.

**tutorials**
- P6 — `first-outbox-app.md:20-26` installs `[validate]` but the
  tutorial never exercises it; drop or add a "What's next" pointer.
- P7 — `add-kafka-relay.md:85-88` — use `uv add 'faststream[cli,kafka]'`
  to avoid extras-merge ambiguity across uv versions.

**relay.md**
- P8 — header-propagation snippet uses `msg: OutboxMessage` without
  importing the annotations alias.
- P9 — "Two-broker lifecycle" should mention the built-in safety net
  (WARNING per unstarted foreign broker; relay fails-and-retries until
  started, `broker.py:223-262`).
- P10 — say what happens to the row on `_OutboxConfigError` (nacked and
  retried until config fixed — not lost).
- P11 — `# NotImplementedError at decoration` comment is attached to
  the wrong decorator line; the raise happens at
  `@broker_outbox.publisher(...)` application.

**timers.md**
- P12 — dedup-window caveat: `timer_id` uniqueness covers only live
  rows (partial index, `schema.py:87-93`); after fire/cancel the same
  id inserts fresh — not a permanent idempotency key.
- P13 — first snippet hardcodes body `{"order_id": 1}` but builds
  `timer_id` from an undefined `order`.

**subscriber.md**
- P14 — multi-queue subscribers (`queues: str | list[str]`) are
  undocumented, along with the connection-budget implication and the
  same-queue worker-competition warning (`registrator.py:84-92`).
- P15 — MANUAL fallback: a handler returning without ack/nack/reject is
  rejected terminally (`message.py:140-148`) — deletes the row (or DLQs
  with `failure_reason="rejected"`); operator-relevant surprise.

**publisher.md**
- P16 — "the decorator's static headers" → "the publisher's static
  headers" (the next section stresses it is *not* a decorator).
- P17 — chained example: `Depends`/`get_session`/`AsyncSession` neither
  imported nor defined; add the duplicate-delivery caveat (chained row
  commits with handler txn; inbound DELETE happens after on the worker
  conn; crash between → redelivery → second chained row; suggest
  `timer_id` dedup for non-idempotent chains).

**fastapi.md**
- P18 — "what's not exposed" list incomplete: `OutboxRouter` also lacks
  `dlq_table`, `metrics_recorder`, `routers` (all on
  `OutboxBroker.__init__`). A FastAPI user cannot enable the DLQ or the
  recorder seam through the router at all — document the limitation
  **or treat as a feature gap and forward the kwargs**.
- P19 — "the **same** `AsyncSession` it would in an HTTP route" is
  misreadable as same-instance; reword to "resolved exactly as in an
  HTTP route (fresh per delivery)".
- P20 — quickstart references undefined `OrderIn`/`Order`; once B5 is
  fixed, define stubs or mark illustrative.

**schema-validation.md**
- P21 — caveat: `compare_server_default=False` (`client.py:463`) — a
  green `validate_schema()` does not prove server defaults exist; a
  missing `server_default=now()` on `next_attempt_at` is the silent-
  outage mode the client docstring itself warns about
  (`client.py:434-440`). Same note belongs in alembic.md's drift
  section (P24).

**observability.md**
- P22 — PromQL "per queue" queries have no `sum by (destination)`
  grouping; add it or drop the label.
- P23 — `fetched` "every cycle" footnote: not emitted when the inflight
  queue is full (`usecase.py:304-307`); multi-queue subscribers tag only
  the first queue.

**operations**
- P24 — alembic.md drift section: mention `compare_server_default=False`
  (see P21).
- P25 — checklist.md:32 quotes the default retry with shorthand kwargs
  (`initial=`, `max=`, `attempts=`, `jitter=`) that don't exist;
  copying raises `TypeError`. Use the real names.

## Source-side notes (not docs defects; tiny-change lane)

- `testing.py:528-529` — `TestOutboxBroker` class docstring claims
  future-dated rows "are *not* dispatched" in sync mode; contradicts the
  implementation (`testing.py:428-432`) and both docs pages, which are
  correct. Fix the docstring.
- `testing.py:71` — stale comment "``broker.fake_client.dlq_rows``"
  (see B3).
- `testing.py:4` — module docstring "backed by a list of dicts" (see I12).
- `publisher/usecase.py:36-39` — `_REJECT_RELAY_MSG` repeats the
  "session that owns the inbound row's terminal write" phrasing (see I11).

## Verified clean

- `mkdocs build --strict` passes; all 183 internal links/anchors across
  22 pages resolve (mechanical sweep, attr-list anchors included).
- All eight recorder event *names* in docs match emission sites.
- Documented defaults match source: `max_workers=1`,
  `fetch_batch_size=10`, `min_fetch_interval=1.0`,
  `max_fetch_interval=10.0`, `lease_ttl_seconds=60.0`,
  `max_deliveries=None`, default retry
  `ExponentialRetry(1.0, ×2.0, max 300.0, 10 attempts, jitter 0.2)`,
  `_LAST_EXCEPTION_MAX_CHARS=8192`, ack policy rules (`ACK_FIRST`
  `ValueError`), extras names, `messaging.system="outbox"`, Prometheus
  consume-by-`handler` / publish-by-`destination` labels, OTel
  meter-only claim, Alembic snippets' column/index definitions
  (`outbox_pending_idx`, `outbox_lease_idx`, `outbox_timer_id_uq` +
  partial predicates), DLQ schema table, atomicity CTE SQL, tutorial 1's
  full `psql \d outbox` dump, drain/graceful-timeout claims, test-broker
  sync/loop mode semantics.
- Per-batch coverage logs (every snippet checked) are in the agent
  outputs; nothing was sampled — all 22 pages and all code blocks were
  read.
