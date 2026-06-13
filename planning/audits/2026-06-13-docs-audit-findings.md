---
status: open
date: 2026-06-13
slug: 2026-06-13-docs-audit-findings
scope: docs/ (22 user-facing mkdocs pages)
prs: []
outcome: >
  In progress. All bugs (B1–B5), the three CLAUDE.md source-of-truth
  drifts (C1–C3), and the full inaccuracy tail (I1–I15) are fixed; the
  improvement tail (P1–P18) is pending. Successor to the
  2026-06-12 docs audit (I1–I22, B1–B16) — that pass closed clean; this
  pass re-swept all 22 pages on convenience / readability / consistency /
  factual drift and surfaced a new high-severity DLQ-migration bug plus a
  long inaccuracy/improvement tail. Three items are source-of-truth
  (CLAUDE.md) drift, not docs/ bugs — tracked separately below.
---

# Docs audit findings — 2026-06-13

Deep audit of all 22 pages under `docs/`, prompted by "is it convenient
to users, understandable and readable, any bugs or inconsistencies?" Six
parallel batch reviews over the mkdocs nav clusters (getting-started,
concepts, subscriber/publisher/router reference, dlq/observability/relay,
guides, operations), each applying four lenses — **bug/inaccuracy**
(claim contradicts the code), **inconsistency** (contradicts another doc
or CLAUDE.md / dead link / naming drift), **readability**, **convenience/
gap** — and verifying every factual claim against source. The two
highest-severity findings (DLQ `timer_id` omission; Kafka listener
explanation) were independently re-verified against source before
inclusion.

Severity tiers:

- **bug** — the doc says something false: a snippet that breaks if
  copied, a migration that produces a broken schema, a metric/kwarg that
  doesn't exist, a claim the code contradicts.
- **inaccuracy** — stale or misleading, but not flatly broken.
- **improvement** — clarity, gaps, structure, convenience.

## Headline

`make_dlq_table` grew an 11th column — **`timer_id String(255)`**
(`schema.py:165`) — and the runtime DLQ CTE writes it
(`client.py:322-329`: `RETURNING … timer_id` / `INSERT … timer_id` /
`SELECT … timer_id`). Four docs still describe the 10-column DLQ. Two of
them are **hand-copyable Alembic DDL** (B1, B2): an operator who follows
`operations/alembic.md` builds a DLQ table without `timer_id`, so every
terminal-failure DLQ write fails the INSERT, the CTE rolls back the
DELETE, and the outbox grows with `lease_lost` spikes — the exact symptom
`operations/troubleshooting.md` describes. This is the one finding that
causes a production incident; everything else is polish or local
confusion.

> **Resolved.** B1–B3 and I1 fixed: `timer_id` added to both Alembic DDL
> blocks (`operations/alembic.md`), the partitioned `SELECT *` copy now
> aligns, and `usage/dlq.md` carries the column in both the schema-
> reference table and the atomicity CTE. `mkdocs build --strict` clean.

## Summary table (bugs + inaccuracies)

| ID | Sev | Page(s) | One-liner |
|---|---|---|---|
| B1 | bug | operations/alembic.md:82-94 | DLQ `op.create_table('outbox_dlq')` omits `timer_id` → runtime DLQ INSERT fails, outbox grows (**verified** vs schema.py:165 + client.py:322-329) |
| B2 | bug | operations/alembic.md:178-192 | Partitioned-DLQ `CREATE TABLE outbox_dlq` omits `timer_id`; the `SELECT *` data copy (210-213) then misaligns (**verified**) |
| B3 | bug | usage/dlq.md:81-89 | Atomicity CTE example drops `timer_id` from RETURNING/INSERT/SELECT — contradicts the real statement (**verified**) |
| B4 | bug | usage/basic.md §Full quickstart | ✅ **fixed** — added the `asyncpg` + `faststream[cli]` prereqs, the required table-creation step (dev `metadata.create_all` + Alembic/tutorial pointers), and gated the run line on the table existing |
| B5 | bug | usage/basic.md:60 §4 | ✅ **fixed** — annotated `Order` as the reader's own ORM model / domain write |
| I1 | inaccuracy | usage/dlq.md:55-66 | DLQ "Schema reference" table omits the `timer_id` column (lists 10 of 11) |
| I2 ✅ | inaccuracy | usage/observability.md:61; usage/dlq.md:144 | `exception_type` tag described "always present / `None`"; source **omits the key** for `max_deliveries` and manual `reject()` (usecase.py:728-729). **Fixed** in both observability.md:61 and dlq.md:144 |
| I3 ✅ | inaccuracy | usage/observability.md | "The first eight [PromQL]…" — only **seven** queries precede the DLQ-divergence one. **Fixed** → "first seven" |
| I4 ✅ | inaccuracy | usage/observability.md:67-106 | PromQL series (`faststream_outbox_*_total`) come from `PrometheusRecorder` (recorder seam); page never says so. **Fixed** — playbook lead-in now names `PrometheusRecorder` and contrasts the native middleware |
| I5 ✅ | inaccuracy | usage/observability.md:77-80 | "Handler error rate" `status!="acked"` silently counts `lease_lost` (mapped to `status="error"`, prometheus.py:273-278). **Fixed** — caveat added to the query |
| I6 ✅ | inaccuracy | concepts/comparison.md:86-88 | "strict subset of what a real bus can do" contradicts the bus-gap list two lines above. **Fixed** → "covers a focused subset … while adding outbox-native features a bare bus lacks" |
| I7 ✅ | inaccuracy | concepts/comparison.md | Names private `_RetryStrategyTemplate` in user-facing prose. **Fixed** → public `ExponentialRetry`/`NoRetry` (also cleaned the adjacent mention in subscriber.md) |
| I8 ✅ | inaccuracy | tutorials/add-kafka-relay.md:34-37,206 | Prose "reaches the host listener at `kafka:9092`" vs advertised `HOST://localhost:9092`. **Fixed** — consumer bootstrap → `localhost:9092` and prose now matches the advertised address |
| I9 ✅ | inaccuracy | usage/subscriber.md | `TransientOnly` retry example used a `**kw` swallow, hiding the real signature. **Fixed** — full `get_next_attempt_delay(*, first_attempt_at, last_attempt_at, attempts_count, exception=None)` spelled out |
| I10 ✅ | inaccuracy | usage/subscriber.md | "get_one()/`async for` not supported" never named the exception. **Fixed** — now states both raise `NotImplementedError` |
| I11 ✅ | inaccuracy | usage/router.md | Called `broker._subscribers` a "list" — it's a `WeakSet` (broker.py:127-129). **Fixed** |
| I12 ✅ | inaccuracy | introduction/how-it-works.md | Worker loop "dispatches via the handler" — vague. **Fixed** → names the `dispatch_one` seam |
| I13 ✅ | inaccuracy | introduction/installation.md:26 | "PostgreSQL 12+" while examples + CI use 17; gap unexplained. **Fixed** — notes the features predate 12 and that 17 is what's exercised |
| I14 ✅ | inaccuracy | usage/basic.md §1 | "three indexes the broker needs" omitted the `outbox_lease_ck` CHECK. **Fixed** — constraint now named |
| I15 ✅ | inaccuracy | tutorials/first-outbox-app.md Step 4 | Sample `\d outbox` output omitted the `Check constraints:` block. **Fixed** — block added |

## Improvements (convenience / readability / gaps)

| ID | Page(s) | One-liner |
|---|---|---|
| P1 | usage/subscriber.md | Options table omits `propagate_inbound_headers` (real kwarg, documented only in relay.md) |
| P2 | usage/subscriber.md | Options table omits standard passthrough kwargs (`dependencies`, `parser`, `decoder`, `title_`, `description_`, `include_in_schema`) — or a note that they pass through |
| P3 | usage/subscriber.md | No params/defaults table for `ConstantRetry` / `LinearRetry` / `ExponentialRetry` (`delay_seconds`, `step_seconds`, `max_attempts`, `max_total_delay_seconds` never tabulated) |
| P4 | usage/publisher.md | Never shows the `broker.publisher(...)` signature (`title`/`description`/`schema`/`include_in_schema`) despite the section being about AsyncAPI config |
| P5 | usage/publisher.md | Chained-publishing example uses `Depends(get_session)` (FastAPI-only) on the generic publisher page; show `async with session_factory()` first |
| P6 | index.md | Three observability entries (Guides "Setup…", Reference "Observability", Concepts "Instrumentation seams") with no signposting of how they differ |
| P7 | concepts/instrumentation-seams.md | Missing the recorder **"must not block"** constraint — the top footgun for a custom recorder, and this is where readers decide to use it |
| P8 | usage/observability.md | No bundled-adapter wiring snippet (`from faststream_outbox.metrics.prometheus import PrometheusRecorder` …) even though the PromQL playbook depends on it |
| P9 | concepts/comparison.md:9-29 | "vs. writing your own" opens with a ~20-line semicolon-joined feature wall — convert to a bulleted list |
| P10 | concepts/comparison.md | No at-a-glance decision matrix; a scanner must read six TL;DRs sequentially |
| P11 | concepts/comparison.md:58-62 | CDC verdict leans on an internal "2026-05-07 reassessment" date with no falsifiable detail a reader can follow up on |
| P12 | introduction/how-it-works.md:184-186 | Relay H2 is a stub (heading + one-line blockquote) after substantial sections — reads unfinished |
| P13 | introduction/how-it-works.md | "Handlers must be idempotent" repeated 3× in close proximity — trim to one fuller treatment + cross-refs |
| P14 | usage/testing.md:84-87 | Loop-mode example body is `...  # poll until…` — no runnable `feed()` + `await asyncio.wait_for` snippet |
| P15 | usage/testing.md | Mixes `@pytest.mark.asyncio` (one example) with documented `asyncio_mode="auto"` inconsistently |
| P16 | operations/troubleshooting.md | "Row count grows + lease_lost spike" → Diagnose could name the missing-`timer_id` DLQ migration as a frequent cause (moot once B1/B2 fixed, useful for already-broken deployments) |
| P17 | usage/basic.md:40-46 | Leads the "basic" subscriber example with an unexplained `max_workers=4`; default is 1 — drop it for the first example |
| P18 | usage/relay.md:31,169-174 | Examples use undefined `engine`/`outbox_table`; the anti-pattern snippet uses `OutboxResponse` with no import (hits `NameError` before the intended dispatch-time `RuntimeError`) |

## Source-of-truth drift (CLAUDE.md — not a docs/ bug, flagged for consistency)

The docs are audited *against* CLAUDE.md, so where CLAUDE.md is the
stale one, fixing the doc to match would introduce a bug. Three cases.
**All three resolved** — verified against source (`retry.py:39,66`,
`subscriber/usecase.py:389`, `broker.py:140`) and fixed in CLAUDE.md.

- **C1 — retry method name.** CLAUDE.md "Retry strategies" says
  `get_next_attempt_at(exception, …)`. The actual method is
  `get_next_attempt_delay(*, first_attempt_at, last_attempt_at,
  attempts_count, exception=None)` and returns a *delay*, not a
  timestamp (`retry.py:39,66`). The usage docs use the correct name;
  CLAUDE.md is the outlier. **Fix CLAUDE.md.**
- **C2 — connection-budget formula.** CLAUDE.md "Connection budget" says
  Postgres `max_connections` must cover `replicas × Σ subs ×
  (max_workers + 1)`, yet the same paragraph notes the extra raw asyncpg
  LISTEN connection. `usage/subscriber.md:238-244` and
  `operations/checklist.md:14` correctly use `(max_workers + 2)`
  server-side (pool `max_workers + 1` + the out-of-pool asyncpg conn,
  `usecase.py:389`). CLAUDE.md's `max_connections` line is internally
  inconsistent and under-counts. **Fix CLAUDE.md to `+2`.**
- **C3 — middleware kwarg.** CLAUDE.md "Metrics + native middleware" says
  register via `broker_middlewares=[...]`. The public `OutboxBroker`
  constructor arg is `middlewares` (broker.py:140), forwarded internally
  to `broker_middlewares`. The docs use the correct public name. **Note
  in CLAUDE.md** that the public arg is `middlewares` so a future edit
  doesn't "correct" the docs to the internal name.

## Verified-clean (lens passed — recorded so the next audit needn't re-check)

- **Getting-started code**: all signatures, imports, defaults, the
  13-column `\d outbox` table, index names, and `faststream run app:app`
  match source. Only B4/B5/I13/I14/I15 above.
- **Reference defaults**: `max_workers=1`, `fetch_batch_size=10`,
  `min_fetch_interval=1.0`, `max_fetch_interval=10.0`,
  `lease_ttl_seconds=60.0`, `max_deliveries=None`, default
  `NACK_ON_ERROR`, `ACK_FIRST → ValueError`, and the full default
  `ExponentialRetry(1.0, 2.0, max_delay=300.0, max_attempts=10,
  jitter=0.2)` all correct.
- **Guides**: every pip extra (`[fastapi]`/`[validate]`/`[prometheus]`/
  `[opentelemetry]`/`[all]`), import path, OTel instrument name, timer
  mutual-exclusion / tz rules, `cancel_timer` SQL, `TestOutboxBroker`
  sync/loop semantics, and the FastAPI `dlq_table`/`metrics_recorder`/
  `routers` limitation are stated correctly. No runnable guide example
  fails.
- **DLQFailureReason** literals (`max_deliveries`/`retry_terminal`/
  `rejected`), the recorder event vocabulary, and `make_dlq_table`'s
  *signature* are accurate (only the column *list* in prose/DDL drifts).
- **Links/anchors**: all inter-doc links and `{ #anchor }` targets
  resolve (full sweep across all 22 pages). No dead links.

## Suggested remediation order

1. **B1–B3, I1** together — the `timer_id` DLQ drift is one root cause
   across four locations; fix the DLQ schema story in one pass
   (regenerate the Alembic DDL against the real `make_dlq_table()`), then
   P16 becomes unnecessary.
2. **B4, B5** — make `basic.md`'s quickstart actually runnable (or scope
   it explicitly as snippets and point to the tutorial).
3. **C1–C3** — fix CLAUDE.md drift first so subsequent doc edits have a
   correct source of truth.
4. **I2–I15** inaccuracy tail.
5. **P1–P18** improvement tail (batchable, low-risk).
