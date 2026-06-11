---
status: draft
date: 2026-06-11
slug: operator-pages
supersedes: null
superseded_by: null
pr: null
outcome: null
---

# Design: Operator pages — Production checklist, Troubleshooting, Alembic migrations

## Summary

Add three new pages under a new `docs/operations/` directory, surfaced
as a fifth top-level **Operations** section in the mkdocs nav:

1. **Production checklist** — scannable scaffold of one-line items
   covering sizing, subscribers, DLQ, drain, schema, and observability.
   Each item links into the existing reference page that already owns
   the full story.
2. **Troubleshooting** — symptom → likely cause → fix playbook for the
   load-bearing signals operators see: `event=lease_lost`, outbox
   growth, idle latency above `max_fetch_interval`, duplicate
   invocations, rolling-deploy row leaks, test-broker scheduling
   surprises, and a handful of "by design" raises.
3. **Alembic migrations** — what `alembic revision --autogenerate`
   actually produces for `make_outbox_table()` and
   `make_dlq_table()`, including the partial indexes and the DLQ-
   addition-as-second-migration shape; plus a drift-detection-in-CI
   recipe and DLQ partition-retention recipe.

Cross-links from five existing pages point at the relevant operator-page
section. No file moves; no existing prose rewritten.

The B follow-on from
[`docs-landing-and-comparison`](../archived/2026-06-10-docs-landing-and-comparison-design.md)
(non-goal §B). The conventions and IA those PRs landed (#49, #50)
anticipated this — the docs spec noted "Operations could become a fifth
section."

## Motivation

The architecture already exposes the right signals (`event=lease_lost`,
`fetched`/`dispatched`/`acked`/`nacked_*`/`lease_lost`/`dlq_written`
recorder events, three terminal-failure reasons, partial-index
predicates that are load-bearing for fetch performance, a
load-bearing dispatch-shutdown race guard, etc.). The reference pages
document each *in place*, but **an operator deploying for the first time
must assemble the checklist from five pages**:

- `subscriber.md § Connection budget` — engine pool sizing.
- `subscriber.md § Slow handlers — dedicated queue` — `lease_ttl_seconds`
  vs handler P99 trade-off.
- `dlq.md § Metric: dlq_written` — alerting on `nacked_terminal` vs
  `dlq_written` divergence.
- `dlq.md § Retention` — partition + cron-prune pattern.
- `observability.md § Consume vs publish label set` — PromQL playbook
  queries, including the "lease_ttl_seconds is too low" operator
  signal.
- `schema-validation.md § Where to call it` — `/health` vs startup
  hook reasoning.

That assembly is exactly the cost the Production Checklist removes.

The **Troubleshooting** page is the matching artifact for incident
response. The library emits structured `extra={"event": ..., "phase":
..., "row_id": ..., "queue": ..., "deliveries_count": ...}` fields on
WARNING-level logs (the lease-lost path is the canonical example). The
operator's log aggregator surfaces these. Today there is no page to
link from those WARNINGs that says "this means your `lease_ttl_seconds`
is too low for the handler's P99 — see §X for the tuning guide."

The **Alembic migrations** page is the only piece without an existing
home in the docs. The Basic-usage page says "the package never creates
or migrates — that's Alembic's job" but never shows what Alembic
should generate. A literal autogenerate-output sample is ~30 lines and
saves the new adopter a half-hour of figuring out which indexes to
include, what the partial-index predicates look like, and how the
`String(64)` columns are declared.

## Non-goals

Deliberately *not* covered here; each is a candidate follow-on:

- **Migration-recipe regression tests** in `tests/integration.py`. The
  spike below produces the literal autogenerate output once; an
  integration test that runs migrations against the postgres fixture
  and asserts `validate_schema()` is clean would pin it against drift.
  Strong follow-up, but adds test-suite scope; deferred.

- **Performance / benchmarking page.** No representative public data to
  share. A "what throughput can I expect?" page that hedges everything
  is worse than no page.

- **Incident postmortem template.** The library has no customer-incident
  track record yet. A template without examples is just bureaucracy.

- **Promoting `planning/architecture/` content into user-facing docs.**
  Several `architecture/*.md` deep-dives (relay, timers, DLQ, drain,
  metrics, test broker) would inform operators too, but they currently
  also encode design rationale not relevant to operators. Promotion
  would require splitting each. Separate spec.

- **Runbook generator from frontmatter.** Three hand-maintained pages
  are fine at the operator-page-count we have; automate later if it
  grows.

- **Rewriting any existing prose.** This spec adds pages and links into
  them. No content currently in `docs/usage/` or
  `docs/introduction/` is rewritten; the existing reference pages stay
  the authoritative source on their topic.

## Design

### 1. New `docs/operations/` directory + nav section

Create `docs/operations/` and add a fifth nav section to `mkdocs.yml`:

```yaml
nav:
  - Overview: index.md
  - Getting started: ...
  - Concepts: ...
  - Guides: ...
  - Reference: ...
  - Operations:
      - Production checklist: operations/checklist.md
      - Troubleshooting: operations/troubleshooting.md
      - Alembic migrations: operations/alembic.md
```

Material theme's expanded sidebar handles five sections cleanly — the
section count is comparable to SQLAlchemy 2.0's docs site. The four
existing sections from the `docs-landing-and-comparison` PR are
unchanged.

The `docs/index.md` decision-tree table is updated with one new row
pointing at the checklist:

| If you want to… | Start at |
|---|---|
| Deploy to production safely | [Production checklist](operations/checklist.md) |

(Inserted before the existing "Install and write the first publisher /
subscriber" row.)

### 2. Production checklist (`docs/operations/checklist.md`)

Scannable scaffold. Each item is one to two lines plus a link into the
existing reference page that owns the full story. **No new prose for
the underlying decisions** — the page exists to make sure operators
don't *miss* the references, not to replace them.

Sections, in order:

**Sizing**

- [ ] **Engine pool ≥ `Σ subs × (max_workers + 1)`** — every
  subscriber holds `max_workers + 1` SQLAlchemy connections
  (one writer per worker + one fetch) plus one raw asyncpg
  connection for LISTEN. Sub-budget formula in [Subscriber §
  Connection budget](../usage/subscriber.md#connection-budget).
- [ ] **Postgres `max_connections` ≥ `replicas × Σ subs × (max_workers + 1)`**
  — the formula is per-process; rolling deploys multiply it.
  Failure mode: pods refuse with `FATAL: too many connections`.

**Subscribers**

- [ ] **`lease_ttl_seconds` > handler P99 with margin** — otherwise
  healthy in-flight handlers race their own lease expiry. The
  lease cutoff is server-side `make_interval(...)`, immune to
  clock skew. Tuning: [Subscriber § Slow handlers — dedicated
  queue](../usage/subscriber.md#slow-handlers-dedicated-queue).
- [ ] **Slow handlers segregated** onto their own subscriber with a
  taller `lease_ttl_seconds`. Don't raise it globally — that
  delays reclaim of *actually* stuck rows everywhere.
- [ ] **`max_deliveries` set** (or knowingly unbounded). Defaults to
  unbounded; pair with a non-`NoRetry()` retry strategy or wedge-
  prone handlers can replay forever.
- [ ] **Retry strategy chosen.** Default
  `ExponentialRetry(initial=1, multiplier=2, max=300, attempts=10,
  jitter=0.2)` is fine for most. Opt into `NoRetry()` explicitly
  for an audit feed.

**DLQ**

- [ ] **`dlq_table=` configured** — opt-in but recommended for any
  service where terminal failures need forensic recovery.
- [ ] **Alert on `nacked_terminal` rate vs `dlq_written` divergence**
  — persistent divergence means either DLQ schema drift (CTE
  rolls back) or `lease_ttl_seconds` too low. See [DLQ § Metric:
  dlq_written](../usage/dlq.md#metric-dlq_written).
- [ ] **DLQ retention plan.** Partition by `failed_at` + cron-drop
  old partitions, or a simple `DELETE … WHERE failed_at <
  interval` cron for low volume.

**Drain & lifecycle**

- [ ] **`graceful_timeout` ≥ handler P99 + margin** — otherwise
  `OutboxSubscriber.stop()` cancels in-flight work and rows are
  reclaimed mid-handler.
- [ ] **K8s `terminationGracePeriodSeconds` ≥ broker
  `graceful_timeout` × parallel-subscriber-count factor** — the
  broker gathers subscriber drains in parallel, but k8s SIGKILLs
  after the grace period regardless.

**Schema**

- [ ] **`/health` calls `validate_schema()`** — opt-in, requires
  `[validate]` extra. Do **not** call at `broker.start()` — that
  would crash-loop on a pending migration. See [Schema validation
  § Where to call it](../usage/schema-validation.md#where-to-call-it).
- [ ] **Outbox `table_name` ≤ ~56 chars** — NOTIFY channel name is
  `outbox_<table_name>`. Postgres' 63-char identifier limit
  silently truncates longer names and `LISTEN/NOTIFY`
  short-circuit degrades to plain polling.

**Observability**

- [ ] **`metrics_recorder` set, native middleware registered, or
  both** — the recommended setup is both. See
  [Observability § Layering](../usage/observability.md#layering-middleware-seam-vs-recorder-seam).
- [ ] **Alert on `lease_lost` rate** — non-zero means
  `lease_ttl_seconds < handler P99` for at least one subscriber.
- [ ] **`LISTEN/NOTIFY` fallback warning checked at startup** — if
  the asyncpg connection fails (driver missing, permission error),
  the subscriber logs once and falls back to polling. Operator
  silently lives with up-to-`max_fetch_interval` idle latency
  otherwise.

### 3. Troubleshooting (`docs/operations/troubleshooting.md`)

Symptom → likely cause → fix, with a table-of-contents table at the
top for fast jump:

| Symptom | Likely cause |
|---|---|
| `event=lease_lost` recurring in logs | Handler P99 > `lease_ttl_seconds` |
| Outbox row count grows + `lease_lost` spike | DLQ CTE failing (DLQ schema drift) |
| Outbox row count grows, no `lease_lost` | Fetch loop not running, or rows future-dated |
| Idle dispatch latency > `max_fetch_interval` | LISTEN setup failed → polling fallback |
| Subscriber blocks at `broker.start()` | Engine pool exhausted on writer-connection checkout |
| Duplicate handler invocations | Lease expired before handler returned, or handler not idempotent |
| Rolling deploy leaks rows | `graceful_timeout` < handler P99, or k8s grace too short |
| `activate_in` / `activate_at` fires immediately in tests | `TestOutboxBroker(run_loops=False)` ignores scheduling |
| `AckPolicy.ACK_FIRST` raises `ValueError` at registration | By design (would defeat outbox reliability) |
| `OutboxResponse(...)` + foreign-publisher decorator gets nacked | By design (dual-fire footgun, raised via dispatch overrides) |
| `validate_schema()` raises `ImportError` | `[validate]` extra not installed |

Each row is a `##` subsection below, in the same order. Per subsection:

- **Symptom** — what the operator sees, exactly. Log line, metric shape,
  user complaint.
- **Likely cause** — the load-bearing invariant or knob that produced it.
- **Diagnose** — the command, metric, or log query that confirms.
- **Fix** — the knob to turn or code change to make.
- **Reference** — link into the relevant section of the existing
  reference pages.

Example (lease_lost):

> ### `event=lease_lost` recurring in logs
>
> **Symptom.** WARNING-level logs with structured field `event=lease_lost`,
> typically with `phase=terminal` or `phase=retry`. One per affected row.
>
> **Likely cause.** The subscriber's `lease_ttl_seconds` is shorter than
> the handler's P99 duration. A handler took longer than the lease,
> another fetch reclaimed the row mid-flight, and the original handler's
> terminal `DELETE`/`UPDATE` matched zero rows.
>
> **Diagnose.** Grep for `event=lease_lost` over the last hour and
> compare the rate against `dispatched`. A non-zero baseline rate
> (rather than occasional spikes) confirms TTL is the issue.
>
> **Fix.** Raise `lease_ttl_seconds` for the affected subscriber, OR
> segregate slow work onto its own subscriber with a taller TTL
> (recommended — see [Subscriber § Slow
> handlers](../usage/subscriber.md#slow-handlers-dedicated-queue)).
> Pick TTL > handler P99 with margin for clock-skew tolerance.

All ten symptoms get the same five-field shape. Total page length ~400
lines.

### 4. Alembic migrations (`docs/operations/alembic.md`)

Sections:

**4a. Initial migration.** What `alembic revision --autogenerate`
produces against a `MetaData` containing only `make_outbox_table()`.

The spec phase includes a **spike**: run autogenerate against a clean
postgres + `make_outbox_table(metadata, table_name="outbox")`, capture
the literal output, paste it into this section verbatim under
"What you get" and annotate inline why each piece exists. The spike is
done by the spec author *before* writing this section; the resulting
sample lives in the spec body as the canonical reference (so the plan
author doesn't have to re-run it).

What we expect autogenerate to emit, based on the model declarations
in `src/faststream_outbox/store/schema.py`:

- `op.create_table("outbox", ...)` with all columns and types from
  `make_outbox_table`.
- Three partial indexes:
  - `(queue, next_attempt_at) WHERE acquired_token IS NULL` — fetch
    Branch A.
  - `(queue, acquired_at) WHERE acquired_token IS NOT NULL` — fetch
    Branch B (expired-lease reclaim).
  - Unique `(queue, timer_id) WHERE timer_id IS NOT NULL` — timer dedup.

The annotation explains that **each partial index's predicate is
load-bearing** — Postgres only uses the index when the query implies
the predicate, and the fetch CTE's WHERE clause is written to do
exactly that. An operator who drops a "redundant-looking" index makes
the CTE fall back to seq-scan as the table grows.

**4b. Adding the DLQ after the fact.** When you introduce
`dlq_table=make_dlq_table(metadata)` to your `MetaData` later,
autogenerate produces a second migration:

- `op.create_table("outbox_dlq", ...)` with the columns described in
  [DLQ § Schema reference](../usage/dlq.md#schema-reference).
- Single non-unique index `(queue, failed_at)`.

This is purely additive — no `op.alter_table` against the outbox
table itself. The existing outbox path stays bit-for-bit identical;
only the terminal-flush statement changes to the CTE shape (a runtime
decision driven by `OutboxBroker.dlq_table`, not a schema change).

**4c. Drift detection in CI.** A small standalone script using
`validate_schema()`:

```python
import asyncio
from faststream_outbox import OutboxBroker, make_outbox_table

async def main() -> None:
    broker = OutboxBroker(engine, outbox_table=outbox_table)
    await broker.validate_schema()

asyncio.run(main())
```

Run after `alembic upgrade head` in CI; non-zero exit on drift.

The page explains why this is **opt-in for `/health`** and not always-
on at startup: a running migration plus an always-on validator races —
operators must be able to roll forward a new schema version without
spinning every pod into a crash loop. The drift check belongs in CI
between `alembic upgrade head` and "deploy".

**4d. DLQ retention via partition drop.** A walkthrough of converting
the DLQ from a plain table to a partitioned-by-`failed_at` shape, with
the literal Alembic ops for:

- Renaming the existing table out of the way.
- Creating the partitioned parent with `PARTITION BY RANGE (failed_at)`.
- Creating initial partitions.
- Copying rows from the old table into the partitions.
- A monthly cron script (raw SQL, not Alembic) that creates next
  month's partition and drops the partition older than the retention
  window.

This is the operator-facing version of the
[DLQ § Retention](../usage/dlq.md#retention) paragraph, which today
gestures at the pattern without showing the SQL.

### 5. Cross-links from existing pages

Each existing page that the operator pages link *from* gets a one-line
"see also" callout pointing at the relevant operator-page section.
Symmetry with the cross-links the `docs-landing-and-comparison` PR added
into the Comparison page.

| Existing page section | New callout |
|---|---|
| `subscriber.md § Connection budget` | "Operator-side: [Production checklist § Sizing](../operations/checklist.md#sizing)." |
| `subscriber.md § Slow handlers — dedicated queue` | "See also [Troubleshooting § event=lease_lost](../operations/troubleshooting.md#event-lease_lost-recurring-in-logs)." |
| `dlq.md § Metric: dlq_written` | "Operator playbook: [Production checklist § DLQ](../operations/checklist.md#dlq)." |
| `dlq.md § Retention` | "Step-by-step: [Alembic migrations § DLQ retention via partition drop](../operations/alembic.md#dlq-retention-via-partition-drop)." |
| `schema-validation.md § Where to call it` | "CI recipe: [Alembic migrations § Drift detection in CI](../operations/alembic.md#drift-detection-in-ci)." |

Five callouts. No prose rewritten.

## Operations

None — fully in-repo. The mkdocs deploy workflow re-runs on push to
`main` whenever `docs/**` or `mkdocs.yml` changes, both of which this
spec triggers. The new URLs become available immediately:

- `https://faststream-outbox.modern-python.org/operations/checklist/`
- `https://faststream-outbox.modern-python.org/operations/troubleshooting/`
- `https://faststream-outbox.modern-python.org/operations/alembic/`

## Out of scope (repeat list)

Already named under Non-goals; repeated here for grep:

- Migration-recipe regression tests in `tests/integration.py`
- Performance / benchmarking page
- Incident postmortem template
- Promoting `planning/architecture/` content into user-facing docs
- Runbook generator from frontmatter
- Rewriting any existing prose

## Testing

Content + nav config; correctness is checked by:

- `just docs-build` (the new `mkdocs build --strict` target) passes
  locally and in the deploy workflow. Catches broken cross-links from
  the five existing-page callouts and from inside the new pages.
- `just lint` passes (markdown EOF + YAML formatting on
  `mkdocs.yml`).
- Spot-check on PR preview that all eight sub-sections of the
  Production checklist render with link targets, that the
  Troubleshooting TOC table jumps to the right sub-section anchors,
  and that the Alembic autogenerate sample renders inside its code
  block.
- The Alembic spike output (captured during the spec phase below) is
  consistent with the model declarations in
  `src/faststream_outbox/store/schema.py` — checked by re-running
  autogenerate once at the start of the implementation plan, before
  pasting into `alembic.md`, against a fresh postgres.

## Risk

- **Alembic autogenerate output drifts** between when the spec spike
  captured it and when an operator runs it. SQLAlchemy and Alembic
  evolve their autogenerate diffs over time. Mitigated by the
  follow-up regression test (out of scope here but called out as the
  natural next step). For now: the spike output is captured as a
  *representative* sample; the page calls that out so an operator
  knows to compare to their own autogenerate.

- **Checklist becomes stale** as subscriber options, default values,
  or new metric events ship. Mitigated by item-level cross-links: when
  someone changes `lease_ttl_seconds` default, they touch
  `subscriber.md § Slow handlers`, the docs review surfaces the
  cross-link, and they update the checklist line at the same time.
  This is the same hygiene that `planning/architecture/` already
  depends on.

- **Troubleshooting page becomes prescriptive of fixes that mask
  underlying problems** (e.g. "raise `lease_ttl_seconds`" when the
  real issue is that the handler is doing too much work). Mitigated by
  always linking each fix back to the design rationale in the
  reference / concept page — the operator sees the principle, not just
  the knob.

- **Nav grows to six sections later** (Operations + a hypothetical
  Recipes, Migration guides, etc.). Material handles six sections, but
  the sidebar starts feeling busy. Acceptable risk; revisit if it
  happens.

- **`docs/operations/` collides with mkdocs-material reserved paths.**
  None known; Material's reserved paths are theme partials under
  `overrides/`. Confirmed by visual inspection of existing
  `mkdocs.yml` and the lack of any `overrides:` block. Spike: trial
  build with one empty placeholder file catches a collision at
  `mkdocs build --strict` time.
