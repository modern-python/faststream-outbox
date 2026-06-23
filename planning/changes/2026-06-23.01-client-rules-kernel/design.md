---
status: shipped
date: 2026-06-23
slug: client-rules-kernel
summary: Deduplicate the outbox rules between the real and fake clients — extract the genuinely-pure bits (DLQ projection, scheduling resolution) and co-verify the irreducibly-SQL bits with one contract suite run against both adapters.
supersedes: null
superseded_by: null
pr: 109
outcome: |
  Landed as planned. Pure bits extracted: `_scheduling.py` (activate-args resolution +
  validation, shared by real and fake publish paths) and `_DLQ_PROJECTION` in `schema.py`
  (single source for the outbox→DLQ column mapping). Irreducibly-SQL rules co-verified by
  `tests/test_client_contract.py` — one parametrized module over both adapters (fake
  everywhere, real Postgres auto-skipped). Scope correction during execution: `cancel_timer`
  and `timer_id` insert-dedup are broker/producer concerns, not on `AbstractOutboxClient`, so
  they were excluded from the contract suite; within-batch fetch *return order* is unspecified
  (F2-09), so the suite asserts FIFO *selection* under LIMIT, not return order. Replace-don't-layer
  removed ~220 lines of subsumed per-adapter tests; full suite 543 passed at 100% coverage.
---

# Design: Dedupe the outbox rules between the real and fake clients

## Summary

`AbstractOutboxClient` already has two real adapters: `OutboxClient` (SQL/Postgres)
and `FakeOutboxClient` (in-memory). The transactional-outbox *rules* — row
eligibility, lease expiry, retry timing, scheduling resolution, the DLQ field
projection — are written down **twice**: once as SQLAlchemy expressions in
`client.py`/`producer.py`, once as Python over `_FakeRow` in `testing.py`. The two
copies are kept in parity by hand and by comment (`P9`, `B10`). This change removes
the duplication where the implementations *can* be shared, and replaces hand-parity
with a machine-checked contract where they cannot.

It does **not** try to put all the rules behind "one pure module both adapters
call" — that is impossible for the rules the real adapter delegates to Postgres
(see Non-goals). Instead it splits the rules into two groups and treats each on its
own terms: **share the pure ones, co-verify the SQL ones.**

## Motivation

The architecture review (HTML report, 2026-06-23) flagged `FakeOutboxClient` as a
"second source of truth." Concretely:

- **DLQ projection drift.** `_build_dlq_cte_stmt` (`client.py:335–348`) hand-writes
  the outbox→DLQ column list; `FakeOutboxClient.delete_with_lease`
  (`testing.py:183–196`) hand-writes the same projection as a dict literal. The
  `# P9 parity with the real DLQ CTE` comment (`testing.py:194`) is parity
  maintained by eyeball. Add a DLQ column and you must edit both, in two languages,
  or silently drop an audit field.
- **Scheduling resolution scattered + duplicated.** `_is_future_dated`
  (`producer.py:35`) and `_compute_next_at_client_side` (`broker.py:73`) are pure,
  but live in two files; the real `publish_batch` *inlines* the same client-side
  `next_attempt_at` computation at `producer.py:182` instead of calling the helper.
- **No coupling between the two clients' tests.** The fake is tested in
  `test_fake.py`, the real in `test_integration.py`. Nothing asserts they agree —
  drift surfaces only if a human duplicates each scenario in both suites. The
  clock-handling already differs (real uses DB `now()` via `make_interval`; fake
  uses worker-clock `utcnow()`), so a green fake test can mask a real-path bug.

## Non-goals

- **A single pure "rules kernel" both adapters call.** The real adapter runs
  eligibility (`SELECT … FOR UPDATE SKIP LOCKED`), the lease cutoff, and retry
  `next_attempt_at` *inside Postgres* — for atomicity, partial-index inference, and
  DB-clock authority. A pure `is_eligible(...)` would have exactly one runtime
  consumer (the fake): a one-adapter hypothetical seam, i.e. pure indirection. These
  rules are deliberately left as two implementations and co-verified instead.
- **Reproducing cross-host clock skew in tests.** An in-process contract test cannot
  manufacture DB-vs-worker clock skew. The suite pins *structural* drift; the
  clock-authority subtlety stays a documented invariant.
- **Changing any runtime behaviour.** This is a refactor + test addition. With
  `dlq_table=None` and no scheduling args, every code path stays bit-for-bit
  identical.

## Design

The rules split into two groups, handled differently.

### 1. Shared: the DLQ projection (`schema.py`)

Introduce one declarative projection next to `make_dlq_table`, where the columns are
already defined — locality: a schema change and its projection change become one
edit in one file.

```python
# schema.py — ordered (outbox_col, dlq_col) pairs copied verbatim on archive.
_DLQ_PROJECTION: tuple[tuple[str, str], ...] = (
    ("id", "original_id"),
    ("queue", "queue"),
    ("payload", "payload"),
    ("headers", "headers"),
    ("deliveries_count", "deliveries_count"),
    ("created_at", "created_at"),
    ("timer_id", "timer_id"),
)
# Injected (not copied from the outbox row): failure_reason, last_exception.
```

- `OutboxClient._build_dlq_cte_stmt` builds its `RETURNING` / `INSERT` / `SELECT`
  column lists from `_DLQ_PROJECTION` instead of literal SQL text.
- `FakeOutboxClient.delete_with_lease` builds its DLQ dict from `_DLQ_PROJECTION`
  instead of a literal dict.

The seam is the projection map; the two adapters (SQL CTE, dict) are its consumers.
The `P9`/`B10` hand-parity comments are deleted — parity is now structural.

### 2. Shared: scheduling resolution + activate-args validation (`_scheduling.py`)

New private leaf module (matching the `_time.py` / `_import_checker.py` convention),
depending only on `_time`:

```python
# _scheduling.py
def is_future_dated(activate_in, activate_at, now) -> bool: ...
def resolve_next_attempt_client_side(activate_in, activate_at, now) -> datetime | None: ...
def validate_activate_args(method_name, activate_in, activate_at) -> None: ...
```

- `is_future_dated` and `resolve_next_attempt_client_side` move out of `producer.py`
  / `broker.py`. The real `publish_batch` (`producer.py:175–201`) calls
  `resolve_next_attempt_client_side` instead of inlining it. The fake producer and
  fake client import from `_scheduling`, not `broker`.
- `validate_activate_args` (the mutex + tz subset used by the fakes, currently
  `broker.py:59`) moves here too — colocating "everything about resolving and
  validating activate-args" under one home. `broker.py` shrinks.
- The single-publish real path keeps its server-side `now() + make_interval(...)`
  for `activate_in` (clock-skew safety) — that is not pure and stays in `producer.py`.

### 3. Co-verified: one client contract suite (`tests/test_client_contract.py`)

The irreducibly-SQL rules (eligibility, lease cutoff, retry timing, the NULL-token
guard) stay as two implementations but are pinned by **one parametrized scenario
module** run against both adapters.

- **Parametrization:** `client ∈ {fake, real}`. The fake param runs everywhere; the
  real param uses the existing `pg_engine` fixture and **auto-skips when Postgres is
  unreachable** (same gate `test_integration.py` uses today).
- **Per-adapter harness fixture** (test-only) bridges the surfaces uniformly:
  - `seed_row(**fields)` — fake: `.feed(...)`; real: raw `INSERT` on the table.
  - `open_conn()` — fake: yields `None`; real: `engine.connect()`.
  - exposes the `client` under test.

  Each scenario reads `harness.seed_row(...)`, `await client.fetch(harness.conn, …)`,
  asserts on the observable result — adapter-agnostic. Expectations are
  hand-specified (not computed from a shared function), so neither adapter passes
  trivially against itself.

- **Contract the suite pins:**
  - `fetch`: claims unleased; skips future-dated (`next_attempt_at > now`); reclaims
    expired lease; skips fresh lease; FIFO order `(next_attempt_at, id)`; respects
    `limit`; filters by queue set.
  - claim side-effects: `acquired_token` / `acquired_at` set, `deliveries_count`
    incremented.
  - `delete_with_lease`: deletes iff token matches; no-op on mismatch / NULL token;
    DLQ row materialized via `_DLQ_PROJECTION` when DLQ configured.
  - `mark_pending_with_lease`: reschedules iff token matches; clears lease; sets
    attempts / timestamps.
  - `cancel_timer`: drops an unleased timer row; refuses a leased one.
  - `timer_id` dedup: re-insert of the same `(queue, timer_id)` is a no-op.

**Replace, don't layer:** scenarios now covered by the contract suite are removed
from `test_fake.py` / `test_integration.py` so the contract is asserted once at the
seam, not echoed per-adapter. Suite-specific behaviour (sync-vs-loop test broker,
real-only schema validation, drain) stays where it is.

### 4. Documentation (ship-time, in the implementing PR)

No `CONTEXT.md` — this project records domain/architecture vocabulary in `CLAUDE.md`
+ `architecture/*.md`. At ship time:

- "DLQ projection" → **User-owned schema** + **DLQ** sections of `CLAUDE.md`, and
  `architecture/dlq.md`.
- "client contract" → **Test broker** section of `CLAUDE.md`, and
  `architecture/test-broker.md` (it is the thing that keeps the fake honest).

## Testing

- New `tests/test_client_contract.py` (parametrized fake + real; real auto-skips
  without Postgres) — the primary deliverable.
- Existing `test_fake.py` / `test_integration.py` scenarios that the contract suite
  now owns are deleted (replace, don't layer); the rest stay green.
- `just test` (full suite, Postgres 17) and `just lint` clean. Coverage stays at
  `--cov-fail-under=100` — the refactor removes code paths, it does not add untested
  ones.

## Risk

- **DLQ projection refactor changes the CTE SQL text.** Likelihood low, impact high
  (terminal failures route through it). Mitigation: the contract suite asserts the
  materialized DLQ row field-by-field against both adapters; the real path is
  exercised under Postgres in CI.
- **Contract suite hides drift it claims to catch (clock authority).** Documented
  explicitly as a non-goal; the residual invariant lives in `CLAUDE.md`. The suite
  catches structural drift, which is the actual maintenance hazard.
- **Import cycles from the new `_scheduling.py`.** Mitigation: it is a leaf
  depending only on `_time`; `producer`, `broker`, and `testing` import *from* it,
  never the reverse.
