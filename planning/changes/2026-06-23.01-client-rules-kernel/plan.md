---
status: shipped
date: 2026-06-23
slug: client-rules-kernel
spec: client-rules-kernel
pr: 109
---

# client-rules-kernel — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the hand-maintained rule duplication between `OutboxClient` and
`FakeOutboxClient` — share the pure bits, co-verify the SQL bits with one contract
suite — without changing any runtime behaviour.

**Spec:** [`design.md`](./design.md)

**Branch:** `refactor/client-rules-kernel`

**Commit strategy:** Per-task commits.

---

### Task 1: Extract `_scheduling.py`

**Files:**
- Create: `faststream_outbox/_scheduling.py`
- Modify: `faststream_outbox/publisher/producer.py`, `faststream_outbox/broker.py`, `faststream_outbox/testing.py`

Pull the three pure activate-args helpers into one leaf module; kill the inlined
copy in `publish_batch`. Pure move — no behaviour change. (Spec §2.)

- [ ] **Step 1: Create the leaf module**

  New `faststream_outbox/_scheduling.py` depending only on `_time`, with:
  `is_future_dated(activate_in, activate_at, now)`,
  `resolve_next_attempt_client_side(activate_in, activate_at, now)`,
  `validate_activate_args(method_name, activate_in, activate_at)`.
  Bodies move verbatim from `producer.py:35` (`_is_future_dated`), `broker.py:73`
  (`_compute_next_at_client_side`), `broker.py:59` (`_validate_activate_args`).

- [ ] **Step 2: Rewire consumers**

  - `producer.py`: import from `_scheduling`; delete local `_is_future_dated`; in
    `publish_batch` (`producer.py:175–201`) call `resolve_next_attempt_client_side`
    instead of inlining `now + cmd.activate_in` / `cmd.activate_at`.
  - `broker.py`: delete local `_compute_next_at_client_side` and
    `_validate_activate_args`; update any in-module callers to import from `_scheduling`.
  - `testing.py`: change the imports at `testing.py:27–28` to source from `_scheduling`.

- [ ] **Step 3: Verify**

  `uv run pytest tests/test_unit.py tests/test_fake.py --no-cov -q` green.
  `grep -rn "_compute_next_at_client_side\|_is_future_dated\|_validate_activate_args" faststream_outbox/`
  shows definitions only in `_scheduling.py`.

- [ ] **Step 4: Commit**

  ```bash
  git add faststream_outbox/_scheduling.py faststream_outbox/publisher/producer.py faststream_outbox/broker.py faststream_outbox/testing.py
  git commit -m "refactor(scheduling): consolidate activate-args helpers into _scheduling.py

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Write the client contract suite (green against current code)

**Files:**
- Create: `tests/test_client_contract.py`
- Modify: `tests/conftest.py` (per-adapter harness fixture)

Establish the cross-adapter safety net *before* refactoring the DLQ projection, so
Task 3 is guarded. The suite must pass against the current code. (Spec §3.)

- [ ] **Step 1: Add the harness fixture**

  In `conftest.py`, a fixture parametrized `client ∈ {fake, real}` yielding a harness
  exposing `client`, `seed_row(**fields)` (fake: `.feed(...)`; real: raw `INSERT`),
  and `open_conn()` (fake: `None`; real: `engine.connect()`). The `real` param uses
  the existing `pg_engine` fixture and **skips when Postgres is unreachable** (mirror
  `test_integration.py`'s skip gate).

- [ ] **Step 2: Write the contract scenarios**

  In `test_client_contract.py`, adapter-agnostic scenarios covering the contract in
  spec §3: `fetch` (unleased / future-dated / expired-lease / fresh-lease / FIFO /
  limit / queue filter), claim side-effects, `delete_with_lease` (token match /
  mismatch / NULL token / DLQ materialization), `mark_pending_with_lease`,
  `cancel_timer`, `timer_id` dedup. Expectations hand-specified, not computed from a
  shared function.

- [ ] **Step 3: Verify both adapters**

  `uv run pytest tests/test_client_contract.py --no-cov -q` green for the fake param.
  `just test tests/test_client_contract.py` green for both params (Postgres up).

- [ ] **Step 4: Commit**

  ```bash
  git add tests/test_client_contract.py tests/conftest.py
  git commit -m "test(client): add contract suite run against both fake and real adapters

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Extract `_DLQ_PROJECTION` (guarded by Task 2)

**Files:**
- Modify: `faststream_outbox/schema.py`, `faststream_outbox/client.py`, `faststream_outbox/testing.py`

Replace the hand-kept outbox→DLQ parity with one declarative projection. (Spec §1.)

- [ ] **Step 1: Declare the projection**

  In `schema.py`, next to `make_dlq_table`, add `_DLQ_PROJECTION` — ordered
  `(outbox_col, dlq_col)` pairs (`id→original_id`, `queue→queue`, `payload`,
  `headers`, `deliveries_count`, `created_at`, `timer_id`). Note injected fields
  (`failure_reason`, `last_exception`) in a comment.

- [ ] **Step 2: Consume it in both adapters**

  - `client.py`: `_build_dlq_cte_stmt` (`:335–348`) builds its `RETURNING` /
    `INSERT (...)` / `SELECT` column lists from `_DLQ_PROJECTION` (preserve the
    identifier-quoting via the dialect preparer). Delete the `B10` literal-list risk.
  - `testing.py`: `delete_with_lease` (`:183–196`) builds the DLQ dict from
    `_DLQ_PROJECTION`. Delete the `# P9 parity` comment — parity is now structural.

- [ ] **Step 3: Verify no drift**

  `just test tests/test_client_contract.py` still green for both params (the suite's
  DLQ-materialization assertions now guard the refactor). Full `just test` green.

- [ ] **Step 4: Commit**

  ```bash
  git add faststream_outbox/schema.py faststream_outbox/client.py faststream_outbox/testing.py
  git commit -m "refactor(dlq): derive outbox->DLQ projection from one declarative map

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: Replace, don't layer — prune redundant tests

**Files:**
- Modify: `tests/test_fake.py`, `tests/test_integration.py`

Delete per-adapter scenarios the contract suite now owns; keep suite-specific tests
(sync-vs-loop test broker, real-only schema validation, drain). (Spec §3.)

- [ ] **Step 1: Identify overlap**

  Find tests in `test_fake.py` / `test_integration.py` whose assertions are now
  subsumed by `test_client_contract.py` (eligibility, lease reclaim, token guard,
  DLQ projection, timer dedup, cancel_timer).

- [ ] **Step 2: Delete the overlap; keep the rest**

  Remove the subsumed cases. Leave anything the contract suite cannot express.

- [ ] **Step 3: Verify coverage holds**

  `just test` green with `--cov-fail-under=100`. If coverage drops, a deleted test
  covered a path the contract suite misses — restore it or extend the suite.

- [ ] **Step 4: Commit**

  ```bash
  git add tests/test_fake.py tests/test_integration.py
  git commit -m "test: drop per-adapter cases now owned by the client contract suite

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 5: Promote docs (ship-time, in this PR)

**Files:**
- Modify: `CLAUDE.md`, `architecture/dlq.md`, `architecture/test-broker.md`
- Modify: `planning/changes/2026-06-23.01-client-rules-kernel/design.md` (frontmatter)

Record the new vocabulary where this project keeps it; close out the change. (Spec §4.)

- [ ] **Step 1: CLAUDE.md + architecture/**

  - "DLQ projection" → **User-owned schema** + **DLQ** sections of `CLAUDE.md`;
    `architecture/dlq.md`.
  - "client contract" → **Test broker** section of `CLAUDE.md`;
    `architecture/test-broker.md`. Note the documented clock-skew residual (the
    contract pins structural drift, not cross-host clock authority).

- [ ] **Step 2: Close out the change**

  Set `status: shipped`, fill `pr:` and `outcome:` in `design.md` (and `plan.md`).
  `just index` to regenerate the listing.

- [ ] **Step 3: Final verification**

  `just lint` and `just test` clean.

- [ ] **Step 4: Commit**

  ```bash
  git add CLAUDE.md architecture/ planning/
  git commit -m "docs: record DLQ projection + client contract; close out change

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```
