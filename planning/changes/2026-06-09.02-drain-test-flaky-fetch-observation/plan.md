---
status: shipped
date: 2026-06-09
slug: drain-test-flaky-fetch-observation
spec: drain-test-flaky-fetch-observation
pr: "48"
---

# Drain Test Flake Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove a Python 3.13/3.14 coverage flake in `test_drain_finishes_inflight_rows_before_returning` by replacing the SQL-polling wait helper with observation of the broker's existing `fetched` recorder event.

**Architecture:** Test-only refactor inside `tests/test_integration.py`. One test is modified to register a `metrics_recorder` closure that accumulates the `count` field from `fetched` events, and waits on a predicate over that counter via the existing generic `_wait_until` helper. The single-caller helper `_wait_until_claimed` is then deleted. No production code is touched.

**Tech Stack:** Python 3.13/3.14, pytest, pytest-asyncio, SQLAlchemy async, FastStream, existing `OutboxBroker.metrics_recorder` callable seam.

**Spec:** `planning/specs/2026-06-09-drain-test-flaky-fetch-observation-design.md`

---

## File Structure

- Modify: `tests/test_integration.py`
  - Add one import (`Mapping` from `collections.abc`).
  - Refactor `test_drain_finishes_inflight_rows_before_returning` (currently lines 1130-1158) to register a recorder closure and use `_wait_until` instead of `_wait_until_claimed`.
  - Delete `_wait_until_claimed` (currently lines 1112-1127) — single caller goes away.

No other files change. No new helpers introduced.

---

## Task 1: Baseline confirmation

**Files:**
- Read-only: `tests/test_integration.py`, `faststream_outbox/subscriber/usecase.py:315-326`

- [ ] **Step 1: Confirm baseline test passes on 3.13 with current code**

Run:

```bash
just test tests/test_integration.py::test_drain_finishes_inflight_rows_before_returning -v --no-cov
```

Expected: `1 passed`. This is the pre-refactor green state; the refactor must keep it green.

- [ ] **Step 2: Confirm the `fetched` event field name**

Read `faststream_outbox/subscriber/usecase.py` around line 323. Verify the emit looks like:

```python
self._emit_metric(
    "fetched",
    {**self._base_tags(self._queues[0] if self._queues else ""), "count": len(rows)},
)
```

The field used by the refactor is `count` and it is an `int`. If this differs from what is shown, stop and reconcile against the spec before continuing — the refactor depends on this field.

- [ ] **Step 3: Confirm there are no other callers of `_wait_until_claimed`**

Run:

```bash
grep -rn "_wait_until_claimed" tests/ faststream_outbox/ planning/
```

Expected output: exactly two matches inside `tests/test_integration.py` (the definition and its single call). If any other reference exists, stop and update the plan — additional callers must also be migrated.

---

## Task 2: Refactor the drain test to use recorder observation

**Files:**
- Modify: `tests/test_integration.py:7` (add `Mapping` to existing imports)
- Modify: `tests/test_integration.py:1130-1158` (rewrite test body)
- Delete: `tests/test_integration.py:1112-1127` (entire `_wait_until_claimed` function)

- [ ] **Step 1: Add `Mapping` import**

Modify the top-of-file imports. Today line 7 reads:

```python
from typing import Any
```

Add a `from collections.abc import Mapping` line immediately before it, so the typing-style imports are:

```python
from collections.abc import Mapping
from typing import Any
from unittest import mock
```

Rationale: the recorder callable's `fields` parameter is typed as `Mapping[str, Any]` (matches the production callable contract). Global imports only (per CLAUDE.md project convention — `feedback_no_local_imports`).

- [ ] **Step 2: Rewrite the test body**

Replace `test_drain_finishes_inflight_rows_before_returning` (currently `tests/test_integration.py:1130-1158`) with the version below. The header docstring, subscriber decorator kwargs, and final asserts are unchanged — only the broker construction and the wait line change.

```python
async def test_drain_finishes_inflight_rows_before_returning(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """Rows claimed by fetch must run to completion when broker.stop() is called."""
    fetched_total = 0

    def recorder(event: str, fields: Mapping[str, Any]) -> None:
        nonlocal fetched_total
        if event == "fetched":
            fetched_total += fields["count"]

    broker = OutboxBroker(
        pg_engine,
        outbox_table=outbox_table,
        graceful_timeout=5.0,
        metrics_recorder=recorder,
    )
    handled: list[int] = []

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.02,
        max_fetch_interval=0.05,
        max_workers=4,
        fetch_batch_size=20,
    )
    async def handle(body: dict) -> None:
        await asyncio.sleep(0.1)
        handled.append(body["i"])

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            for i in range(20):
                await broker.publish({"i": i}, queue="orders", session=session)
        await _wait_until(lambda: fetched_total >= 20, timeout=3.0)
        await broker.stop()

    assert sorted(handled) == list(range(20))
    assert await _row_count(pg_engine, outbox_table) == 0
```

Key differences from the current test:

1. New `fetched_total` counter + `recorder` closure declared at the top of the test function.
2. `OutboxBroker(...)` call gains `metrics_recorder=recorder`.
3. The wait line changes from `await _wait_until_claimed(pg_engine, outbox_table, timeout=3.0)` to `await _wait_until(lambda: fetched_total >= 20, timeout=3.0)`.
4. Everything else (subscriber decorator, handler body, session block, asserts) is byte-for-byte unchanged.

- [ ] **Step 3: Delete `_wait_until_claimed`**

Delete the entire function definition at `tests/test_integration.py:1112-1127`:

```python
async def _wait_until_claimed(
    pg_engine: AsyncEngine,
    outbox_table: Table,
    *,
    timeout: float,  # noqa: ASYNC109
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    stmt = select(outbox_table.c.id).where(outbox_table.c.acquired_token.is_(None))
    while asyncio.get_event_loop().time() < deadline:
        async with pg_engine.connect() as conn:
            unclaimed = (await conn.execute(stmt)).fetchall()
        if not unclaimed:
            return
        await asyncio.sleep(0.02)
    msg = "fetch never claimed every row"  # pragma: no cover
    raise AssertionError(msg)  # pragma: no cover
```

After deletion, the line that previously preceded this function (the closing line of the test above it) should be directly followed by two blank lines, then `async def test_drain_finishes_inflight_rows_before_returning(...)`.

- [ ] **Step 4: Run the modified test in isolation**

Run:

```bash
just test tests/test_integration.py::test_drain_finishes_inflight_rows_before_returning -v --no-cov
```

Expected: `1 passed`. If it fails, the most likely causes are:
- Typo in the `fetched_total` accumulation closure.
- The `fetched` event payload's `count` field name is wrong (re-check Task 1 Step 2).
- `Mapping` import missing or misspelled.

Fix and re-run until green before continuing.

- [ ] **Step 5: Run the full integration test file with coverage**

Run:

```bash
just test tests/test_integration.py -v
```

Expected: all tests pass, coverage report shows `tests/test_integration.py` at 100%, no `Required test coverage of 100% not reached` failure. The total stmt count for `tests/test_integration.py` should drop by ~16 (the deleted helper body).

If a different test now reports a coverage miss, stop — the change has had an unintended side effect. Do not paper over with `pragma: no cover`; surface to the user.

- [ ] **Step 6: Run lint**

Run:

```bash
just lint
```

Expected: zero issues. The only new construct is a `Mapping` import that ruff/ty should accept without complaint. If ruff flags `Mapping` as unused, the recorder signature was not updated correctly.

- [ ] **Step 7: Commit**

```bash
git add tests/test_integration.py
git commit -m "$(cat <<'EOF'
test: drain test waits via fetched recorder, not SQL poll

Removes _wait_until_claimed and migrates the lone caller
(test_drain_finishes_inflight_rows_before_returning) to observe
the broker's fetched recorder event. The SQL-poll variant had
a Python 3.13/3.14 timing race that left tests/test_integration.py:1125
uncovered on 3.14, tripping the 100% fail-under gate.

Spec: planning/specs/2026-06-09-drain-test-flaky-fetch-observation-design.md
EOF
)"
```

---

## Task 3: CI verification

**Files:** None modified.

- [ ] **Step 1: Push the branch and open a PR**

Standard project flow. Once the PR is open, CI runs both the 3.13 and 3.14 matrix legs of `_checks.yml`.

- [ ] **Step 2: Confirm both Python matrix legs are green**

Watch for:

```
checks / pytest (3.13)  ✓
checks / pytest (3.14)  ✓
checks / lint           ✓
```

Both pytest jobs must report 100% coverage with no `FAIL Required test coverage of 100% not reached`. If 3.14 still misses a line, it is a *different* flake than the one this plan fixes — capture the new symptom and stop. Do not retry.

- [ ] **Step 3: Merge**

Standard merge once green and reviewed.

---

## Out of Scope

The following items are explicitly excluded from this plan (matches the spec's Out of Scope section):

- Fixing the upstream FastStream `RuntimeWarning` ("Error `{e!r}` occurred at AST parsing") seen on 3.14.
- Any change to the `scheduled.yml` workflow's issue-creation flow.
- Any change to the generic `_wait_until` helper.
- Any production code change.

If any of these surface as blockers during execution, stop and re-scope rather than expanding the plan.
