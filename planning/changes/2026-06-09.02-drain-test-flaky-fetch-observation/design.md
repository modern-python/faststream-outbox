---
status: shipped
date: 2026-06-09
slug: drain-test-flaky-fetch-observation
summary: Drain test waits via the fetched recorder instead of an SQL poll, killing a 3.14 coverage flake.
supersedes: null
superseded_by: null
pr: "48"
outcome: merged 2026-06-10 as #48
---

# Drain test: replace SQL-poll flake with recorder observation

## Problem

`tests/test_integration.py::test_drain_finishes_inflight_rows_before_returning` failed CI on Python 3.14 with a single-line coverage miss (`tests/test_integration.py:1125` — the `await asyncio.sleep(0.02)` inside the test helper `_wait_until_claimed`). 3.13 reached 100%; 3.14 reached 99.98%, tripping `--cov-fail-under=100`.

The miss is not a regression in the package — it is a timing race in the test setup itself. The helper polls Postgres for "rows where `acquired_token IS NULL`" until none remain:

```python
async def _wait_until_claimed(pg_engine, outbox_table, *, timeout):
    deadline = asyncio.get_event_loop().time() + timeout
    stmt = select(outbox_table.c.id).where(outbox_table.c.acquired_token.is_(None))
    while asyncio.get_event_loop().time() < deadline:
        async with pg_engine.connect() as conn:
            unclaimed = (await conn.execute(stmt)).fetchall()
        if not unclaimed:
            return
        await asyncio.sleep(0.02)  # line 1125 — only runs when first poll missed
    msg = "fetch never claimed every row"  # pragma: no cover
    raise AssertionError(msg)  # pragma: no cover
```

On 3.13 the test setup's asyncio scheduling typically interleaves so that the first poll observes some rows still unclaimed, the sleep runs, then the second poll observes all claimed. On 3.14 the worker reliably fetches before the first poll runs, so the loop returns on the first iteration and the sleep is never executed.

Either path produces a passing test functionally — what changes is which line is exercised. The line is real code, so `pragma: no cover` would only hide the timing dependency, not remove it. The test is genuinely flaky in the coverage dimension.

## Goal

Remove the SQL-polling race in the drain test by observing fetch progress through the broker's own observability surface (`metrics_recorder`), so the test's wait condition is independent of asyncio scheduling order between the publish commit and the worker's first fetch.

Restore deterministic 100% coverage across 3.13 and 3.14.

## Design

### Replace `_wait_until_claimed` with `_wait_until` + recorder observation

The broker already emits a `fetched` event on every fetch tick (`subscriber/usecase.py:323`) with payload `{queue, count, ...base_tags}` — `count` is the number of rows the CTE claimed on that tick (zero on idle). Summing `count` across recorder calls gives the total rows claimed so far, which is precisely what `_wait_until_claimed` is approximating via SQL.

The refactored test:

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

Then **delete `_wait_until_claimed`** (lines 1112-1127) — it has no other callers.

### Why this removes the flake

The original race window was between two events the test cannot order deterministically: "publish session commit" and "subscriber's first fetch CTE." The SQL poll observes the *outcome* (rows claimed in DB), so a fast first fetch makes the loop return before the sleep.

The recorder observation hooks into the broker's own callback emitted *at the moment of fetch*. The wait condition (`fetched_total >= 20`) is satisfied when the broker reports having claimed all rows — there is no observation window separate from the event itself. The `_wait_until` generic helper still uses an interior `await asyncio.sleep(0.05)`, but that helper has ~17 callers in this file, several of which guaranteeably exercise the slow path (predicate not satisfied on the first check), so its line-level coverage is robust regardless of single-call timing.

### Coverage shape after the change

- `_wait_until_claimed` is deleted → its line 1125 ceases to exist → no flake source.
- `_wait_until`'s own `await asyncio.sleep(0.05)` (line 35) stays at 100% via its other callers; the new caller adds a 21st invocation and does not change coverage character.
- Total file statements drop by ~16 (deleted helper body); no new statements introduced.

### What is *not* changing

- Test semantics: still asserts all 20 rows handled, still asserts table empty after stop.
- Drain behavior under test: still calls `stop()` after the broker has claimed all 20 rows, so the test still exercises "drain finishes in-flight rows" (not "drain leaves unfetched rows behind").
- Other tests that import or reference `_wait_until_claimed`: none exist (verified by grep).
- The `metrics_recorder` callable signature, the `fetched` event, the broker's emission point — all consumed as-is, no production code touched.

## Out of scope

- The Python 3.14 FastStream `RuntimeWarning` ("Error `{e!r}` occurred at AST parsing") seen in the same run. It is unrelated to the coverage failure and is upstream FastStream behavior.
- The scheduled workflow's issue-creation flow. It works as designed (gated on `event_name == 'schedule'`) and has not yet been exercised because the first scheduled run is 2026-06-15.
- Any change to the generic `_wait_until` helper signature or polling cadence.
- Any production code change. This is a test-only refactor.

## Risks

- **Recorder field rename.** If a future change renames `count` to something else in the `fetched` payload, this test breaks loudly with `KeyError`. The `fetched` event's payload shape is documented in CLAUDE.md and emitted from exactly one site (`subscriber/usecase.py:323`); renaming it is already a search-and-replace operation and the test breakage would be appropriate signal.
- **3.14-specific scheduling drift elsewhere.** If 3.14 introduces broader timing differences, other tests in this file may surface similar coverage gaps. Out of scope for this spec — handle per-test as they arise.

## Verification

1. `uv run pytest tests/test_integration.py::test_drain_finishes_inflight_rows_before_returning -v` passes locally on 3.13.
2. `uv run pytest tests/test_integration.py` produces 100% coverage on 3.13 with `--cov-fail-under=100` re-enabled.
3. Reasoning check for 3.14: the wait condition is satisfied by the recorder event itself, which fires unconditionally on every fetch tick regardless of scheduler ordering. No timing assumption remains in the test.
4. CI on the change branch runs both the 3.13 and 3.14 matrix legs to 100% green.
