---
status: shipped
date: 2026-06-04
slug: faststream-0.7.1-testbroker-typing
spec: faststream-0.7.1-testbroker-typing
pr: "43"
---

# FastStream 0.7.1 TestBroker typing alignment — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adopt FastStream 0.7.1's `TestBroker[Broker, EnterType]` typing fix here by binding `EnterType = OutboxBroker` and removing the two `# ty: ignore` directives that worked around the upstream `Broker | list[Broker]` return-type bug.

**Architecture:** Bump the FastStream pin to `>=0.7.1`, switch `TestOutboxBroker`'s generic to `TestBroker[OutboxBroker, OutboxBroker]`, update the ASGI registry hook return annotation to the new two-param shape, delete the two suppressions that the upstream fix obsoletes, and add a regression test in `tests/test_fake.py` that proves the entered context yields a single `OutboxBroker` (not a list/tuple).

**Tech Stack:** Python 3.13, FastStream 0.7.1, `uv` for deps, `ruff` for lint, `ty` for type check, `pytest` (under docker compose for the Postgres-backed suite).

**Spec:** [`planning/specs/2026-06-04-faststream-0.7.1-testbroker-typing-design.md`](../specs/2026-06-04-faststream-0.7.1-testbroker-typing-design.md)

**Commit strategy:** Single bundled commit at the end of Task 5. Tasks 1–4 stage incrementally without committing, so intermediate `ty check` runs reflect work-in-progress and the final commit captures one logical change.

**Branch:** `chore/faststream-0.7.1-testbroker-typing` (matches the sibling project's naming).

---

### Task 1: Bump the FastStream pin to >=0.7.1

**Files:**
- Modify: `pyproject.toml:12`

- [ ] **Step 1: Create the feature branch from main**

Run: `git switch -c chore/faststream-0.7.1-testbroker-typing`
Expected: switched to a new branch off `main`.

- [ ] **Step 2: Edit the dependency line**

In `pyproject.toml`, change:

```toml
"faststream>=0.7,<0.8",
```

to:

```toml
"faststream>=0.7.1,<0.8",
```

- [ ] **Step 3: Refresh the lockfile and sync**

Run: `just install`

This runs `uv lock --upgrade` followed by `uv sync --all-extras --all-groups --frozen`. Expected: `uv` resolves `faststream==0.7.1` (the only version satisfying both `>=0.7.1` and `<0.8` at time of writing). The sync should report no errors.

- [ ] **Step 4: Confirm the resolved version**

Run: `uv pip show faststream | grep -i version`
Expected output: `Version: 0.7.1`

- [ ] **Step 5: Sanity-run the no-Postgres tests against the upgraded library**

Run: `uv run pytest tests/test_unit.py tests/test_fake.py -v --no-cov`
Expected: all tests pass. (The two `ty: ignore` directives are still in place and still satisfy the type checker under 0.7.1, because `EnterType` defaults to `Any`. If anything fails here, **stop** — the upgrade has surfaced an unrelated regression that needs investigation before refactoring.) `--no-cov` is required because partial runs would otherwise trip the `--cov-fail-under=100` ratchet from `pyproject.toml`'s `addopts`.

- [ ] **Step 6: Stage the change (do not commit yet)**

Run: `git add pyproject.toml uv.lock`

(Verify `uv.lock` is tracked first with `git status`; if it isn't, drop it from the `git add`.)

---

### Task 2: Add the regression test for `__aenter__` return shape

**Files:**
- Modify: `tests/test_fake.py` (append new test at the end of the file)

This test is added *before* the refactor so we can prove the contract (single `OutboxBroker` returned from the context, never a list or tuple) survives the suppression removal. Under the current code (where `TestBroker[OutboxBroker]`'s `__aenter__` returns `OutboxBroker | list[OutboxBroker]` and we suppress with `ty: ignore`) the test should pass on the first run because the runtime `__aenter__` already returns the single broker — the bug was purely in the type annotation.

- [ ] **Step 1: Append the new test to `tests/test_fake.py`**

Add this test as the last function in the file (after `test_fake_dlq_not_emitted_on_handler_success`):

```python
async def test_test_broker_aenter_returns_single_outbox_broker() -> None:
    """0.7.1's EnterType binding means TestOutboxBroker yields a single OutboxBroker, not a list/tuple.

    Guards the contract through the upstream typing refactor: even if the base
    class signature changes again, our single-broker subclass must always hand
    back a single broker instance.
    """
    broker = _make_broker()
    async with TestOutboxBroker(broker) as br:
        assert isinstance(br, OutboxBroker)
```

No new imports needed — `TestOutboxBroker`, `OutboxBroker`, and the module-local `_make_broker()` helper are already in scope in `tests/test_fake.py`.

- [ ] **Step 2: Run the new test to confirm it passes against the current code**

Run: `uv run pytest tests/test_fake.py::test_test_broker_aenter_returns_single_outbox_broker -v --no-cov`
Expected: PASS. (The runtime `__aenter__` already returns the single `OutboxBroker`; we're locking that contract in.)

- [ ] **Step 3: Stage the change**

Run: `git add tests/test_fake.py`

---

### Task 3: Refactor `TestOutboxBroker` — bind `EnterType`, drop the type-arguments suppression

**Files:**
- Modify: `faststream_outbox/testing.py:521`

- [ ] **Step 1: Replace the class declaration**

In `faststream_outbox/testing.py`, locate line 521:

```python
class TestOutboxBroker(TestBroker[OutboxBroker]):  # ty: ignore[invalid-type-arguments]
```

Replace with:

```python
class TestOutboxBroker(TestBroker[OutboxBroker, OutboxBroker]):
```

The base `__aenter__` now returns `EnterType`, which we bind to `OutboxBroker`. The `invalid-type-arguments` suppression that worked around the 0.7.0 single-param shape is no longer needed.

**Do not touch** the `# ty: ignore[invalid-argument-type]` on `patch_broker_calls(broker)` later in the same file (around line 626) — that's unrelated (config-generic invariance on `BrokerUsecase`) and is documented in `CLAUDE.md`'s ignore table.

- [ ] **Step 2: Confirm `ty` is satisfied with the new annotation**

Run: `uv run ty check faststream_outbox/testing.py`
Expected: no errors. (If `ty` flags a *different* diagnostic on the class declaration, **stop** — the upstream 0.7.1 fix isn't behaving as documented in our environment; investigate before adding a replacement suppression.)

- [ ] **Step 3: Re-run the regression test from Task 2**

Run: `uv run pytest tests/test_fake.py::test_test_broker_aenter_returns_single_outbox_broker -v --no-cov`
Expected: PASS. (The result now comes from the base class via `EnterType = OutboxBroker` instead of falling through the union.)

- [ ] **Step 4: Re-run the full `test_fake.py` suite to confirm no regression**

Run: `uv run pytest tests/test_fake.py -v --no-cov`
Expected: every test passes. Every test in this file uses `async with TestOutboxBroker(broker)`; if `EnterType` were wired wrong, the subsequent `await broker.publish(...)` calls would still work at runtime (the override never affected runtime), but any `ty` mismatch would surface here when the next steps run `just lint`.

- [ ] **Step 5: Stage the change**

Run: `git add faststream_outbox/testing.py`

---

### Task 4: Update the ASGI registry hook annotation — drop the return-type suppression

**Files:**
- Modify: `faststream_outbox/__init__.py:45-47`

- [ ] **Step 1: Update the return type annotation and drop the inline ignore**

In `faststream_outbox/__init__.py`, locate lines 45–47:

```python
    @functools.lru_cache(maxsize=1)
    def get_broker_registry() -> dict[type[BrokerUsecase[typing.Any, typing.Any]], type[TestBroker[typing.Any]]]:
        return {**original_get_broker_registry(), OutboxBroker: TestOutboxBroker}  # ty: ignore[invalid-return-type]
```

Replace with:

```python
    @functools.lru_cache(maxsize=1)
    def get_broker_registry() -> dict[
        type[BrokerUsecase[typing.Any, typing.Any]],
        type[TestBroker[typing.Any, typing.Any]],
    ]:
        return {**original_get_broker_registry(), OutboxBroker: TestOutboxBroker}
```

This matches the new shape of `faststream.asgi.factories.asyncapi.try_it_out._get_broker_registry`, which 0.7.1 typed as `dict[..., type[TestBroker[Any, Any]]]`. With `TestOutboxBroker` now declared `TestBroker[OutboxBroker, OutboxBroker]` (from Task 3), the dict value type is structurally assignable and the `invalid-return-type` suppression comes off cleanly.

- [ ] **Step 2: Confirm `ty` is satisfied with the new annotation**

Run: `uv run ty check faststream_outbox/__init__.py`
Expected: no errors. (Same stop-and-investigate rule as Task 3 Step 2 if `ty` flags a different diagnostic.)

- [ ] **Step 3: Stage the change**

Run: `git add faststream_outbox/__init__.py`

---

### Task 5: Final validation and bundled commit

**Files:**
- All four changes above are now staged together.

- [ ] **Step 1: Lint the staged changes**

Run: `just lint`
Expected: `eof-fixer`, `ruff format`, `ruff check --fix`, and `ty check` all pass. If `ruff format` or `ruff check --fix` modifies any staged file, re-stage with `git add <modified-files>` and re-run `just lint` until clean.

- [ ] **Step 2: Run the full test suite under docker compose**

Run: `just test`
Expected: every test in `tests/test_unit.py`, `tests/test_fake.py`, and `tests/test_integration.py` passes, and the `--cov-fail-under=100` ratchet is satisfied. The new regression test from Task 2 should appear in the output and pass.

- [ ] **Step 3: Review staged diff one more time**

Run: `git diff --staged`
Expected: changes only in `pyproject.toml`, `uv.lock` (if tracked), `tests/test_fake.py`, `faststream_outbox/testing.py`, and `faststream_outbox/__init__.py`. No drive-by edits.

- [ ] **Step 4: Commit**

Run:

```bash
git commit -m "$(cat <<'EOF'
chore: adopt faststream 0.7.1 TestBroker typing fix

ag2ai/faststream#2903 makes TestBroker generic over a second EnterType
TypeVar (default Any) and threads it through __aenter__. Bind
EnterType = OutboxBroker in our TestOutboxBroker and drop the
# ty: ignore[invalid-type-arguments] on the class declaration plus the
# ty: ignore[invalid-return-type] on get_broker_registry's return that
worked around the old Broker | list[Broker] return type. Update the
ASGI registry annotation to the new two-param shape and bump the
faststream floor to >=0.7.1.

Adds a regression test guarding the single-broker contract through
future upstream changes.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"

```

- [ ] **Step 5: Verify clean tree**

Run: `git status`
Expected: working tree clean; one new commit on `chore/faststream-0.7.1-testbroker-typing`.

---

## Validation summary (post-implementation)

After Task 5, the following should hold:

- `pyproject.toml` requires `faststream>=0.7.1,<0.8`.
- `TestOutboxBroker` declares `TestBroker[OutboxBroker, OutboxBroker]` with **no** `# ty: ignore` on the class declaration.
- `get_broker_registry` returns `dict[..., type[TestBroker[typing.Any, typing.Any]]]` with **no** `# ty: ignore` on the return statement.
- The `# ty: ignore[invalid-argument-type]` on `patch_broker_calls(broker)` in `_fake_start` is **untouched** (documented in `CLAUDE.md` as a separate concern).
- `tests/test_fake.py::test_test_broker_aenter_returns_single_outbox_broker` is present and passing.
- `just lint` and `just test` both succeed.

## Risks & mitigations

- **Upstream AST helper depth.** `TestOutboxBroker.__init__` adds an extra frame on top of `TestBroker.__init__`. The 0.7.1 PR adds a `while … name == "__init__"` walk in `_internal/testing/ast.py` exactly for this, so the `async with` AST analysis still finds the user frame. Validated implicitly by Task 3 Step 4 (the full `test_fake.py` suite, which depends on this mechanism). No code change required on our side.
- **`uv lock --upgrade` pulling in unrelated upgrades.** `just install` runs `uv lock --upgrade` which refreshes *all* dependencies. If this surfaces incidental breakage in Task 1 Step 3, narrow the upgrade to `uv lock --upgrade-package faststream` and re-run `uv sync --all-extras --all-groups --frozen` to avoid pulling in unrelated changes.
- **`just test` requires Docker.** If Docker isn't running locally, `just test` will fail at the compose step. Start Docker before Task 5 Step 2. The no-Docker subset (`tests/test_unit.py`, `tests/test_fake.py`) already ran in Task 1 Step 5 and Task 3 Step 4; Task 5 Step 2 is what actually exercises `tests/test_integration.py` against real Postgres and satisfies the 100% coverage ratchet.
- **`ty` still flagging diagnostics after the suppressions come off.** If Task 3 Step 2 or Task 4 Step 2 surfaces a *different* `ty` diagnostic, the playbook is to investigate (not re-add the original ignore). The original suppressions targeted bugs that 0.7.1 fixes; a new diagnostic would mean either the upstream fix isn't behaving as documented in our environment, or our annotation has a separate issue that deserves its own justification entry in `CLAUDE.md`'s ignore table.
