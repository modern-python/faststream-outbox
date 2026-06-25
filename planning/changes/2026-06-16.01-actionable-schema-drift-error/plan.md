# actionable-schema-drift-error — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `validate_schema()`'s `RuntimeError` point operators to a
hand-written-migration recipe whenever it reports drift that
`alembic revision --autogenerate` cannot remediate (the `lease_ck` CHECK and the
partial-index predicates).

**Spec:** [`design.md`](./design.md)

**Branch:** `fix/actionable-schema-drift-error`

**Commit strategy:** Per-task commits.

**Tooling notes for the executor:**
- Pure-helper unit tests run with no Postgres: `uv run pytest tests/test_unit.py -k <name>`.
- Integration tests need Postgres via docker compose: `just test tests/test_integration.py -k <name>`.
  Pass a single `-k` keyword — `just test -k "a or b"` word-splits and fails.
- Coverage gate is `--cov-fail-under=100` on the full `just test` run; partial
  runs fail it, so use `--no-cov` while iterating and rely on the final full run.
- All imports go at module top — never inline (project convention). Type every
  new test parameter, including fixtures.

---

### Task 1: Add the pure message-composition helper (TDD)

**Files:**
- Modify: `faststream_outbox/client.py`
- Test: `tests/test_unit.py`

Introduce `_compose_schema_mismatch_message` plus the `_SCHEMA_MISMATCH_PREFIX`
and `_AUTOGEN_BLIND_HINT` constants. Pure function, fully unit-testable without
Postgres.

- [ ] **Step 1: Write the failing tests**

  Add to `tests/test_unit.py`. Extend the existing client import on line 42
  (`from faststream_outbox.client import OutboxClient, _validate_schema_sync`) to
  also import the new names:

  ```python
  from faststream_outbox.client import (
      OutboxClient,
      _AUTOGEN_BLIND_HINT,
      _SCHEMA_MISMATCH_PREFIX,
      _compose_schema_mismatch_message,
      _validate_schema_sync,
  )
  ```

  Then add these three tests (place them near the other client unit tests):

  ```python
  def test_compose_schema_mismatch_message_appends_hint_on_blind_drift() -> None:
      msg = _compose_schema_mismatch_message(
          ["missing CHECK constraint 'outbox_lease_ck' (expected '...')"],
          has_blind_drift=True,
      )
      assert msg.startswith(_SCHEMA_MISMATCH_PREFIX)
      assert _AUTOGEN_BLIND_HINT in msg
      assert "#fixing-drift-autogenerate-cant-see" in msg


  def test_compose_schema_mismatch_message_omits_hint_without_blind_drift() -> None:
      msg = _compose_schema_mismatch_message(
          ["table 'outbox' missing column 'headers'"],
          has_blind_drift=False,
      )
      assert msg == _SCHEMA_MISMATCH_PREFIX + "table 'outbox' missing column 'headers'"
      assert _AUTOGEN_BLIND_HINT not in msg


  def test_compose_schema_mismatch_message_joins_multiple_errors() -> None:
      msg = _compose_schema_mismatch_message(["a", "b"], has_blind_drift=False)
      assert msg == _SCHEMA_MISMATCH_PREFIX + "a; b"
  ```

- [ ] **Step 2: Run the tests to verify they fail**

  Run: `uv run pytest tests/test_unit.py -k compose_schema_mismatch --no-cov -v`
  Expected: collection/import error or FAIL — `_compose_schema_mismatch_message`
  (and the two constants) do not exist yet.

- [ ] **Step 3: Implement the helper in `client.py`**

  Add the following at module scope in `faststream_outbox/client.py`, immediately
  after the `_validate_check_constraints_sync` function (after the block ending at
  the current line 576), so all schema-validation machinery stays grouped:

  ```python
  # The published docs anchor for hand-written migrations that fix drift
  # `alembic revision --autogenerate` cannot emit (no check-constraint comparator;
  # the index comparator ignores postgresql_where). Appended to the RuntimeError
  # only when an Alembic-blind probe actually fired — see validate_schema().
  _SCHEMA_MISMATCH_PREFIX = "Outbox schema mismatch: "
  _AUTOGEN_BLIND_HINT = (
      "These (CHECK constraints and partial-index predicates) are invisible to "
      "'alembic revision --autogenerate' — hand-write the migration: "
      "https://faststream-outbox.modern-python.org/operations/alembic/"
      "#fixing-drift-autogenerate-cant-see"
  )


  def _compose_schema_mismatch_message(errors: list[str], *, has_blind_drift: bool) -> str:
      """Build the validate_schema RuntimeError text; append the remediation pointer for Alembic-blind drift."""
      msg = _SCHEMA_MISMATCH_PREFIX + "; ".join(errors)
      if has_blind_drift:
          msg += "\n\n" + _AUTOGEN_BLIND_HINT
      return msg
  ```

- [ ] **Step 4: Run the tests to verify they pass**

  Run: `uv run pytest tests/test_unit.py -k compose_schema_mismatch --no-cov -v`
  Expected: 3 passed.

- [ ] **Step 5: Commit**

  ```bash
  git add faststream_outbox/client.py tests/test_unit.py
  git commit -m "feat(client): add schema-mismatch message composer with autogen-blind hint

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Wire `validate_schema()` to the helper and assert the pointer in integration tests

**Files:**
- Modify: `faststream_outbox/client.py:401-425` (the `validate_schema` method)
- Test: `tests/test_integration.py`

Track the Alembic-blind probe errors separately and raise through the new helper.
Cover the present/absent pointer behaviour against real Postgres.

- [ ] **Step 1: Update the integration tests (failing assertions first)**

  In `tests/test_integration.py`, edit
  `test_validate_schema_fails_when_lease_check_constraint_missing` (currently
  lines 1919-1929) to capture the exception and assert the pointer:

  ```python
  async def test_validate_schema_fails_when_lease_check_constraint_missing(
      pg_engine: AsyncEngine,
      outbox_table: Table,
  ) -> None:
      """A dropped ``<table>_lease_ck`` CHECK must be caught — alembic's diff can't see it (audit 2026-06-14)."""
      drop_sql = f'ALTER TABLE "{outbox_table.name}" DROP CONSTRAINT "{outbox_table.name}_lease_ck"'
      async with pg_engine.begin() as conn:
          await conn.exec_driver_sql(drop_sql)
      client = OutboxClient(pg_engine, outbox_table)
      with pytest.raises(RuntimeError, match="missing CHECK constraint") as excinfo:
          await client.validate_schema()
      assert "operations/alembic/#fixing-drift-autogenerate-cant-see" in str(excinfo.value)
  ```

  And edit `test_validate_schema_fails_when_columns_missing` (currently lines
  454-461) to assert the pointer is **absent** for autogenerate-fixable drift:

  ```python
  async def test_validate_schema_fails_when_columns_missing(pg_engine, outbox_table) -> None:
      """Drop a column the package expects and verify validate_schema reports it."""
      drop_sql = f'ALTER TABLE "{outbox_table.name}" DROP COLUMN headers'
      async with pg_engine.begin() as conn:
          await conn.exec_driver_sql(drop_sql)
      client = OutboxClient(pg_engine, outbox_table)
      with pytest.raises(RuntimeError, match="missing column 'headers'") as excinfo:
          await client.validate_schema()
      assert "fixing-drift-autogenerate-cant-see" not in str(excinfo.value)
  ```

- [ ] **Step 2: Run the two tests to verify the new assertions fail**

  Run: `just test tests/test_integration.py -k test_validate_schema_fails_when_lease_check_constraint_missing`
  Expected: FAIL — the current message has no pointer, so the new
  `assert ... in str(excinfo.value)` fails.
  (The columns-missing test still passes at this point — the pointer is already
  absent — but its new negative assertion guards Step 3.)

- [ ] **Step 3: Rewire `validate_schema` in `client.py`**

  Replace the body of `validate_schema` (lines 410-425) — keep the docstring
  above it unchanged. Old:

  ```python
          async with self._engine.connect() as conn:
              errors = await conn.run_sync(_validate_schema_sync, self._table)
              # S2: alembic's autogenerate diff compares index columns + uniqueness but NOT
              # the partial-index WHERE predicate, so a wrong postgresql_where slips through
              # and later breaks the producer's ON CONFLICT arbiter. Probe the predicates
              # directly against the live catalog.
              errors.extend(await conn.run_sync(_validate_index_predicates_sync, self._table))
              # Alembic's compare_metadata has no check-constraint comparator, so a missing
              # or altered <table>_lease_ck (the half-set-lease guard) passes the diff above
              # silently. Probe pg_constraint directly, mirroring the partial-index probe.
              errors.extend(await conn.run_sync(_validate_check_constraints_sync, self._table))
              if self._dlq_table is not None:
                  errors.extend(await conn.run_sync(_validate_dlq_schema_sync, self._dlq_table))
          if errors:
              msg = "Outbox schema mismatch: " + "; ".join(errors)
              raise RuntimeError(msg)
  ```

  New:

  ```python
          async with self._engine.connect() as conn:
              errors = await conn.run_sync(_validate_schema_sync, self._table)
              # S2 / lease_ck: these two probes catch drift that `alembic revision
              # --autogenerate` cannot remediate — its index comparator ignores
              # postgresql_where and it has no check-constraint comparator at all.
              # Collect them separately so the raised error can point operators at
              # the hand-written-migration recipe (_AUTOGEN_BLIND_HINT) only when
              # one of them actually fired.
              blind_errors = await conn.run_sync(_validate_index_predicates_sync, self._table)
              blind_errors.extend(await conn.run_sync(_validate_check_constraints_sync, self._table))
              errors.extend(blind_errors)
              if self._dlq_table is not None:
                  errors.extend(await conn.run_sync(_validate_dlq_schema_sync, self._dlq_table))
          if errors:
              raise RuntimeError(
                  _compose_schema_mismatch_message(errors, has_blind_drift=bool(blind_errors)),
              )
  ```

- [ ] **Step 4: Run the affected integration tests to verify they pass**

  Run each separately (single `-k` keyword each):
  - `just test tests/test_integration.py -k test_validate_schema_fails_when_lease_check_constraint_missing`
  - `just test tests/test_integration.py -k test_validate_schema_fails_when_columns_missing`
  - `just test tests/test_integration.py -k test_validate_schema_fails_when_lease_check_constraint_predicate_wrong`
  Expected: PASS for each. The third confirms the predicate-drift path also still
  raises (it now flows through the helper with `has_blind_drift=True`).

- [ ] **Step 5: Commit**

  ```bash
  git add faststream_outbox/client.py tests/test_integration.py
  git commit -m "fix(client): point operators at the migration recipe on autogen-blind drift

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Document the hand-written-migration recipe

**Files:**
- Modify: `docs/operations/alembic.md`
- Modify: `docs/usage/schema-validation.md`

Add the anchored section the error points to, and cross-link it from the schema
validation page.

- [ ] **Step 1: Add the recipe section to `docs/operations/alembic.md`**

  Insert the following **between** the end of the "Drift detection in CI" section
  (after the server-defaults paragraph that ends at line 158) and the
  `## DLQ retention via partition drop { #dlq-retention-via-partition-drop }`
  heading (line 160):

  ````markdown
  ## Fixing drift autogenerate can't see { #fixing-drift-autogenerate-cant-see }

  Two kinds of drift that
  [`validate_schema()`](../usage/schema-validation.md) reports **cannot** be
  remediated by `alembic revision --autogenerate` — the same blindness that
  let them drift in also stops autogenerate from emitting a fix:

  - **The `outbox_lease_ck` CHECK constraint.** Alembic's `compare_metadata`
    has no check-constraint comparator, so a missing or altered CHECK never
    appears in an autogenerated migration.
  - **Partial-index predicates.** Alembic's index comparator ignores
    `postgresql_where`, so an index that exists but was created non-partial,
    with the wrong `WHERE`, or (for `outbox_timer_id_uq`) non-unique is
    invisible to the diff.

  When `validate_schema()` raises for one of these, its error ends with a
  pointer to this section. Re-running autogenerate produces an empty
  `upgrade()` — hand-write the migration instead, then re-run
  `validate_schema()` to confirm the drift is cleared.

  ### Restore the lease CHECK

  ```python
  # Drop first ONLY if the constraint exists with a wrong predicate; skip the
  # drop if it is absent entirely.
  op.drop_constraint('outbox_lease_ck', 'outbox', type_='check')
  op.create_check_constraint(
      'outbox_lease_ck',
      'outbox',
      '(acquired_token IS NULL) = (acquired_at IS NULL)',
  )
  ```

  ### Restore a partial index

  Drop the drifted index and recreate it with its load-bearing predicate. The
  three indexes and their expected shape:

  | Index | Columns | Unique | `postgresql_where` |
  | --- | --- | --- | --- |
  | `outbox_pending_idx` | `queue, next_attempt_at` | no | `acquired_token IS NULL` |
  | `outbox_lease_idx` | `queue, acquired_at` | no | `acquired_token IS NOT NULL` |
  | `outbox_timer_id_uq` | `queue, timer_id` | yes | `timer_id IS NOT NULL` |

  ```python
  op.drop_index('outbox_timer_id_uq', table_name='outbox')
  op.create_index(
      'outbox_timer_id_uq',
      'outbox',
      ['queue', 'timer_id'],
      unique=True,
      postgresql_where=sa.text('timer_id IS NOT NULL'),
  )
  ```

  Substitute the columns / `unique` / predicate from the table above for
  `outbox_pending_idx` and `outbox_lease_idx`.
  ````

- [ ] **Step 2: Cross-link from `docs/usage/schema-validation.md`**

  Insert this paragraph immediately after the "Extras are intentionally ignored"
  paragraph (after line 39, before the `!!! warning "Server defaults..."`
  admonition):

  ```markdown
  Some drift cannot be fixed by re-running `alembic revision --autogenerate` — a
  missing/altered `outbox_lease_ck` CHECK or a drifted partial-index predicate.
  For those, the `RuntimeError` ends with a pointer to
  [Alembic migrations § Fixing drift autogenerate can't see](../operations/alembic.md#fixing-drift-autogenerate-cant-see),
  which holds the hand-written migration recipe.
  ```

- [ ] **Step 3: Build the docs strict to verify anchors resolve**

  Run: `just docs-build`
  Expected: `mkdocs build --strict` succeeds with no warnings — the new
  `#fixing-drift-autogenerate-cant-see` anchor resolves and the cross-link from
  `schema-validation.md` does not 404.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/operations/alembic.md docs/usage/schema-validation.md
  git commit -m "docs: hand-written migration recipe for autogen-blind drift

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: Full verification

**Files:** none (verification only).

Confirm lint, the full test suite, and the 100% coverage gate all pass with the
changes in place.

- [ ] **Step 1: Lint**

  Run: `just lint-ci`
  Expected: clean — `eof-fixer`, `ruff format --check`, `ruff check`, `ty check`
  all pass. (If `_compose_schema_mismatch_message` or the constants trip an unused
  import in `test_unit.py`, fix the import list rather than suppressing.)

- [ ] **Step 2: Full test suite with coverage gate**

  Run: `just test`
  Expected: all tests pass and `--cov-fail-under=100` is satisfied — the helper's
  three branches are covered by the Task 1 unit tests; the `has_blind_drift` True
  and False paths in `validate_schema` are covered by the Task 2 integration tests.

- [ ] **Step 3: Commit (only if Step 1/2 required fixups)**

  ```bash
  git add -A
  git commit -m "chore: lint/coverage fixups for autogen-blind drift hint

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

## On merge

Move this bundle to `planning/changes/` with `status: shipped`, `pr:`,
and `outcome:` filled, and promote the conclusion into the affected
architecture record: note in `CLAUDE.md`'s "User-owned schema" section (and any
`architecture/` deep-dive covering `validate_schema`) that an Alembic-blind
drift error now carries a remediation pointer to
`docs/operations/alembic.md#fixing-drift-autogenerate-cant-see`.
