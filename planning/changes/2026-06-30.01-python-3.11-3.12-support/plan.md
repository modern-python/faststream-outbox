# python-3.11-3.12-support — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lower the supported-Python floor from 3.13 to 3.11 so the package
installs and runs on 3.11, 3.12, 3.13, and 3.14.

**Spec:** [`design.md`](./design.md)

**Architecture:** Pure-Python library, no compiled extensions. One source
construct (`override`, added to `typing` in 3.12) is backported via
`typing_extensions` declared as a direct runtime dependency, with no
`sys.version_info` gating. Everything else is metadata (`requires-python`,
classifiers, ruff target) plus a wider CI matrix.

**Tech Stack:** Python, faststream 0.7, sqlalchemy 2 (asyncio), anyio,
`typing_extensions`, uv (package manager), just (task runner), ruff + ty
(lint/type-check), pytest, docker compose (integration suite + Postgres 17).

**Branch:** `feat/python-3.11-3.12-support`

**Commit strategy:** Per-task commits.

## Global Constraints

- New Python floor: `requires-python = ">=3.11,<4"`. Every edit must stay valid
  on CPython 3.11 through 3.14.
- `typing-extensions>=4.12.0` (matches faststream's existing transitive pin;
  `override` has existed in `typing_extensions` since 4.4.0).
- All `import` statements at module top — never inside function bodies. Tests
  included. No `# noqa: PLC0415` to keep an import inline; hoist instead.
- Type-checker suppressions use `# ty: ignore[...]`, never `# type: ignore`.
- `uv.lock` is git-ignored — regenerate locally so resolution succeeds, but do
  **not** commit it.
- Coverage gate is 100% (`--cov-fail-under=100`) and only the full docker suite
  enforces it. Local 3.11 subset runs use `--no-cov` (import/runtime smoke
  check, not the coverage gate).
- Leave the existing `# ty: ignore[invalid-method-override]` comments on the
  decorated methods untouched — unrelated to this change.
- The uv-managed 3.11 interpreter for local verification:
  `/Users/kevinsmith/.local/share/uv/python/cpython-3.11.9-macos-aarch64-none/bin/python3.11`

---

### Task 1: Make the package compatible with Python 3.11/3.12

Lower the floor, add the `typing_extensions` dependency, and reroute the
`override` import in the four affected source files. The pyproject floor change
and the source fixes are inseparable: the `override` fix can only be proven by
importing on a real 3.11 interpreter, which uv refuses until `requires-python`
is lowered; and ruff `target-version` must move to `py311` alongside the source
edits so the lint pass validates against the floor.

**Files:**
- Modify: `pyproject.toml` (dependencies, `requires-python`, classifiers,
  `[tool.ruff] target-version`)
- Modify: `faststream_outbox/registrator.py:3`
- Modify: `faststream_outbox/publisher/usecase.py:13`
- Modify: `faststream_outbox/broker.py` (import block + `@typing.override` at
  204/212/222/293/335)
- Modify: `faststream_outbox/subscriber/usecase.py` (import block +
  `@typing.override` at 232/247/804/808/818/868)
- Regenerate (do **not** commit): `uv.lock`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: a package importable on 3.11. No public API names change; `override`
  keeps its meaning (re-exported stdlib object on 3.12+, backport on 3.11).

- [ ] **Step 1: Confirm the break on 3.11 (RED)**

  Show that the stdlib `override` import — used directly in two files and as
  `typing.override` in two others — does not exist on 3.11:

  ```bash
  PY311=/Users/kevinsmith/.local/share/uv/python/cpython-3.11.9-macos-aarch64-none/bin/python3.11
  $PY311 -c "from typing import override"
  ```

  Expected: FAIL with `ImportError: cannot import name 'override' from 'typing'`.
  This is the failing state the task fixes.

- [ ] **Step 2: Edit `pyproject.toml` — floor, dependency, classifiers, ruff target (all together)**

  Change the floor at line 9 from:

  ```toml
  requires-python = ">=3.13,<4"
  ```

  to:

  ```toml
  requires-python = ">=3.11,<4"
  ```

  In `classifiers` (lines 15-16), add 3.11 and 3.12 above 3.13 so the block reads:

  ```toml
      "Programming Language :: Python :: 3.11",
      "Programming Language :: Python :: 3.12",
      "Programming Language :: Python :: 3.13",
      "Programming Language :: Python :: 3.14",
  ```

  Change the `dependencies` block (lines 20-23) from:

  ```toml
  dependencies = [
      "faststream>=0.7.1,<0.8",
      "sqlalchemy[asyncio]>=2.0",
  ]
  ```

  to:

  ```toml
  dependencies = [
      "faststream>=0.7.1,<0.8",
      "sqlalchemy[asyncio]>=2.0",
      "typing-extensions>=4.12.0",
  ]
  ```

  In `[tool.ruff]` (line 65), change:

  ```toml
  target-version = "py313"
  ```

  to:

  ```toml
  target-version = "py311"
  ```

- [ ] **Step 3: Edit `faststream_outbox/registrator.py`**

  Line 3 is currently:

  ```python
  from typing import TYPE_CHECKING, Any, override
  ```

  Remove `override` from it and add a `typing_extensions` import. The result
  (ruff will normalize ordering in Step 7):

  ```python
  from typing import TYPE_CHECKING, Any

  from typing_extensions import override
  ```

  The `@override` decorators at lines 41 and 99 are unchanged.

- [ ] **Step 4: Edit `faststream_outbox/publisher/usecase.py`**

  Line 13 is currently:

  ```python
  from typing import override
  ```

  Replace it with:

  ```python
  from typing_extensions import override
  ```

  The `@override` decorators at lines 60, 104, 116 are unchanged.

- [ ] **Step 5: Edit `faststream_outbox/broker.py`**

  This file uses `import typing` (line 11) and the qualified form
  `@typing.override`. Add a direct import near the other third-party imports
  (e.g. after the `from sqlalchemy...` lines around line 30):

  ```python
  from typing_extensions import override
  ```

  Then replace the decorator at lines 204, 212, 222, 293, 335 — each currently:

  ```python
      @typing.override
  ```

  with:

  ```python
      @override
  ```

  Leave `import typing` in place (still used for `typing.Self`, `typing.Any`,
  etc.).

- [ ] **Step 6: Edit `faststream_outbox/subscriber/usecase.py`**

  This file uses `import typing` (line 27) and `@typing.override`. Add a direct
  import near the other third-party imports (after the `from faststream...`
  block, around line 38):

  ```python
  from typing_extensions import override
  ```

  Then replace the decorator at lines 232, 247, 804, 808, 818, 868 — each
  currently:

  ```python
      @typing.override
  ```

  with:

  ```python
      @override
  ```

  Leave `import typing` in place (still used elsewhere in the file).

- [ ] **Step 7: Lint (sorts the new imports) and type-check**

  ```bash
  just lint
  ```

  Expected: passes. `ruff --fix` sorts each new `from typing_extensions import
  override` into the third-party group; `ty` resolves `override` from
  `typing_extensions` cleanly. If `ty` reports anything, do not add new
  suppressions — fix the import.

- [ ] **Step 8: Regenerate the lockfile for the lowered floor (do not commit it)**

  ```bash
  uv lock
  ```

  Expected: resolves `typing-extensions` and re-pins for `>=3.11`. `uv.lock` is
  git-ignored; it will not appear in `git status`.

- [ ] **Step 9: Prove the fix on real 3.11 (GREEN)**

  Sync deps for 3.11 and run the two no-Postgres suites (which exercise the
  full import graph) without the coverage gate:

  ```bash
  uv sync --python 3.11 --all-extras
  uv run --python 3.11 --no-sync pytest tests/test_unit.py tests/test_fake.py --no-cov -q
  ```

  Expected: PASS. The package and all four edited modules import and run on
  CPython 3.11. (Re-sync the default interpreter afterward with `uv sync
  --all-extras` if you want the local venv back on 3.14.)

- [ ] **Step 10: Commit**

  ```bash
  git add pyproject.toml faststream_outbox/registrator.py \
    faststream_outbox/publisher/usecase.py faststream_outbox/broker.py \
    faststream_outbox/subscriber/usecase.py
  git commit -m "feat: support Python 3.11 and 3.12

Backport typing.override via typing_extensions, lower requires-python to
>=3.11, add 3.11/3.12 classifiers and ruff target."
  ```

---

### Task 2: Widen the CI matrix and finalize the bundle

Add 3.11 and 3.12 to the pytest matrix so the 100%-coverage suite runs on every
supported interpreter, and close out the planning bundle. No architecture
promotion is required — the design verified no `architecture/<capability>.md`
references the Python floor.

**Files:**
- Modify: `.github/workflows/_checks.yml` (pytest matrix)
- Modify: `planning/changes/2026-06-30.01-python-3.11-3.12-support/design.md`
  (finalize `summary:`)

**Interfaces:**
- Consumes: the lowered floor from Task 1 (CI installs each matrix interpreter
  via `uv python install ${{ matrix.python-version }}` and runs `pytest`).
- Produces: nothing later tasks rely on.

- [ ] **Step 1: Confirm no architecture page references the floor (guards the "no promotion" claim)**

  ```bash
  grep -rn "3\.13\|requires-python\|Python 3" architecture/ || echo "no floor references — no promotion needed"
  ```

  Expected: no matches (or only matches unrelated to the supported-version
  floor). If a real floor reference appears, update that page in this PR and
  note it here.

- [ ] **Step 2: Edit `.github/workflows/_checks.yml` — widen the pytest matrix**

  The matrix block (lines 24-26) is currently:

  ```yaml
        matrix:
          python-version:
            - "3.13"
            - "3.14"
  ```

  Change it to:

  ```yaml
        matrix:
          python-version:
            - "3.11"
            - "3.12"
            - "3.13"
            - "3.14"
  ```

  Leave the lint job's `uv python install 3.13` (line 16) unchanged — lint stays
  on 3.13.

- [ ] **Step 3: Finalize the bundle summary**

  In `planning/changes/2026-06-30.01-python-3.11-3.12-support/design.md`,
  confirm the front-matter `summary:` states the realized result (it already
  reads as the shipped outcome — adjust only if the implementation deviated from
  the spec).

- [ ] **Step 4: Validate the planning bundle**

  ```bash
  just check-planning
  ```

  Expected: `planning: OK`.

- [ ] **Step 5: Commit**

  ```bash
  git add .github/workflows/_checks.yml \
    planning/changes/2026-06-30.01-python-3.11-3.12-support/design.md
  git commit -m "ci: run pytest matrix on Python 3.11 and 3.12"
  ```

---

### Task 3: Open the PR and watch CI

Ship via PR (never local-merge). The widened matrix on real CI is the
authoritative verification — local runs only smoke-tested 3.11.

**Files:** none.

- [ ] **Step 1: Push the branch and open the PR**

  ```bash
  git push -u origin feat/python-3.11-3.12-support
  gh pr create --fill --base main
  ```

- [ ] **Step 2: Watch CI to green**

  ```bash
  gh pr checks --watch
  ```

  Expected: all matrix legs (3.11, 3.12, 3.13, 3.14) plus lint pass. The 100%
  coverage gate holds on each interpreter. If a 3.11/3.12-only failure surfaces
  (e.g. a dependency runtime difference), fix it on the branch and re-push —
  catching exactly that is why the matrix was widened.
