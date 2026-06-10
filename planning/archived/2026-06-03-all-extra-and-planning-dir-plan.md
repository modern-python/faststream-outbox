---
status: shipped
date: 2026-06-03
slug: all-extra-and-planning-dir
spec: all-extra-and-planning-dir
pr: "41"
---

# `all` extra and `planning/` workflow dir — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `faststream-outbox[all]` aggregate extra and bootstrap the `planning/` directory convention (`specs/` + `plans/`) used by the sister `httpware` project.

**Architecture:** No runtime code change. Three artifacts touched: `pyproject.toml` (one new line under `[project.optional-dependencies]`), `planning/specs/.gitkeep` + `planning/plans/.gitkeep` (zero-byte placeholders), and `CLAUDE.md` (one new `## Workflow` section between `## Commands` and `## Architecture`). All changes land in a single commit.

**Tech Stack:** `uv` (lock + sync), `just` (lint + test recipes), Python 3.13+, Postgres 17 (test target — not exercised by this plan but the test suite must still pass).

**Spec:** `planning/specs/2026-06-03-all-extra-and-planning-dir-design.md` (committed as `91201a2`).

---

## File map

| Path | Action | Responsibility |
|---|---|---|
| `pyproject.toml` | Modify (add one entry to `[project.optional-dependencies]`) | Expose `all` aggregate extra |
| `planning/specs/.gitkeep` | Create (zero bytes) | Keep dir tracked even if all specs are ever removed |
| `planning/plans/.gitkeep` | Create (zero bytes) | Keep dir tracked (this plan file will live here too, but `.gitkeep` is defensive per spec §2) |
| `CLAUDE.md` | Modify (insert new `## Workflow` section between line 18 and line 19) | Document the per-feature workflow lifecycle so future contributors find it |

No source files (`src/...`) or test files (`tests/...`) change. There are no new Python symbols.

---

## Task 1: Add `all` extra to `pyproject.toml`

**Files:**
- Modify: `pyproject.toml` (insert after line 21, the `opentelemetry` entry, inside the `[project.optional-dependencies]` table)

- [ ] **Step 1: Verify current state of the extras table**

Run:
```bash
sed -n '16,22p' /Users/kevinsmith/src/pypi/faststream-outbox/pyproject.toml
```

Expected output (exact):
```toml
[project.optional-dependencies]
asyncpg = ["asyncpg>=0.29"]
validate = ["alembic>=1.13"]
fastapi = ["fastapi>=0.95"]
prometheus = ["prometheus-client>=0.19"]
opentelemetry = ["opentelemetry-api>=1.20", "opentelemetry-sdk>=1.20"]
```

If the output differs, STOP — the file has drifted from the spec's assumptions and the spec needs revisiting.

- [ ] **Step 2: Add the `all` entry**

Use the Edit tool to append after the `opentelemetry` line. The new content immediately following `opentelemetry = [...]` should be:

```toml
all = ["faststream-outbox[asyncpg,validate,fastapi,prometheus,opentelemetry]"]
```

Concretely, the `old_string` for Edit is:
```toml
opentelemetry = ["opentelemetry-api>=1.20", "opentelemetry-sdk>=1.20"]
```

And the `new_string` is:
```toml
opentelemetry = ["opentelemetry-api>=1.20", "opentelemetry-sdk>=1.20"]
all = ["faststream-outbox[asyncpg,validate,fastapi,prometheus,opentelemetry]"]
```

- [ ] **Step 3: Verify TOML still parses**

Run:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run python -c "import tomllib, pathlib; data = tomllib.loads(pathlib.Path('pyproject.toml').read_text()); print(data['project']['optional-dependencies']['all'])"
```

Expected output:
```
['faststream-outbox[asyncpg,validate,fastapi,prometheus,opentelemetry]']
```

If it errors with a `tomllib.TOMLDecodeError`, the edit broke the file — re-open and fix.

- [ ] **Step 4: Verify `uv` can resolve `--extra all`**

Run:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv sync --extra all 2>&1 | tail -20
```

Expected: completes without errors. Look for `Resolved <N> packages` and `Installed` / `Audited` lines mentioning `alembic`, `asyncpg`, `fastapi`, `prometheus-client`, `opentelemetry-api`, `opentelemetry-sdk`. (If the env already had them via `dev` group, the output may just say `Audited <N> packages in <ms>` — that's also success.)

If it fails with `failed to resolve`, the most likely cause is the self-referential extra name not matching the project name. Verify the project name with:
```bash
grep '^name =' /Users/kevinsmith/src/pypi/faststream-outbox/pyproject.toml
```
It must read `name = "faststream-outbox"` (hyphen, not underscore).

- [ ] **Step 5: Verify `--extra all` matches `--all-extras`**

Run:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && \
  uv sync --extra all --quiet && uv pip list --format=freeze | sort > /tmp/with-all.txt && \
  uv sync --all-extras --quiet && uv pip list --format=freeze | sort > /tmp/with-all-extras.txt && \
  diff /tmp/with-all.txt /tmp/with-all-extras.txt && echo "MATCH"
```

Expected: `MATCH` on stdout, no diff output. If diff prints lines, the `all` extra is missing or has extras the broad form doesn't — re-check the entry against the spec.

Cleanup:
```bash
rm -f /tmp/with-all.txt /tmp/with-all-extras.txt
```

**Do not commit yet — single commit at the end of Task 4.**

---

## Task 2: Create `planning/` placeholder files

**Files:**
- Create: `planning/specs/.gitkeep` (zero bytes)
- Create: `planning/plans/.gitkeep` (zero bytes)

The directories `planning/specs/` and `planning/plans/` already exist on disk (created during the brainstorming step that produced the spec) and `planning/specs/2026-06-03-all-extra-and-planning-dir-design.md` is already tracked. Only the `.gitkeep` placeholders need adding.

- [ ] **Step 1: Confirm the directories exist**

Run:
```bash
ls -la /Users/kevinsmith/src/pypi/faststream-outbox/planning/
```

Expected: shows `specs/` and `plans/` subdirectories. If either is missing, run `mkdir -p /Users/kevinsmith/src/pypi/faststream-outbox/planning/{specs,plans}` first.

- [ ] **Step 2: Create both `.gitkeep` files**

Run:
```bash
touch /Users/kevinsmith/src/pypi/faststream-outbox/planning/specs/.gitkeep && \
touch /Users/kevinsmith/src/pypi/faststream-outbox/planning/plans/.gitkeep
```

- [ ] **Step 3: Verify both files exist and are empty**

Run:
```bash
wc -c /Users/kevinsmith/src/pypi/faststream-outbox/planning/specs/.gitkeep /Users/kevinsmith/src/pypi/faststream-outbox/planning/plans/.gitkeep
```

Expected output (byte counts of 0):
```
0 .../planning/specs/.gitkeep
0 .../planning/plans/.gitkeep
0 total
```

If `eof-fixer` later complains about the files (it generally leaves zero-byte files alone, but if it doesn't), accept whatever it does — the file existing under the name `.gitkeep` is what matters; the byte count is incidental.

**Do not commit yet.**

---

## Task 3: Add `## Workflow` section to `CLAUDE.md`

**Files:**
- Modify: `CLAUDE.md` (insert a new `## Workflow` section between line 18 and line 19, i.e. between the blank line that ends `## Commands` and the `## Architecture` heading)

- [ ] **Step 1: Verify the insertion point**

Run:
```bash
sed -n '15,21p' /Users/kevinsmith/src/pypi/faststream-outbox/CLAUDE.md
```

Expected output (exact):
```
- `just build` / `just down` / `just sh` — image build, teardown, shell into the app container.

`tests/test_unit.py` and `tests/test_fake.py` need no Postgres — runnable with `uv run pytest tests/test_unit.py` directly. `tests/test_integration.py` requires Postgres at `POSTGRES_DSN` (default `postgresql+asyncpg://outbox:outbox@localhost:5432/outbox`); the `pg_engine` fixture skips if unreachable. Coverage is on by default (`pyproject.toml` `addopts`) with a strict `--cov-fail-under=100` ratchet — partial runs (`pytest -k name`, a single test file, etc.) will fail that gate. Pass `--no-cov` or `--cov-fail-under=0` when iterating locally on a subset; the full `just test` run satisfies the gate.

## Architecture

The package wires a FastStream `Broker`/`Registrator`/`Subscriber` trio whose transport is Postgres rows, not a message bus.
```

If the output differs, the file has drifted — STOP and re-locate the insertion point manually.

- [ ] **Step 2: Insert the `## Workflow` section**

Use the Edit tool with this exact replacement.

`old_string` (matches the blank-line / `## Architecture` boundary unambiguously because `## Architecture` only appears once in the file):

```
gate.

## Architecture
```

`new_string`:

```
gate.

## Workflow

Per-feature workflow: brainstorming → spec in `planning/specs/YYYY-MM-DD-<slug>-design.md` → writing-plans → plan in `planning/plans/YYYY-MM-DD-<slug>-plan.md` → executing-plans / subagent-driven-development → requesting-code-review → finishing-a-development-branch.

Topic slugs are kebab-case descriptions (e.g. `dlq-on-terminal-failure`), not story IDs.

## Architecture
```

Note: the surrounding `gate.` line is the tail of the long Commands paragraph (the last word of line 17). Anchoring on it makes `old_string` unique even though the markdown structure (blank line + heading) is otherwise generic.

- [ ] **Step 3: Verify the section landed**

Run:
```bash
grep -n '^## ' /Users/kevinsmith/src/pypi/faststream-outbox/CLAUDE.md
```

Expected output (order matters):
```
5:## Project
9:## Commands
19:## Workflow
25:## Architecture
...
```

(Architecture's new line number will be 25 or thereabouts depending on whitespace — the key check is that `## Workflow` appears between `## Commands` and `## Architecture`.)

- [ ] **Step 4: Verify the section body is correct**

Run:
```bash
sed -n '/^## Workflow/,/^## Architecture/p' /Users/kevinsmith/src/pypi/faststream-outbox/CLAUDE.md
```

Expected output:
```
## Workflow

Per-feature workflow: brainstorming → spec in `planning/specs/YYYY-MM-DD-<slug>-design.md` → writing-plans → plan in `planning/plans/YYYY-MM-DD-<slug>-plan.md` → executing-plans / subagent-driven-development → requesting-code-review → finishing-a-development-branch.

Topic slugs are kebab-case descriptions (e.g. `dlq-on-terminal-failure`), not story IDs.

## Architecture
```

**Do not commit yet.**

---

## Task 4: Verify and commit

**Files:** none new — this task just runs verification + creates the commit.

- [ ] **Step 1: Run `just lint-ci`**

Run:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && just lint-ci
```

Expected: all checks pass (`eof-fixer`, `ruff format --check`, `ruff check`, `ty check`). No Python files were touched in this plan, so `ruff` and `ty` are effectively no-ops on the diff; `eof-fixer` may report something about `.gitkeep` or `CLAUDE.md` — if it modifies them, re-run `just lint-ci` and confirm clean.

If `just` is unavailable, the equivalent is:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && \
  uv run eof-fixer --check . && \
  uv run ruff format --check . && \
  uv run ruff check . && \
  uv run ty check .
```

- [ ] **Step 2: Run the test suite**

Run:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && just test
```

Expected: full suite passes with 100% coverage (the `--cov-fail-under=100` gate). No behavior change is expected — this confirms `pyproject.toml` is still syntactically valid and the package still installs cleanly in the docker compose env.

If `just test` is impractical (docker not available), at minimum run the no-Postgres subset:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run pytest tests/test_unit.py tests/test_fake.py --no-cov
```
…and note in the commit description that the integration suite was not exercised locally.

- [ ] **Step 3: Inspect the full diff one last time**

Run:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git status && echo '---' && git diff && echo '---' && git diff --stat
```

Expected `git status` shows:
- Modified: `CLAUDE.md`
- Modified: `pyproject.toml`
- (Possibly) Modified: `uv.lock` if the `--extra all` sync touched it
- Untracked: `planning/plans/.gitkeep`, `planning/specs/.gitkeep`

(The plan file `planning/plans/2026-06-03-all-extra-and-planning-dir-plan.md` should already have been committed in a separate `docs: plan ...` commit ahead of execution — analogous to how the spec was committed at `91201a2`. If it still shows as untracked, commit it on its own *before* proceeding to Step 4.)

`git diff --stat` should show ~3 modified lines in `pyproject.toml` (one added entry, possibly reformatted), and a small CLAUDE.md insertion (~6 lines).

If `uv.lock` shows a diff, that's fine — include it in the commit (lock files belong with the change that produced them).

- [ ] **Step 4: Stage the changes**

Run:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add \
  pyproject.toml \
  CLAUDE.md \
  planning/specs/.gitkeep \
  planning/plans/.gitkeep
```

If `uv.lock` is also modified:
```bash
git add uv.lock
```

Verify staged set:
```bash
git status
```

Expected: all of the above under `Changes to be committed`, nothing under `Changes not staged for commit` (other than possibly untracked files outside this work).

- [ ] **Step 5: Commit**

Run:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git commit -m "$(cat <<'EOF'
chore: add 'all' extra and planning/ workflow directory

- pyproject.toml: self-referential `all` aggregate so users can
  `pip install faststream-outbox[all]` for the full feature set
  (asyncpg + validate + fastapi + prometheus + opentelemetry).
- planning/{specs,plans}/: adopt the layout used by the sister
  httpware project for superpowers spec/plan artifacts.
- CLAUDE.md: document the per-feature workflow lifecycle in a new
  `## Workflow` section between `## Commands` and `## Architecture`.

Spec: planning/specs/2026-06-03-all-extra-and-planning-dir-design.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 6: Confirm the commit landed**

Run:
```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git log -1 --stat
```

Expected: one new commit with the message above and the file list from step 4.

---

## Acceptance criteria (spec §"Verification checklist")

After Task 4 completes, all of these must be true:

- [ ] `uv sync --extra all` succeeds and pulls in alembic, asyncpg, fastapi, prometheus-client, opentelemetry-api, opentelemetry-sdk. (Task 1, Step 4.)
- [ ] `uv sync --extra all` and `uv sync --all-extras` produce the same installed set. (Task 1, Step 5.)
- [ ] `just lint-ci` passes. (Task 4, Step 1.)
- [ ] `just test` passes. (Task 4, Step 2.)
- [ ] `planning/specs/` and `planning/plans/` exist and are tracked by git (the design doc, this plan, and both `.gitkeep` files appear in `git ls-files`). Verify with: `git ls-files planning/`.
- [ ] `CLAUDE.md` `## Workflow` heading appears between `## Commands` and `## Architecture`. (Task 3, Step 3.)
