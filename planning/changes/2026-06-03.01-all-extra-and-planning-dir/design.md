---
summary: Add faststream-outbox[all] aggregate extra; bootstrap the planning/ directory itself.
---

# Design: `all` aggregate extra + `planning/` workflow directory

## Summary

Two independent project-hygiene changes:

1. Add a self-referential `all` aggregate to `[project.optional-dependencies]`
   so users can `pip install faststream-outbox[all]` for the full feature set.
2. Adopt the `planning/` directory convention (`planning/specs/`,
   `planning/plans/`) for superpowers spec/plan artifacts, mirroring the sister
   `httpware` project. Document the workflow in `CLAUDE.md`.

Neither change touches runtime code, test code, or public API.

## Motivation

- **`all` extra:** `faststream-outbox` currently exposes five optional extras
  (`asyncpg`, `validate`, `fastapi`, `prometheus`, `opentelemetry`). Users
  wanting the full feature set must enumerate every extra. An `all` aggregate
  is the standard PyPI convention and is one line to maintain.
- **`planning/` directory:** Superpowers skills default to
  `docs/superpowers/specs/` for design artifacts. The user's sister project
  (`httpware`) uses `planning/specs/` and `planning/plans/` as a flat, top-level
  workflow root, and has documented the per-feature lifecycle there. Adopting
  the same layout keeps the two repos navigable in the same way and surfaces
  the workflow earlier than burying it under `docs/`.

## Design

### 1. `all` extra (pyproject.toml)

Append one entry to `[project.optional-dependencies]`:

```toml
all = ["faststream-outbox[asyncpg,validate,fastapi,prometheus,opentelemetry]"]
```

**Rationale for self-reference over an explicit dep list:** the per-extra pin
sets (`asyncpg>=0.29`, `alembic>=1.13`, etc.) live in their own entries; an
explicit `all` list would duplicate them and risk drift when an individual
extra's pin is bumped. The self-referential form references the names, not the
versions, so version updates flow through automatically.

**Future-extra discipline:** adding a sixth extra still requires updating `all`.
Acceptable trade — the alternative (`--all-extras` everywhere) is not a
package-level surface, and a CI step that diffs `all` against the union of
other extras is overkill for a five-extra package.

**Verification command:** `uv sync --extra all` should resolve to the same
package set as `uv sync --all-extras`.

### 2. `planning/` directory layout

Create:

```
planning/
├── specs/
│   └── .gitkeep    # (this design doc populates the dir, but .gitkeep stays
│                   #  so the dir survives if all specs are ever removed)
└── plans/
    └── .gitkeep
```

`.gitkeep` files are zero-byte. `planning/specs/` will hold this design doc on
first commit, so the `.gitkeep` there is defensive but not strictly required;
`planning/plans/.gitkeep` is necessary because no plan exists yet.

No `deferred-work.md` scaffold. Will be added when there is actual deferred
work to track, not preemptively.

### 3. `.gitignore`

No change. The existing `plan.md` entry targets a file literally named
`plan.md` (legacy artifact), not the new `planning/plans/*.md` shape. Gitignore
patterns without wildcards or slashes match on exact basename at any depth, so
`plan.md` matches only `plan.md` — never `2026-06-03-<slug>-plan.md`.

### 4. `CLAUDE.md` — new `## Workflow` section

Insert between `## Commands` and `## Architecture`:

```markdown
## Workflow

Per-feature workflow: brainstorming → spec in
`planning/specs/YYYY-MM-DD-<slug>-design.md` → writing-plans →
plan in `planning/plans/YYYY-MM-DD-<slug>-plan.md` →
executing-plans / subagent-driven-development →
requesting-code-review → finishing-a-development-branch.

Topic slugs are kebab-case descriptions (e.g. `dlq-on-terminal-failure`),
not story IDs.
```

**Placement rationale:** `## Commands` is the second section a contributor
reads after `## Project`; placing `## Workflow` immediately after gives the
lifecycle convention visibility without breaking the long, narrative
`## Architecture` section that follows.

**Slug example chosen:** `dlq-on-terminal-failure` — matches the most recent
PR (#40) merged into this repo, so the example resolves to something a reader
can `git log --grep` for.

## Order of operations (single commit)

1. Edit `pyproject.toml` — add `all` line.
2. `mkdir -p planning/specs planning/plans` — already done as part of writing
   this spec.
3. `touch planning/specs/.gitkeep planning/plans/.gitkeep`.
4. Edit `CLAUDE.md` — insert `## Workflow` section.
5. `git add` the four touched paths + this spec file.
6. Single commit: `chore: add 'all' extra and planning/ workflow directory`.

## Out of scope

- No retroactive migration of historical design notes (none exist outside git
  commit messages).
- No changes to any superpowers skill files (`~/.claude/...`); the per-call
  `args` to `/superpowers:brainstorming` plus the new `CLAUDE.md` line are the
  only steering mechanism we need.
- No `bumpversion`, no changelog entry — version is currently `"0"` (sentinel),
  and there is no `CHANGELOG.md` in the repo.
- No `mkdocs.yml` change — `planning/` is contributor-facing, not user-facing
  docs.

## Verification checklist (for the plan / executing-plans phase)

- [ ] `uv sync --extra all` succeeds and pulls in alembic, asyncpg, fastapi,
      prometheus-client, opentelemetry-api, opentelemetry-sdk.
- [ ] `uv sync --extra all` and `uv sync --all-extras` produce the same
      installed package set (diff `uv pip list` output).
- [ ] `just lint-ci` still passes (no Python files touched, but `eof-fixer`
      runs over `.gitkeep` and `CLAUDE.md`).
- [ ] `just test` still passes (no behavior change expected, but confirms no
      pyproject.toml syntax regression).
- [ ] `planning/specs/` and `planning/plans/` exist in the working tree and
      are tracked by git.
- [ ] `CLAUDE.md` renders correctly — `## Workflow` heading shows up in the
      table-of-contents order: Project, Commands, Workflow, Architecture,
      Conventions.
