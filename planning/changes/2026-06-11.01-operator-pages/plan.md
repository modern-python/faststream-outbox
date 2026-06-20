---
status: shipped
date: 2026-06-11
slug: operator-pages
spec: operator-pages
pr: "53"
---

# operator-pages — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three operator-facing pages (Production checklist,
Troubleshooting playbook, Alembic migrations) under a new
`docs/operations/` directory and a fifth top-level **Operations** nav
section, with five one-line cross-link callouts from existing reference
pages.

**Spec:** [`planning/active/2026-06-11-operator-pages-design.md`](./2026-06-11-operator-pages-design.md)

**Branch:** `docs/operator-pages`

**Commit strategy:** Per-task commits. Tasks 2–7 each produce a
commit and each leaves the docs site `--strict`-buildable. Task 8 is
verification-only.

---

### Task 1: Branch + commit spec, plan, and README index update

**Files:**
- Create: `planning/active/2026-06-11-operator-pages-design.md` (already drafted in working tree)
- Create: `planning/active/2026-06-11-operator-pages-plan.md` (this file, already drafted)
- Modify: `planning/README.md`

Land the planning artifacts and surface them in the index before
touching any docs content.

- [ ] **Step 1: Create the feature branch from `main`**

  Run: `git switch -c docs/operator-pages`
  Expected: `Switched to a new branch 'docs/operator-pages'`.

- [ ] **Step 2: Update `planning/README.md`**

  Replace the `## Active` block. Current state (post-#52):

  ```markdown
  ## Active

  _None._
  ```

  Becomes:

  ```markdown
  ## Active

  - **[operator-pages](active/2026-06-11-operator-pages-design.md)**
    — Three new pages under a new `docs/operations/` section:
    Production checklist, Troubleshooting playbook, Alembic
    migrations. The B follow-on from #50.
  ```

- [ ] **Step 3: Commit**

  ```bash
  git add planning/active/2026-06-11-operator-pages-design.md \
          planning/active/2026-06-11-operator-pages-plan.md \
          planning/README.md
  git commit -m "docs: spec + plan for operator pages

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Alembic autogenerate spike → update spec

**Files:**
- Modify: `planning/active/2026-06-11-operator-pages-design.md` (paste captured output into §4a)

The spec §4a calls for the **literal `alembic revision --autogenerate`
output** against `make_outbox_table()`. Capture it now so Task 6 has a
verbatim sample to paste, and so the spec stays the canonical reference
for what autogenerate produces.

The spike can be done two ways; pick whichever you find faster.

**Option A — programmatic.** Write a one-off Python script under
`/tmp/` that uses Alembic's autogenerate APIs (the same APIs
`broker.validate_schema()` already uses internally — see
`src/faststream_outbox/_schema_validation.py`). Construct a `MetaData`
holding `make_outbox_table(metadata, table_name="outbox")`, attach to a
running Postgres connection, call `produce_migrations` /
`render_python_code`, print the rendered upgrade ops.

**Option B — actual Alembic env.** Set up a minimal Alembic project
under `/tmp/alembic-spike/`: `alembic.ini`, `env.py` that imports
`make_outbox_table` and sets `target_metadata`, run
`alembic revision --autogenerate -m "initial"` against an empty
Postgres, open the generated migration file.

Either way, Postgres must be running. The repo's `compose.yml` already
provides one: `docker compose up -d postgres` (port 5432).

- [ ] **Step 1: Start Postgres if not running**

  Run: `docker compose up -d postgres`
  Expected: container ready; `pg_isready` succeeds on `localhost:5432`.

- [ ] **Step 2: Run the spike (Option A or B)**

  Capture the rendered Python upgrade ops (`op.create_table(...)`,
  `op.create_index(...)` calls) plus their column lists and index
  predicates.

- [ ] **Step 3: Paste into spec §4a**

  In `planning/active/2026-06-11-operator-pages-design.md` §4a
  "Initial migration", under a new "Captured autogenerate output"
  subsection, paste the literal rendered Python verbatim inside a
  ` ```python ` block. Add a one-line preamble noting the
  SQLAlchemy + Alembic versions used (read from
  `uv pip list | grep -E "sqlalchemy|alembic"`).

- [ ] **Step 4: Verify the partial-index predicates match the spec's
  prediction**

  The spec predicts three partial indexes:
  - `(queue, next_attempt_at) WHERE acquired_token IS NULL`
  - `(queue, acquired_at) WHERE acquired_token IS NOT NULL`
  - unique `(queue, timer_id) WHERE timer_id IS NOT NULL`

  If the captured output differs (extra index, different predicate),
  STOP and flag — the spec's §4a section needs updating before Task 6
  can write the page truthfully.

- [ ] **Step 5: Commit**

  ```bash
  git add planning/active/2026-06-11-operator-pages-design.md
  git commit -m "spec: capture alembic autogenerate output for operator-pages

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: New Operations nav section + three stub pages

**Files:**
- Create: `docs/operations/checklist.md` (stub)
- Create: `docs/operations/troubleshooting.md` (stub)
- Create: `docs/operations/alembic.md` (stub)
- Modify: `mkdocs.yml`

Land the new directory, three stub files (H1 only), and the new
nav section all at once. `mkdocs build --strict` succeeds because
each file in the nav exists; the stubs render as nearly-empty pages
but that is fine until Tasks 4–6 fill them in.

- [ ] **Step 1: Create three stub files**

  Each contains nothing but the H1 expected for the page:

  - `docs/operations/checklist.md` → `# Production checklist`
  - `docs/operations/troubleshooting.md` → `# Troubleshooting`
  - `docs/operations/alembic.md` → `# Alembic migrations`

- [ ] **Step 2: Add the Operations nav section to `mkdocs.yml`**

  Append after the existing `Reference:` block (per spec §1):

  ```yaml
    - Operations:
        - Production checklist: operations/checklist.md
        - Troubleshooting: operations/troubleshooting.md
        - Alembic migrations: operations/alembic.md
  ```

- [ ] **Step 3: Smoke-build**

  Run: `just docs-build`
  Expected: clean. Five sections in sidebar, all three new pages
  present (as near-empty stubs).

- [ ] **Step 4: Commit**

  ```bash
  git add docs/operations/ mkdocs.yml
  git commit -m "docs: scaffold Operations nav section and three operator pages

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: Production checklist content

**Files:**
- Modify: `docs/operations/checklist.md`

Write the full checklist per [spec §2
](./2026-06-11-operator-pages-design.md#2-production-checklist-docsoperationschecklistmd).
Six sections in this exact order: Sizing → Subscribers → DLQ → Drain &
lifecycle → Schema → Observability. Each item is a checkbox bullet
(`- [ ] **...**`) of one-to-two lines plus a relative link into the
existing reference page that owns the underlying detail.

The spec § lists all sixteen items verbatim; transcribe them rather
than inventing new ones. The page's purpose is to surface existing
references, not to re-document them.

- [ ] **Step 1: Write `docs/operations/checklist.md`**

  Header structure:

  ```markdown
  # Production checklist

  Scannable scaffold of pre-launch checks. Each item is one to two
  lines; the link points at the existing reference page that owns the
  full story.

  ## Sizing
  ...
  ## Subscribers
  ...
  ## DLQ
  ...
  ## Drain & lifecycle
  ...
  ## Schema
  ...
  ## Observability
  ...
  ```

- [ ] **Step 2: Smoke-build**

  Run: `just docs-build`
  Expected: clean. All `../usage/...` relative links resolve.

- [ ] **Step 3: Commit**

  ```bash
  git add docs/operations/checklist.md
  git commit -m "docs: production checklist content

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 5: Troubleshooting playbook content

**Files:**
- Modify: `docs/operations/troubleshooting.md`

Write the full playbook per [spec §3
](./2026-06-11-operator-pages-design.md#3-troubleshooting-docsoperationstroubleshootingmd).
Eleven symptoms in this order:

1. `event=lease_lost` recurring in logs
2. Outbox row count grows + `lease_lost` spike
3. Outbox row count grows, no `lease_lost`
4. Idle dispatch latency > `max_fetch_interval`
5. Subscriber blocks at `broker.start()`
6. Duplicate handler invocations
7. Rolling deploy leaks rows
8. `activate_in` / `activate_at` fires immediately in tests
9. `AckPolicy.ACK_FIRST` raises `ValueError` at registration
10. `OutboxResponse(...)` + foreign-publisher decorator gets nacked
11. `validate_schema()` raises `ImportError`

- [ ] **Step 1: Write the TOC table at the top**

  Two-column table (Symptom | Likely cause) per spec §3. Each row is
  also an anchor link to the `##` heading below.

- [ ] **Step 2: Write each `##` subsection**

  Use the five-field shape per spec (Symptom / Likely cause /
  Diagnose / Fix / Reference). The spec's worked example for
  `event=lease_lost` is the canonical template; match its tone and
  field order for the other ten.

  Reference fields link into the existing reference pages — keep
  links relative (`../usage/...`, `../introduction/...`).

- [ ] **Step 3: Smoke-build**

  Run: `just docs-build`
  Expected: clean. TOC table anchors resolve to the `##` headings
  below.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/operations/troubleshooting.md
  git commit -m "docs: troubleshooting playbook content

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 6: Alembic migrations page content

**Files:**
- Modify: `docs/operations/alembic.md`

Write the full page per [spec §4
](./2026-06-11-operator-pages-design.md#4-alembic-migrations-docsoperationsalembicmd).
Four sections:

- §4a Initial migration — paste the **captured autogenerate output
  from Task 2** verbatim, then annotate inline why each piece (table,
  partial index, partial unique index, column type) exists. The
  annotations explain that each partial index's predicate is
  load-bearing for fetch performance.
- §4b Adding the DLQ after the fact — describe the additive nature
  (only `create_table` + a single non-unique index; no `alter_table`
  on the outbox), and show the analogous autogenerate output for
  `make_dlq_table(metadata, table_name="outbox_dlq")` (re-run the
  spike for this case during this task, or include a "run
  autogenerate against your `MetaData` to see the exact output for
  your DLQ table name" placeholder if a re-spike is impractical).
- §4c Drift detection in CI — show the small standalone
  `validate_schema()` script from spec §4c verbatim. Explain why this
  is opt-in for `/health` and belongs between `alembic upgrade head`
  and the deploy step in CI.
- §4d DLQ retention via partition drop — walk through the Alembic
  ops for converting the DLQ from a plain table to one partitioned
  by `failed_at`, plus the monthly cron SQL for creating next
  month's partition and dropping the oldest.

- [ ] **Step 1: Write the page**

  Use level-2 headings for the four sub-sections so they appear in
  the page TOC. Code blocks for Alembic op samples and the CI
  script.

- [ ] **Step 2: Re-run the autogenerate spike for `make_dlq_table`**

  Same mechanism as Task 2, but with `make_dlq_table(metadata,
  table_name="outbox_dlq")` added to the metadata. Capture the
  additional `create_table("outbox_dlq", ...)` + `create_index(...)`
  ops. Paste into §4b.

  If §4b's autogenerate sample is impractical to capture for any
  reason (Postgres unavailable, etc.), STOP and flag — the page
  needs the verbatim sample to be useful.

- [ ] **Step 3: Smoke-build**

  Run: `just docs-build`
  Expected: clean.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/operations/alembic.md
  git commit -m "docs: alembic migrations page content

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 7: Index decision-tree row + five cross-link callouts

**Files:**
- Modify: `docs/index.md`
- Modify: `docs/usage/subscriber.md`
- Modify: `docs/usage/dlq.md`
- Modify: `docs/usage/schema-validation.md`

Add the one new row to the landing page's decision-tree table per
spec §1, and the five one-line "see also" callouts per spec §5.

- [ ] **Step 1: Add the decision-tree row in `docs/index.md`**

  Insert before the existing "Install and write the first publisher /
  subscriber" row:

  ```markdown
  | Deploy to production safely | [Production checklist](operations/checklist.md) |
  ```

- [ ] **Step 2: Add the five callouts** per the spec §5 table:

  | Existing page | Section | Callout text |
  |---|---|---|
  | `docs/usage/subscriber.md` | Connection budget (end) | `_Operator-side: [Production checklist § Sizing](../operations/checklist.md#sizing)._` |
  | `docs/usage/subscriber.md` | Slow handlers — dedicated queue (end) | `_See also [Troubleshooting § event=lease_lost](../operations/troubleshooting.md#event-lease_lost-recurring-in-logs)._` |
  | `docs/usage/dlq.md` | Metric: dlq_written (end) | `_Operator playbook: [Production checklist § DLQ](../operations/checklist.md#dlq)._` |
  | `docs/usage/dlq.md` | Retention (end) | `_Step-by-step: [Alembic migrations § DLQ retention via partition drop](../operations/alembic.md#dlq-retention-via-partition-drop)._` |
  | `docs/usage/schema-validation.md` | Where to call it (end) | `_CI recipe: [Alembic migrations § Drift detection in CI](../operations/alembic.md#drift-detection-in-ci)._` |

  All callouts use italics + en-dash style for consistency with the
  Comparison callouts the `docs-landing-and-comparison` PR (#50)
  landed.

- [ ] **Step 3: Smoke-build**

  Run: `just docs-build`
  Expected: clean. All five callout cross-links resolve to anchors
  inside the new operator pages (Tasks 4–6).

- [ ] **Step 4: Commit**

  ```bash
  git add docs/index.md docs/usage/subscriber.md docs/usage/dlq.md docs/usage/schema-validation.md
  git commit -m "docs: cross-link callouts from existing pages into operator pages

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 8: Verify

**Files:** none modified; no commit produced.

- [ ] **Step 1: Full strict build**

  Run: `just docs-build`
  Expected: clean.

- [ ] **Step 2: Lint pass**

  Run: `just lint`
  Expected: `eof-fixer`, `ruff format`, `ruff check`, `ty check`
  all pass. Markdown EOF + YAML formatting on `mkdocs.yml` are the
  only things touched in this PR.

- [ ] **Step 3: Manual sidebar scan**

  Run: `just docs-serve`
  Open the served site. Confirm:

  - Sidebar shows five sections in order: Overview → Getting
    started → Concepts → Guides → Reference → Operations.
  - Operations contains three pages in order: Production checklist,
    Troubleshooting, Alembic migrations.
  - The decision-tree table on the landing page has the new
    "Deploy to production safely" row pointing at the checklist.
  - The Comparison page (from #50) is still present and reachable
    under Concepts.
  - All eleven Troubleshooting TOC table entries scroll-to the
    matching `##` heading on click.
  - All sixteen Production-checklist items render with working
    relative links.
  - The Alembic page's autogenerate code blocks render verbatim with
    the right partial-index predicates (matches what Task 2 captured
    in the spec).

- [ ] **Step 4: Open the PR**

  Stop. Hand off to `superpowers:requesting-code-review` /
  `superpowers:finishing-a-development-branch` per the standard
  workflow.

  On merge, both halves of the pair move to `planning/archived/` and
  get `status: shipped`, `pr:`, and `outcome:` filled — same
  archive-PR pattern that #52 dogfooded.
