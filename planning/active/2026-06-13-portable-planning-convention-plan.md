---
status: draft
date: 2026-06-13
slug: portable-planning-convention
spec: portable-planning-convention
pr: null
---

# Portable planning-convention — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure `planning/` to the two-axis OpenSpec-shaped convention —
`architecture/` (root, unchanged) as living truth, `planning/changes/`
(`active/` → `archive/`) as folder-bundle change history, with `.NN`-tiebroken
ids, three lanes, dedicated `audits/`+`retros/`, `deferred.md`, and a portable
README — and migrate every existing artifact into it.

**Spec:** [`planning/active/2026-06-13-portable-planning-convention-design.md`](./2026-06-13-portable-planning-convention-design.md)

**Branch:** `docs/portable-planning-convention` (already created; spec already
committed there).

**Commit strategy:** Per-task commits. Pure docs / file moves — no runtime code,
no tests. Verification is grep + `mkdocs build --strict` + `just lint-ci` + a
tree check, all in the final task.

**No-code note:** This plan has no pytest/TDD cycle. "Verification" steps run
shell commands and assert on their output. Use `git mv` for every move so blame
follows.

---

## Reference: archived-pair → bundle-id mapping

Used by Task 2. `.NN` is assigned per date in PR-merge order (lower PR = `.01`).
PR numbers are from the current `planning/README.md` index.

| Old slug (in `planning/archived/`) | PR | New bundle folder (in `planning/changes/archive/`) | Has |
|---|---|---|---|
| `2026-06-03-all-extra-and-planning-dir` | #41 | `2026-06-03.01-all-extra-and-planning-dir/` | design+plan |
| `2026-06-03-faststream-0.7-migration` | #42 | `2026-06-03.02-faststream-0.7-migration/` | design+plan |
| `2026-06-04-faststream-0.7.1-testbroker-typing` | #43 | `2026-06-04.01-faststream-0.7.1-testbroker-typing/` | design+plan |
| `2026-06-04-foreign-broker-relay` | #44 | `2026-06-04.02-foreign-broker-relay/` | design+plan |
| `2026-06-09-mkdocs-github-pages` | #45 | `2026-06-09.01-mkdocs-github-pages/` | design+plan |
| `2026-06-09-drain-test-flaky-fetch-observation` | #48 | `2026-06-09.02-drain-test-flaky-fetch-observation/` | design+plan |
| `2026-06-10-planning-conventions` | #49 | `2026-06-10.01-planning-conventions/` | design only |
| `2026-06-10-docs-landing-and-comparison` | #50 | `2026-06-10.02-docs-landing-and-comparison/` | design+plan |
| `2026-06-11-operator-pages` | #53 | `2026-06-11.01-operator-pages/` | design+plan |
| `2026-06-11-docs-tutorials-and-observability-split` | #56 | `2026-06-11.02-docs-tutorials-and-observability-split/` | design+plan |
| `2026-06-12-docs-tutorials` | #58 | `2026-06-12.01-docs-tutorials/` | design+plan |

Findings (Task 3) → `planning/audits/`, names unchanged:
`2026-06-12-code-audit-findings.md`, `2026-06-12-docs-audit-findings.md`.

---

### Task 1: Scaffold new dirs and move this change's own bundle

**Files:**
- Create: `planning/changes/active/2026-06-13.01-portable-planning-convention/design.md` (moved)
- Create: `planning/changes/active/2026-06-13.01-portable-planning-convention/plan.md` (moved)
- Create: `planning/changes/active/.gitkeep`, `planning/changes/archive/.gitkeep`

- [ ] **Step 1: Create the changes/ skeleton**

  ```bash
  mkdir -p planning/changes/active planning/changes/archive
  touch planning/changes/active/.gitkeep planning/changes/archive/.gitkeep
  ```

- [ ] **Step 2: Move this change's design + plan into a bundle folder**

  ```bash
  mkdir -p planning/changes/active/2026-06-13.01-portable-planning-convention
  git mv planning/active/2026-06-13-portable-planning-convention-design.md \
         planning/changes/active/2026-06-13.01-portable-planning-convention/design.md
  git mv planning/active/2026-06-13-portable-planning-convention-plan.md \
         planning/changes/active/2026-06-13.01-portable-planning-convention/plan.md
  ```

- [ ] **Step 3: Update this bundle's internal cross-links + status**

  In the moved `plan.md` frontmatter, leave `spec: portable-planning-convention`
  as-is. In the moved `plan.md` body, fix the **Spec:** link to the sibling:
  change `[`planning/active/2026-06-13-portable-planning-convention-design.md`](./2026-06-13-portable-planning-convention-design.md)`
  to `[`design.md`](./design.md)`.

  In the moved `design.md` body, two references point at the pre-migration
  path `../archived/2026-06-10-planning-conventions-design.md` (one in Summary,
  one in the Non-goals "index generator" bullet). Rewrite both to the
  post-migration bundle path
  `../../archive/2026-06-10.01-planning-conventions/design.md` (relative from
  `changes/active/2026-06-13.01-…/`). This link resolves after Task 2 creates
  that bundle.

  ```bash
  d=planning/changes/active/2026-06-13.01-portable-planning-convention/design.md
  grep -c "archived/2026-06-10-planning-conventions-design.md" "$d"   # expect 2
  # after editing:
  grep -c "../../archive/2026-06-10.01-planning-conventions/design.md" "$d"  # expect 2
  ```

  Set `status: approved` in both `design.md` and `plan.md` frontmatter (the spec
  is approved; the plan is being executed).

- [ ] **Step 4: Verify the move**

  ```bash
  ls planning/changes/active/2026-06-13.01-portable-planning-convention/
  ```
  Expected: `design.md  plan.md`

- [ ] **Step 5: Commit**

  ```bash
  git add -A planning/
  git commit -m "docs: scaffold planning/changes/ and self-migrate this bundle

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Regroup archived pairs into changes/archive/ folders

**Files:** moves only, per the mapping table above. `planning/archived/*` →
`planning/changes/archive/<bundle>/{design,plan}.md`.

- [ ] **Step 1: Move all ten full pairs**

  ```bash
  cd planning
  A=archived; B=changes/archive
  for map in \
    "2026-06-03-all-extra-and-planning-dir:2026-06-03.01-all-extra-and-planning-dir" \
    "2026-06-03-faststream-0.7-migration:2026-06-03.02-faststream-0.7-migration" \
    "2026-06-04-faststream-0.7.1-testbroker-typing:2026-06-04.01-faststream-0.7.1-testbroker-typing" \
    "2026-06-04-foreign-broker-relay:2026-06-04.02-foreign-broker-relay" \
    "2026-06-09-mkdocs-github-pages:2026-06-09.01-mkdocs-github-pages" \
    "2026-06-09-drain-test-flaky-fetch-observation:2026-06-09.02-drain-test-flaky-fetch-observation" \
    "2026-06-10-docs-landing-and-comparison:2026-06-10.02-docs-landing-and-comparison" \
    "2026-06-11-operator-pages:2026-06-11.01-operator-pages" \
    "2026-06-11-docs-tutorials-and-observability-split:2026-06-11.02-docs-tutorials-and-observability-split" \
    "2026-06-12-docs-tutorials:2026-06-12.01-docs-tutorials"; do
      old="${map%%:*}"; new="${map##*:}"; mkdir -p "$B/$new"
      git mv "$A/${old}-design.md" "$B/$new/design.md"
      git mv "$A/${old}-plan.md"   "$B/$new/plan.md"
    done
  cd ..
  ```

- [ ] **Step 2: Move the design-only bundle (planning-conventions, no plan)**

  ```bash
  mkdir -p planning/changes/archive/2026-06-10.01-planning-conventions
  git mv planning/archived/2026-06-10-planning-conventions-design.md \
         planning/changes/archive/2026-06-10.01-planning-conventions/design.md
  ```

- [ ] **Step 3: Verify all eleven bundles exist**

  ```bash
  ls -d planning/changes/archive/*/ | wc -l    # expect 11
  ls planning/changes/archive/2026-06-10.01-planning-conventions/   # expect: design.md
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add -A planning/
  git commit -m "docs: regroup archived spec/plan pairs into changes/archive bundles

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Move findings to audits/, create retros/, drop emptied dirs

**Files:**
- Create: `planning/audits/2026-06-12-code-audit-findings.md` (moved)
- Create: `planning/audits/2026-06-12-docs-audit-findings.md` (moved)
- Create: `planning/retros/.gitkeep`
- Delete: `planning/active/`, `planning/archived/` (now empty)

- [ ] **Step 1: Move the two findings reports**

  ```bash
  mkdir -p planning/audits
  git mv planning/archived/2026-06-12-code-audit-findings.md planning/audits/
  git mv planning/archived/2026-06-12-docs-audit-findings.md planning/audits/
  ```

- [ ] **Step 2: Create retros/ placeholder**

  ```bash
  mkdir -p planning/retros
  touch planning/retros/.gitkeep
  ```

- [ ] **Step 3: Remove the now-empty old lifecycle dirs**

  `planning/active/` still holds `.gitkeep`; `planning/archived/` is empty.

  ```bash
  git rm -f planning/active/.gitkeep
  rmdir planning/active planning/archived 2>/dev/null || true
  ```

- [ ] **Step 4: Verify old dirs are gone and audits/ populated**

  ```bash
  ls planning/audits/                          # expect both findings files
  test ! -d planning/active && echo "active gone"
  test ! -d planning/archived && echo "archived gone"
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add -A planning/
  git commit -m "docs: move audit findings to planning/audits, add retros, drop old active/archived

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: Rename deferred-work.md → deferred.md

**Files:**
- Rename: `planning/deferred-work.md` → `planning/deferred.md`

- [ ] **Step 1: Rename the file**

  ```bash
  git mv planning/deferred-work.md planning/deferred.md
  ```

- [ ] **Step 2: Fix the in-file relative link to active/**

  In `planning/deferred.md`, the intro links graduated items to
  `[`active/`](active/)`. Update it to `[`changes/active/`](changes/active/)`.

- [ ] **Step 3: Verify no other in-repo reference to the old name remains**

  ```bash
  grep -rn "deferred-work.md" . --exclude-dir=.git || echo "no stale refs"
  ```
  Expected: only the README reference (fixed in Task 6) may remain; note it and
  move on. No other hits.

- [ ] **Step 4: Commit**

  ```bash
  git add -A planning/
  git commit -m "docs: rename deferred-work.md to deferred.md

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 5: Add the change.md template, update design/plan path refs

**Files:**
- Create: `planning/_templates/change.md`
- Modify: `planning/_templates/plan.md` (Spec link path)
- Modify: `planning/_templates/design.md` (no path refs to fix — verify)

- [ ] **Step 1: Create the lightweight-lane template**

  Write `planning/_templates/change.md`:

  ````markdown
  ---
  status: draft
  date: YYYY-MM-DD
  slug: my-change
  supersedes: null
  superseded_by: null
  pr: null
  outcome: null
  ---

  # Change: One-line capitalized title

  **Lane:** lightweight — ≲30 LOC net, ≤2 files, no new file, no public-API
  change, a single straightforward test. If it outgrows this, split into
  `design.md` + `plan.md`.

  ## Goal

  One or two sentences: what changes and why.

  ## Approach

  The shape of the change in brief — enough that a reviewer sees the design
  without a full spec. Link the truth home (`architecture/<capability>.md`) if a
  capability contract moves.

  ## Files

  - `path/to/file.py` — what changes
  - `tests/test_x.py` — test added / updated

  ## Verification

  - [ ] Failing test first — command + expected error.
  - [ ] Apply the change.
  - [ ] Test passes — command.
  - [ ] `just test` — full suite green.
  - [ ] `just lint` — clean.
  ````

- [ ] **Step 2: Update the plan template's Spec link**

  In `planning/_templates/plan.md`, change the **Spec:** line from
  `[`planning/active/YYYY-MM-DD-my-change-design.md`](./YYYY-MM-DD-my-change-design.md)`
  to `[`design.md`](./design.md)` (siblings now share a bundle folder).

- [ ] **Step 3: Confirm design.md template needs no path edit**

  ```bash
  grep -n "planning/active\|planning/archived" planning/_templates/design.md || echo "clean"
  ```
  Expected: `clean`.

- [ ] **Step 4: Commit**

  ```bash
  git add -A planning/_templates/
  git commit -m "docs: add change.md lightweight template, fix plan template spec link

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 6: Rewrite planning/README.md (portable Conventions + Index)

**Files:**
- Modify: `planning/README.md` (full rewrite)

- [ ] **Step 1: Replace the file with the two-section form**

  Overwrite `planning/README.md` with the content below. The **Archived**
  bullets are ported from the *current* README verbatim (same one-line
  summaries), with each link rewritten to its new bundle path per the Task 2
  mapping table (`archived/<date>-<slug>-design.md` →
  `changes/archive/<date>.NN-<slug>/design.md`).

  ````markdown
  # Planning

  Specs, plans, and change history for `faststream-outbox`. The living truth
  about *what the system does now* lives in [`architecture/`](../architecture/)
  at the repo root; this directory records *how it got there*.

  ## Conventions

  > This section is the portable convention — identical across the
  > modern-python repos. The Index below is repo-specific. To adopt elsewhere,
  > copy this section plus [`_templates/`](_templates/) and point that repo's
  > `CLAUDE.md` Workflow + truth home at it.

  ### Two axes, never mixed

  - **`architecture/` (repo root) — the present.** One file per capability,
    living prose, updated whenever a change ships. The truth home.
  - **`planning/changes/` — the past-and-pending.** One folder per change,
    frozen once shipped.

  Shipping a change **promotes** its conclusions into the affected
  `architecture/<capability>.md` by hand, then archives the bundle. That
  hand-edit is what keeps `architecture/` true; the archived bundle carries the
  *why*.

  ### Change bundles

  A change is a folder `changes/active/YYYY-MM-DD.NN-<slug>/`:

  - `YYYY-MM-DD` — proposal date; `.NN` — zero-padded intra-day counter
    (`.01`, `.02`, …) that breaks same-date ties so the timeline sorts stably.
  - `<slug>` — kebab-case description, not a story ID.

  On merge the folder moves to `changes/archive/` with `status: shipped`, `pr:`,
  and `outcome:` filled, and its line moves from **Active** to **Archived** in
  the Index below.

  ### Three lanes

  | Lane | Artifacts | Use when |
  |------|-----------|----------|
  | **Full** | `design.md` + `plan.md` | design judgment; new file/module; public-API change; cross-cutting/multi-file; non-trivial test design |
  | **Lightweight** | `change.md` | small-but-real: ≲30 LOC net, ≤2 files, no new file, no public-API change, single straightforward test |
  | **Tiny** | none — conventional commit | typo, dep bump, linter/formatter/CI tweak, mechanical rename, single-line config |

  Heavier lane wins on ambiguity. A `change.md` that outgrows its lane splits
  into `design.md` + `plan.md`.

  ### Artifacts at a glance

  - **`design.md`** — the spec: the *thinking* (why, design, trade-offs, scope).
  - **`plan.md`** — the plan: the *sequencing* (the executor's task checklist).
  - **`change.md`** — both, condensed, for the lightweight lane.
  - **`releases/<semver>.md`** — per-release user-facing notes.
  - **`audits/<date>-<slug>.md`** — findings from a code/docs/bug-hunt sweep;
    spawns fix changes.
  - **`retros/<date>-<slug>.md`** — what we learned after a body of work.
  - **`deferred.md`** — real-but-unscheduled items, each with a revisit trigger.

  Templates live in [`_templates/`](_templates/).

  ### Frontmatter

  `design.md` / `change.md`: `status` (draft|approved|shipped|superseded),
  `date`, `slug`, `supersedes`, `superseded_by`, `pr`, `outcome`.
  `plan.md`: `status`, `date`, `slug`, `spec`, `pr`. Files in `architecture/`
  carry **no** frontmatter — living prose, dated by git.

  ## Index

  ### Active

  _None._

  ### Archived (shipped)

  - **[docs-tutorials](changes/archive/2026-06-12.01-docs-tutorials/design.md)**
    (#58, 2026-06-12) — The two tutorials deferred from #56: *Your first outbox
    app* and *Add a Kafka relay*. Kill-Kafka step folded into an at-least-once
    callout after `aiokafka` absorbed the outage on both attempts.
  - **[docs-tutorials-and-observability-split](changes/archive/2026-06-11.02-docs-tutorials-and-observability-split/design.md)**
    (#56, 2026-06-12) — Three-way split of `usage/observability.md` into
    Reference + How-to + Explanation; tutorials deferred to #58.
  - **[operator-pages](changes/archive/2026-06-11.01-operator-pages/design.md)**
    (#53, 2026-06-11) — `docs/operations/`: Production checklist, Troubleshooting
    playbook, Alembic migrations. The B follow-on from #50.
  - **[docs-landing-and-comparison](changes/archive/2026-06-10.02-docs-landing-and-comparison/design.md)**
    (#50, 2026-06-10) — Docs landing rewrite, four-section nav reshape, new
    Comparison page.
  - **[planning-conventions](changes/archive/2026-06-10.01-planning-conventions/design.md)**
    (#49, 2026-06-10) — Spec/plan boundary, `active/`/`archived/`/`_templates/`
    layout, frontmatter, migration of the existing pairs. *Superseded by
    [portable-planning-convention](changes/archive/2026-06-13.01-portable-planning-convention/design.md).*
  - **[drain-test-flaky-fetch-observation](changes/archive/2026-06-09.02-drain-test-flaky-fetch-observation/design.md)**
    (#48, 2026-06-10) — Drain test waits via the `fetched` recorder instead of an
    SQL poll, killing a 3.14 coverage flake.
  - **[mkdocs-github-pages](changes/archive/2026-06-09.01-mkdocs-github-pages/design.md)**
    (#45, 2026-06-09) — Docs hosting moves from Read the Docs to GitHub Pages on
    `faststream-outbox.modern-python.org`.
  - **[foreign-broker-relay](changes/archive/2026-06-04.02-foreign-broker-relay/design.md)**
    (#44, 2026-06-05) — `OutboxSubscriber` officially supports the
    FastStream-native decorator relay to Kafka/Rabbit/NATS/Redis with three
    guardrails.
  - **[faststream-0.7.1-testbroker-typing](changes/archive/2026-06-04.01-faststream-0.7.1-testbroker-typing/design.md)**
    (#43, 2026-06-04) — Adopt FastStream 0.7.1's `TestBroker[Broker, EnterType]`
    typing fix; drop two `# ty: ignore` directives.
  - **[faststream-0.7-migration](changes/archive/2026-06-03.02-faststream-0.7-migration/design.md)**
    (#42, 2026-06-03) — Migrate to `faststream>=0.7,<0.8`; fix mechanical break
    points; drop per-call `middlewares=` kwarg.
  - **[all-extra-and-planning-dir](changes/archive/2026-06-03.01-all-extra-and-planning-dir/design.md)**
    (#41, 2026-06-03) — Add `faststream-outbox[all]` aggregate extra; bootstrap
    the `planning/` directory itself.

  ## Other

  - **[`architecture/`](../architecture/)** at the repo root — the living
    capability truth (relay, timers, dlq, drain, metrics, test broker). This is
    the promotion target on every ship.
  - **[audits/](audits/)** — findings reports (2026-06-12 code + docs audits).
  - **[lint-suppressions.md](lint-suppressions.md)** — repo-specific extra (not
    part of the portable core): audit of `noqa` / `ty: ignore` directives and
    why each one stays.
  - **[deferred.md](deferred.md)** — the long-tail register of real-but-
    unscheduled items with revisit triggers.
  ````

- [ ] **Step 2: Verify every Index link resolves**

  ```bash
  cd planning
  grep -oE 'changes/archive/[^)]+/design.md' README.md | while read p; do
    test -f "$p" || echo "BROKEN: $p"
  done; echo "link check done"
  cd ..
  ```
  Expected: `link check done` with no `BROKEN:` lines. (The
  `2026-06-13.01-portable-planning-convention` link points into `changes/active/`
  today; it resolves after Task 10 archives it — note it and move on.)

- [ ] **Step 3: Commit**

  ```bash
  git add planning/README.md
  git commit -m "docs: rewrite planning/README.md as portable conventions + index

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 7: Mark planning-conventions superseded

**Files:**
- Modify: `planning/changes/archive/2026-06-10.01-planning-conventions/design.md`

- [ ] **Step 1: Set superseded_by in frontmatter**

  In that `design.md`, change `superseded_by: null` to
  `superseded_by: portable-planning-convention` and `status: shipped` to
  `status: superseded`.

- [ ] **Step 2: Add a callout at the top of the body**

  Immediately after the `# Design: …` H1, insert:

  ```markdown
  > **Superseded by [portable-planning-convention](../2026-06-13.01-portable-planning-convention/design.md).**
  > The `active/` + `archived/` lifecycle layout this spec introduced is replaced
  > by the `changes/` (active → archive) + `architecture/`-promotion model. The
  > migration this spec shipped still happened; only the layout was later
  > reworked.
  ```

- [ ] **Step 3: Verify**

  ```bash
  grep -n "superseded" planning/changes/archive/2026-06-10.01-planning-conventions/design.md
  ```
  Expected: the frontmatter `status:`/`superseded_by:` lines plus the callout.

- [ ] **Step 4: Commit**

  ```bash
  git add planning/changes/archive/2026-06-10.01-planning-conventions/design.md
  git commit -m "docs: mark planning-conventions superseded by portable-planning-convention

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 8: Update CLAUDE.md Workflow section

**Files:**
- Modify: `CLAUDE.md` (`## Workflow` section body only; `## Architecture`
  untouched)

- [ ] **Step 1: Replace the four Workflow paragraphs**

  Replace the entire body between the `## Workflow` heading and the
  `## Architecture` heading with:

  ````markdown
  Per-feature: brainstorming → spec in `planning/changes/active/YYYY-MM-DD.NN-<slug>/design.md` → writing-plans → plan in `planning/changes/active/YYYY-MM-DD.NN-<slug>/plan.md` → executing-plans / subagent-driven-development → requesting-code-review → finishing-a-development-branch. Each change is a folder bundle; `<slug>` is a kebab-case description, not a story ID; `.NN` is a zero-padded intra-day counter that breaks same-date ties so the timeline sorts stably. On merge, the bundle moves to `planning/changes/archive/` with `status: shipped`, `pr:`, and `outcome:` filled, **and the change promotes its conclusions into the affected `architecture/<capability>.md`** — that hand-edit is what keeps `architecture/` true. See [`planning/README.md`](planning/README.md) for the conventions + index and [`planning/_templates/`](planning/_templates/) for copy-and-fill starting points.

  **Spec** (`design.md`) captures the *thinking* — why we are doing this, what the design is, what trade-offs were considered, what is out of scope. Written before code; rarely revised after merge. **Plan** (`plan.md`) captures the *sequencing* — the ordered checklist of tasks an executor (human or agent) walks. References the spec for the "why"; never re-explains it. **`architecture/`** captures the *invariants* of shipped systems — the living truth, promoted from a change on merge. A plan paragraph that would still read correctly with all task numbers and checkboxes removed is design content and belongs in the spec.

  **Three lanes.** Scale the artifact to the change. **Full** — a `design.md` + `plan.md` bundle — for real design judgment, a new file/module, a public-API change, cross-cutting/multi-file work, or non-trivial test design. **Lightweight** — a single `change.md` — for small-but-real changes (≲30 LOC net, ≤2 files, no new file, no public-API change, a single straightforward test). **Tiny** — no bundle, just a conventional commit — for a typo fix, dep bump, linter/formatter/CI tweak, a mechanical rename to satisfy a just-landed convention, or a single-line config change. Heavier lane wins on ambiguity; a `change.md` that outgrows its lane splits into `design.md` + `plan.md`.
  ````

- [ ] **Step 2: Verify no stale planning path strings remain in CLAUDE.md**

  ```bash
  grep -n "planning/active\|planning/archived" CLAUDE.md || echo "clean"
  ```
  Expected: `clean`.

- [ ] **Step 3: Commit**

  ```bash
  git add CLAUDE.md
  git commit -m "docs: update CLAUDE.md workflow for changes/ bundles + three lanes + promotion

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 9: Full verification pass

**Files:** none (read-only checks).

- [ ] **Step 1: No stale references anywhere**

  ```bash
  grep -rn "planning/active\|planning/archived\|deferred-work.md" . \
    --exclude-dir=.git --exclude-dir=site --exclude-dir=.venv \
    | grep -v "planning/changes/active/2026-06-13.01-portable-planning-convention/" \
    || echo "no stale refs"
  ```
  Expected: `no stale refs`. (The exclusion covers this change's own bundle,
  whose plan.md documents the old paths in the mapping table — that's history,
  not a live link.)

- [ ] **Step 2: Docs build is strict-clean**

  ```bash
  just docs-build
  ```
  Expected: `mkdocs build --strict` succeeds — confirms no `docs/` link broke
  (none should; `docs/` and `architecture/` were untouched).

- [ ] **Step 3: Lint passes**

  ```bash
  just lint-ci
  ```
  Expected: clean (eof-fixer, ruff format check, markdown). No code touched.

- [ ] **Step 4: Tree matches the spec's §2**

  ```bash
  find planning architecture -maxdepth 2 -type d | sort
  ```
  Expected (no `planning/active`, no `planning/archived`):
  ```
  architecture
  planning
  planning/_templates
  planning/audits
  planning/changes
  planning/changes/active
  planning/changes/archive
  planning/releases
  planning/retros
  ```

- [ ] **Step 5: Commit any lint fixups**

  ```bash
  git add -A
  git diff --cached --quiet && echo "nothing to commit" || \
  git commit -m "docs: lint fixups for planning convention migration

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 10: On-merge archival of this bundle (do at merge time)

**Files:**
- Move: `planning/changes/active/2026-06-13.01-portable-planning-convention/`
  → `planning/changes/archive/2026-06-13.01-portable-planning-convention/`

This is the standard "on merge" step the new convention prescribes, applied to
this change itself. Do it once the PR number is known (open the PR first), as
the final commit on the branch.

- [ ] **Step 1: Archive the bundle**

  ```bash
  mkdir -p planning/changes/archive/2026-06-13.01-portable-planning-convention
  git mv planning/changes/active/2026-06-13.01-portable-planning-convention/design.md \
         planning/changes/archive/2026-06-13.01-portable-planning-convention/design.md
  git mv planning/changes/active/2026-06-13.01-portable-planning-convention/plan.md \
         planning/changes/archive/2026-06-13.01-portable-planning-convention/plan.md
  ```

- [ ] **Step 2: Fill shipped frontmatter**

  In both files set `status: shipped`; in `design.md` set `pr: "<N>"` and
  `outcome: "merged 2026-06-13 as #<N>"`; in `plan.md` set `pr: "<N>"`. Replace
  `<N>` with the actual PR number.

- [ ] **Step 3: Move the README Index line from Active to Archived**

  In `planning/README.md`, add a bullet under **Archived (shipped)** (newest,
  at the top):

  ```markdown
  - **[portable-planning-convention](changes/archive/2026-06-13.01-portable-planning-convention/design.md)**
    (#<N>, 2026-06-13) — Two-axis OpenSpec-shaped planning convention:
    `architecture/` truth + `changes/` folder bundles, `.NN` tiebreak, three
    lanes, `audits/`+`retros/`. Supersedes planning-conventions.
  ```

  The **Active** section returns to `_None._`.

- [ ] **Step 4: Verify active/ is empty and re-run the link check**

  ```bash
  ls planning/changes/active/   # expect only .gitkeep
  cd planning && grep -oE 'changes/(archive|active)/[^)]+/design.md' README.md \
    | while read p; do test -f "$p" || echo "BROKEN: $p"; done; echo "ok"; cd ..
  ```
  Expected: `ok`, no `BROKEN:` lines (the portable-planning-convention link now
  resolves under `changes/archive/`).

- [ ] **Step 5: Commit**

  ```bash
  git add -A planning/
  git commit -m "docs: archive portable-planning-convention bundle (#<N>)

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

## Self-review notes

- **Spec coverage:** §2 layout → Tasks 1–3; §3 `.NN` ids → Task 2 mapping; §4
  lanes → Tasks 5, 6, 8; §5 promotion → documented in README (Task 6) + CLAUDE.md
  (Task 8); §6 frontmatter → templates (Task 5) + README (Task 6); §7
  audits/retros → Task 3; §8 releases/deferred/templates → Tasks 4, 5; §9 README
  → Task 6; §10 CLAUDE.md → Task 8; §11 migration → Tasks 1–4, 7, 10.
  `releases/` is intentionally untouched (no task needed).
- **Promotion has no library-capability target here** (the convention lives in
  README, not `architecture/`), so this change's own ship skips spec-promotion —
  consistent with spec §11 step 9.
- **Same-date collisions** in the existing archive (`-03`, `-04`, `-09`, `-10`,
  `-11`) are resolved by the explicit mapping table, not left to the executor.
