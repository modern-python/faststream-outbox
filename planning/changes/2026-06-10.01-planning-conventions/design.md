---
status: superseded
date: 2026-06-10
slug: planning-conventions
summary: Spec/plan boundary, active/archived/_templates layout, frontmatter, migration of the existing pairs. Superseded by portable-planning-convention.
supersedes: null
superseded_by: portable-planning-convention
pr: "49"
outcome: merged 2026-06-10 as #49
---

# Design: Rework planning conventions + migrate existing artifacts

> **Superseded by [portable-planning-convention](../2026-06-13.01-portable-planning-convention/design.md).**
> The `active/` + `archived/` lifecycle layout this spec introduced is replaced
> by the `changes/` (active → archive) + `architecture/`-promotion model. The
> migration this spec shipped still happened; only the layout was later
> reworked.

## Summary

Codify the spec / plan boundary, add `active/` and `archived/` subdirectories
under `planning/`, ship copy-and-fill templates, give every spec and plan
YAML frontmatter, and migrate the six already-shipped artifact pairs to the
new layout — all in one PR.

The goal is **higher orientation per glance** (which work is in flight, which
shipped, what each was about, what shipped under it) and a **clear contract
for what goes in a spec vs. what goes in a plan** so plans stop absorbing
design content and growing without bound.

No runtime code, no test code, no public API touched.

## Motivation

Concrete observations from the current `planning/` tree:

- **The spec / plan boundary is undefined.** `CLAUDE.md` names both files
  and the workflow that produces them but does not say what content each
  owns. Result: plans absorb design rationale that belongs in the spec.
  Example from `2026-06-04-foreign-broker-relay-plan.md` (Task 1, Step 2):
  > "We add it to the `dev` dependency group (not
  > `[project.optional-dependencies]`, which is for runtime extras) so
  > that user installs of `faststream-outbox` do not pick up Kafka
  > transport unless they explicitly depend on it."

  That is a design trade-off, not an executor instruction. It belongs in
  the spec. Six paired artifacts all leak similar content in similar
  ways; the largest plan is 62 KB.

- **Active vs. shipped is invisible from `ls`.** Every spec and plan piles
  into a single flat directory each, sorted by date. Six pairs today; 50
  pairs in a year. Finding "what's in flight" requires opening files to
  read the `Status:` line in the body. The reader pays the cost on every
  visit; the writer (whoever ships the change) almost never updates the
  status line — every existing spec still says `Status: Draft` or
  `Status: Approved` even though the work merged weeks ago.

- **No index.** There is no `planning/README.md` summarizing what each
  artifact is about. The slug carries some signal
  (`mkdocs-github-pages`, `foreign-broker-relay`) but slug alone is
  thin — a one-line summary per artifact pays back on every visit and
  costs the author one line.

- **Style is mimetic.** New specs are written by reading the most recent
  spec and matching shape. This has worked (the six existing specs are
  consistent), but it is fragile — one contributor in a hurry copies a
  pre-2026-06 file that did not yet have the convention's current shape
  and the drift begins. A template file makes the convention explicit and
  copy-pasteable.

- **No PR / merge linkage.** When a spec ships, there is no record of
  *which* PR shipped it. Tracing a decision back from code requires
  guessing the slug, opening the spec, then reading git history for
  files mentioned in the spec to find the merge. A one-line
  `outcome:` frontmatter entry (`PR #47, merged 2026-06-09`) collapses
  that to a single field on the artifact.

- **No "this overturns that".** The CDC/WAL rejection memo lives only in
  `/Users/kevinsmith/.claude/projects/.../memory/cdc_wal_rejected.md`,
  not in `planning/`. If a future spec changes a decision recorded in
  an earlier spec, there is nothing in the file system that flags the
  relationship — readers of the older spec take its conclusion at face
  value. A `supersedes:` / `superseded_by:` link makes the relationship
  visible.

The fix for all of this is small, single PR, low risk: a directory split,
a template, a frontmatter convention, an index, and a migration of the
six existing pairs.

## Non-goals

Deliberately *not* covered here; each is a candidate follow-on:

- **`just plans` index generator.** The frontmatter conventions this spec
  lands make a generator a 30-line script, but the index is small enough
  (six artifacts grown to maybe twenty over the next year) that a hand-
  maintained `planning/README.md` is sufficient. Automate later if the
  list grows.

- **Trimming existing plans down to fit the new spec / plan boundary.**
  Existing plans stay verbatim in `archived/`. Promoting design content
  from a shipped plan back into its paired spec is an opportunistic
  follow-on — touching frozen history without a forcing function is
  rarely worth it. The convention shapes *future* plans; existing ones
  are historical artifacts.

- **Relocating `planning/architecture/`.** It does not belong in
  `planning/` (it documents shipped invariants, not pending work), but
  moving it changes URLs and inbound links from `CLAUDE.md`. Separate
  spec.

- **Workflow tooling.** No new CI checks (e.g. "frontmatter must parse").
  Editor support for frontmatter is universal; the cost of a malformed
  block surfaces immediately on PR review.

- **A "tiny-change" lane that bypasses the spec → plan flow.** Worth
  defining (typo fix, dep bump, etc.) but orthogonal to the conventions
  this spec lands. Separate change to `CLAUDE.md`.

## Design

### 1. The spec / plan boundary, codified

Three sentences added to `CLAUDE.md`'s Workflow section, alongside the
existing per-feature pipeline:

> **Spec** captures the *thinking* — why we are doing this, what the
> design is, what trade-offs were considered, what is out of scope.
> Written before code; rarely revised after merge.
>
> **Plan** captures the *sequencing* — the ordered checklist of tasks
> an executor (human or agent) walks. References the spec for the
> "why"; never re-explains it. Often a markdown checkbox list with a
> few prose notes between groups.
>
> **`planning/architecture/`** captures the *invariants* of shipped
> systems — the load-bearing properties future contributors must
> preserve. Written after merge by promoting the relevant parts of a
> spec.

A plan that ends up explaining *why* a design choice was made (versus
*what step to take next*) should move that explanation back into the
spec. The smell to watch for: any plan paragraph that would still read
correctly if you removed all the task numbers and checkboxes — that is
design content.

### 2. Directory layout

```
planning/
├── README.md                                  # NEW — index, hand-maintained
├── _templates/
│   ├── design.md                              # NEW — copy-and-fill spec
│   └── plan.md                                # NEW — copy-and-fill plan
├── active/                                    # NEW — in-flight pairs
│   ├── 2026-06-10-docs-landing-and-comparison-design.md
│   ├── 2026-06-10-planning-conventions-design.md    # ← this spec
│   └── (plans land here as they get written)
├── archived/                                  # NEW — shipped pairs, frozen
│   ├── 2026-06-03-all-extra-and-planning-dir-design.md
│   ├── 2026-06-03-all-extra-and-planning-dir-plan.md
│   ├── 2026-06-03-faststream-0.7-migration-design.md
│   ├── 2026-06-03-faststream-0.7-migration-plan.md
│   ├── 2026-06-04-faststream-0.7.1-testbroker-typing-design.md
│   ├── 2026-06-04-faststream-0.7.1-testbroker-typing-plan.md
│   ├── 2026-06-04-foreign-broker-relay-design.md
│   ├── 2026-06-04-foreign-broker-relay-plan.md
│   ├── 2026-06-09-drain-test-flaky-fetch-observation-design.md
│   ├── 2026-06-09-drain-test-flaky-fetch-observation-plan.md
│   ├── 2026-06-09-mkdocs-github-pages-design.md
│   └── 2026-06-09-mkdocs-github-pages-plan.md
├── architecture/                              # unchanged (separate spec)
└── lint-suppressions.md                       # unchanged
```

The old `specs/` and `plans/` directories disappear. Pairing is now
implicit in the shared `YYYY-MM-DD-<slug>-` prefix and explicit in
frontmatter. Both halves of a pair always live in the same directory
(`active/` or `archived/`); a pair cannot be half-shipped because the
plan is the executor's record of execution.

### 3. Lifecycle

```
                    ┌─────────────────────────┐
   spec drafted ───▶│  planning/active/       │
                    │  status: draft          │
                    └────────────┬────────────┘
                                 │
                         spec approved
                                 ▼
                    ┌─────────────────────────┐
                    │  planning/active/       │
                    │  status: approved       │
                    │  plan written           │
                    └────────────┬────────────┘
                                 │
                          PR merged
                                 ▼
                    ┌─────────────────────────┐
                    │  planning/archived/     │
                    │  status: shipped        │
                    │  pr: 51                 │
                    │  outcome: "merged …"    │
                    └─────────────────────────┘
```

The move from `active/` to `archived/` happens in the same PR that
merges the implementation (or an immediate follow-up). Until merge,
the artifact is `active/`; the moment the code lands, the spec + plan
move to `archived/` with `status: shipped`, `pr:`, and `outcome:`
filled.

`planning/README.md` is updated in the same PR — one line moves from
the "Active" section to the "Archived" section. Hand-maintained;
small cost per merge; payoff on every visit thereafter.

### 4. Frontmatter

YAML frontmatter on every spec and every plan:

**Spec:**
```yaml
---
status: draft | approved | shipped | superseded
date: 2026-06-10
slug: planning-conventions
supersedes: null                      # or <slug> of older spec
superseded_by: null                   # or <slug> of newer spec
pr: null                              # or "47", set when merged
outcome: null                         # or "merged 2026-06-09 as #47"
---
```

**Plan:**
```yaml
---
status: draft | approved | shipped
date: 2026-06-10
slug: planning-conventions
spec: planning-conventions            # paired spec slug, sanity check
pr: null
---
```

Notes:

- **YAML, not body header.** The current `**Status:** Draft / **Date:**
  ... / **Slug:** ...` body header is human-readable but not machine-
  readable without a custom parser. Frontmatter is parseable by every
  static-site generator, every editor, and a one-liner shell script if
  we ever automate the index. Migration cost is small (six pairs, twelve
  files, ~10 lines each).
- **`status: superseded`** is a separate top-level state, not a sub-
  state of `shipped`. A spec that was approved, partly implemented, then
  overturned by a later spec is `superseded` even if no PR shipped it.
- **`pr:` is a string**, not a number — supports cross-repo references
  later if ever needed, and keeps YAML simple.
- **`outcome:` is freeform** — typically `"merged 2026-06-09 as #47"` but
  can also be `"abandoned 2026-06-12, see planning/archived/2026-06-12-X"`.
- The body header (`**Status:** ...` etc.) **is removed** from existing
  specs as part of migration. The frontmatter is the single source of
  truth. The H1 title remains.

### 5. `planning/README.md` — hand-maintained index

One-line summary per artifact, grouped by lifecycle:

```markdown
# Planning

Specs and plans for `faststream-outbox` changes. See [CLAUDE.md](../CLAUDE.md#workflow)
for the per-feature workflow.

## Active

- **[docs-landing-and-comparison](active/2026-06-10-docs-landing-and-comparison-design.md)**
  — Rewrite docs landing, reshape nav into Concepts/Guides/Reference, add a
  Comparison page.
- **[planning-conventions](active/2026-06-10-planning-conventions-design.md)**
  — This spec. Codify spec/plan boundary, add active/archived/templates,
  migrate existing pairs.

## Archived (shipped)

- **[mkdocs-github-pages](archived/2026-06-09-mkdocs-github-pages-design.md)**
  (#?, 2026-06-09) — Move docs hosting from Read the Docs to GitHub Pages.
- **[drain-test-flaky-fetch-observation](archived/2026-06-09-drain-test-flaky-fetch-observation-design.md)**
  (#48, 2026-06-?) — Drain test waits via fetched recorder, not SQL poll.
- **[foreign-broker-relay](archived/2026-06-04-foreign-broker-relay-design.md)**
  (#?, 2026-06-?) — Decorator-relay pattern with `OutboxSubscriber` as
  source, three guardrails, docs push.
- **[faststream-0.7.1-testbroker-typing](archived/2026-06-04-faststream-0.7.1-testbroker-typing-design.md)**
  (#?, 2026-06-?) — Type fixes for TestBroker against FastStream 0.7.1.
- **[faststream-0.7-migration](archived/2026-06-03-faststream-0.7-migration-design.md)**
  (#?, 2026-06-?) — Migration to FastStream 0.7.
- **[all-extra-and-planning-dir](archived/2026-06-03-all-extra-and-planning-dir-design.md)**
  (#?, 2026-06-?) — Introduce the `[all]` extras bundle and the
  `planning/` directory itself.
```

The migration step (§8) fills in the actual PR numbers and merge dates by
reading `git log --grep="<slug>"` and the merge commits.

### 6. Templates

`planning/_templates/design.md`:

```markdown
---
status: draft
date: YYYY-MM-DD
slug: my-change
supersedes: null
superseded_by: null
pr: null
outcome: null
---

# Design: One-line capitalized title

## Summary

One paragraph. What changes, at the level a reader needs to decide if this
spec is worth reading in full.

## Motivation

Why now. What is broken or missing. Concrete observations / numbers, not
abstract complaints. Link to memory entries or earlier specs when relevant.

## Non-goals

What is deliberately out of scope and (when nontrivial) why. Each item is
a sentence; one line each.

## Design

### 1. <First piece>

What changes, in enough detail that a reader who has not seen the codebase
can follow. Code samples / diagrams welcome.

### 2. <Second piece>

...

## Operations

Out-of-repo steps (DNS, infra, external account changes). Omit if none.

## Out of scope

Already covered above under Non-goals if appropriate. Repeat-list of
explicitly-excluded follow-ups belongs here when the list is long.

## Testing

How we know it landed correctly. New pytest? Smoke check on live URL?
Lint pass? Be specific.

## Risk

What could go wrong, ranked by likelihood × impact. Mitigations.
```

`planning/_templates/plan.md`:

```markdown
---
status: draft
date: YYYY-MM-DD
slug: my-change
spec: my-change
pr: null
---

# <slug> — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One sentence — what shipping this plan achieves. No design
rationale; link to the spec for that.

**Spec:** [`planning/active/YYYY-MM-DD-my-change-design.md`](./YYYY-MM-DD-my-change-design.md)

**Branch:** `feat/my-change` (or `fix/`, `chore/`, etc.)

**Commit strategy:** Per-task commits / single commit / squash on merge.
Whichever fits.

---

### Task 1: <imperative description>

**Files:**
- Modify: `path/to/file.py`
- Create: `path/to/new.py`

One sentence on what this task accomplishes. No deeper reasoning — that's
in the spec.

- [ ] **Step 1: <action>**

  Run / edit / verify command. Expected output.

- [ ] **Step 2: <action>**

  ...

- [ ] **Step 3: Commit**

  ```bash
  git add path/to/file.py
  git commit -m "<type>: <subject>

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: ...
```

Both templates are copy-and-rename, not generated. A `just new-spec` /
`just new-plan` target could automate the rename + date substitution, but
is **out of scope** here.

### 7. Supersedes / superseded_by

When a new spec overturns a decision in an earlier spec:

1. The new spec sets `supersedes: <old-slug>` in frontmatter and adds a
   one-line `## Supersedes` section near the top explaining what changes
   and why.
2. The old spec (in `archived/`) gets `superseded_by: <new-slug>` set and
   a `## Superseded by` callout at the top.
3. `status:` on the old spec flips to `superseded`. It stays in
   `archived/` — the implementation that shipped still landed; the
   *design conclusion* was later replaced.

Cheap, manual, and visible. No automation.

### 8. Migration of existing artifacts

One-time, performed in the same PR that ships this spec. Per pair (six
pairs total):

1. **Move both files** from `planning/specs/` and `planning/plans/` to
   `planning/archived/`. The shared date-slug prefix in the filenames
   preserves pairing.
2. **Replace the body header** (`**Status:** ...` / `**Date:** ...` /
   `**Slug:** ...`) with YAML frontmatter at the top of the file.
3. **Set `status: shipped`** (or `superseded` where applicable — none in
   the current six).
4. **Fill `pr:` and `outcome:`** by reading `git log --merges --grep="<slug>"`
   and inspecting the merge commits visible in the git history. Recent
   commit titles from the repo log already name the PRs (e.g.
   `Merge pull request #48 from modern-python/fix/drain-test-flake-recorder`
   for `drain-test-flaky-fetch-observation`).
5. **Sanity-check** that the spec/plan pair share a slug — if not, fix
   the slug field rather than the filename.

The migration touches twelve files, ~10 lines of frontmatter each, plus
the deletion of three body-header lines per file. No prose content is
rewritten. `git mv` preserves history for blame.

Old empty directories `planning/specs/` and `planning/plans/` are removed
in the same commit (the `.gitkeep` files go too). The post-state matches
the tree in §2 exactly.

### 9. CLAUDE.md update

Two edits in the `## Workflow` section:

1. **Update the per-feature pipeline path string** to reflect the new
   `active/` directory:
   > Per-feature: brainstorming → spec in
   > `planning/active/YYYY-MM-DD-<slug>-design.md` → writing-plans →
   > plan in `planning/active/YYYY-MM-DD-<slug>-plan.md` → executing-
   > plans / subagent-driven-development → requesting-code-review →
   > finishing-a-development-branch.

2. **Append the spec/plan/architecture boundary** as a sub-paragraph
   immediately after, lifted from §1 above.

Nothing else in `CLAUDE.md` changes.

## Out of scope (repeat list)

Already named under Non-goals; repeated here for grep:

- `just plans` index generator
- Trimming existing plans (promote design content back into specs)
- Relocating `planning/architecture/` out of `planning/`
- Frontmatter parsing CI checks
- Tiny-change lane for typo / dep-bump scope

## Testing

Configuration + content + file moves; correctness is checked by:

- `just lint-ci` passes (markdown EOF + ruff on YAML formatting in the
  Justfile if it changes — it does not in this spec).
- Every artifact under `planning/active/` and `planning/archived/` has
  parseable YAML frontmatter (spot-checked manually on review; the cost
  of a malformed block surfaces on read).
- `planning/README.md` links resolve — manual click-through on PR
  preview.
- The post-migration tree exactly matches §2.

No new pytest hooks. No new CI jobs.

## Risk

- **`git log` archaeology produces wrong PR numbers for older specs.**
  Some of the six existing pairs may have shipped under squashed PRs
  whose subjects do not mention the slug. Mitigation: the migration
  step falls back to leaving `pr: null` and `outcome: "shipped, PR
  unknown"` on any artifact where the merge commit cannot be identified
  with confidence. Future merges fill the fields correctly because the
  convention is in place.

- **`git mv` history blame breakage on tooling that ignores `--follow`.**
  GitHub Web UI follows renames in blame; some editor blame integrations
  do not. Low practical impact — readers blaming a planning artifact
  are usually looking for "who wrote this spec" not "what was the line
  five revisions ago". `git log --follow planning/archived/<file>` works
  regardless.

- **Frontmatter conflicts with mkdocs builds** if `planning/` ever gets
  served by mkdocs. It does not today (`docs_dir: docs`), and there is
  no plan to. If a future spec exposes `planning/` to mkdocs, the
  frontmatter format we land here is already mkdocs-compatible.

- **Convention drift on the next contributor.** Mitigated by the
  templates — copy the file, fill the blanks. Material risk is
  someone editing an existing template-shaped spec without the
  frontmatter (because they grepped an older shipped one). Acceptable
  loss; PR review catches it.

- **The hand-maintained `README.md` index falls out of date.** A
  contributor lands a spec and forgets to update the index. Acceptable:
  PR review catches it the same way it catches a missing test. The
  `just plans` automation in §"Non-goals" is the answer if drift
  becomes chronic.
