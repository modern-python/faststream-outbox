---
status: draft
date: 2026-06-13
slug: portable-planning-convention
supersedes: planning-conventions
superseded_by: null
pr: null
outcome: null
---

# Design: Portable OpenSpec-shaped planning convention

## Summary

Replace the current lifecycle-split `planning/` layout (`active/` + `archived/`
holding flat date-prefixed `*-design.md` / `*-plan.md` pairs) with a coherent
**two-axis model**: `architecture/` (kept at the repo root) holds the *living
truth* — what the system does now, one file per capability — and
`planning/changes/` holds the *change history*, each change a self-contained
folder bundle, `active/` → `archive/`. The boundary that is implicit today is
made explicit: shipping a change **promotes** its conclusions into the relevant
`architecture/<capability>.md` by hand — no formal spec-delta syntax. Three
ceremony lanes (full / lightweight / tiny) scale the artifact to the change.
Audits and retros get dedicated dirs; `releases/`, `deferred.md`, and
`_templates/` carry over.

The layout is **OpenSpec-shaped** (its spatial model: a living-truth space + a
`changes/` space with an `archive/`) but keeps the **superpowers artifact
vocabulary** the repo already uses (`design.md` = spec, `plan.md` = plan), and
keeps the living-truth space where this repo already has it — `architecture/`
at the root — rather than relocating it under `planning/`. It is designed to be
**portable**: the same convention prose + templates drop into `httpware`,
`modern-di`, and `lite-bootstrap`, each pointing promotion at its own truth home
(`architecture/` here, `engineering.md` in `httpware`, etc.), with
`faststream-outbox` as the first adopter. This spec covers the convention and
this repo's migration only; rolling it out to the other three repos is separate
follow-on work.

This supersedes [`planning-conventions`](../archived/2026-06-10-planning-conventions-design.md)
(the `active/`+`archived/` lifecycle model it introduced is replaced by the
`changes/` model here). No runtime code, no test code, no public API touched.

## Motivation

A maintainer survey of the four sibling repos (`faststream-outbox`, `httpware`,
`modern-di`, `lite-bootstrap`) and a comparison against the OpenSpec
spec-driven-development convention surfaced four distinct pains, all of which
this change targets:

- **Truth vs. history is muddled.** This repo half-implements the OpenSpec
  split without naming it: `architecture/` (at the repo root) is the
  capability-organized *living truth* (relay, timers, dlq, drain, metrics,
  test-broker) and `planning/` is the feature/time-organized *change history*.
  But the boundary is implicit — nothing in the convention says "a shipped
  change updates `architecture/`," so readers cannot tell from the layout that
  these are two different axes, and `architecture/` drifts from reality because
  no step forces its update.

- **Discovery costs too much per visit.** Across the ecosystem there is **no
  settled convention** — each repo organizes `planning/` differently (this repo
  by lifecycle; `httpware`/`modern-di`/`lite-bootstrap` by type, with and
  without archiving). The flat date-prefixed pairs also **sort incorrectly when
  two changes land on the same date** — the date prefix has no intra-day
  tiebreak, so the "timeline" silently breaks exactly when activity is highest.

- **Too much ceremony for small work.** Every tracked change pays the full
  `design.md` + `plan.md` + frontmatter + README cost even when the diff is a
  two-line fix. `lite-bootstrap` already grew a *lightweight-plan template*
  (after a 222-line plan shipped a 2-line edit) and this repo's `CLAUDE.md`
  already carves out a *tiny-change lane* — the lanes exist informally but are
  not codified into the directory convention.

- **Ecosystem inconsistency.** The four repos diverge on every axis: organizing
  principle (lifecycle vs. type), archiving (move vs. flat-forever), extra dirs
  (`audits/`, `retros/`, `scripts/`, `engineering.md`), templates, and indexes.
  There is no single convention to converge them.

OpenSpec ([concepts](https://github.com/Fission-AI/OpenSpec/blob/main/docs/concepts.md))
resolves the first pain by separating two spaces — a living source of truth
organized by capability, and `changes/` organized by proposal that archive after
deploy. Adopting its *spatial* model (without its heavyweight spec-delta
machinery, and without moving the truth home this repo already has) fixes
truth-vs-history, and the supporting decisions below fix the other three.

## Non-goals

- **Relocating `architecture/`.** It stays at the repo root as the living-truth
  home. Moving it under `planning/` would force a wide link rewrite across
  `CLAUDE.md` and `docs/` for no coherence gain — naming the promotion boundary
  achieves the same two-axis clarity at a fraction of the cost.

- **Rolling the convention out to the other three repos.** This spec ships the
  convention + this repo's migration. `httpware`/`modern-di`/`lite-bootstrap`
  adoption is separate follow-on work, demand-gated per repo.

- **Formal OpenSpec spec-deltas.** No `ADDED`/`MODIFIED`/`REMOVED` requirement
  blocks, no delta-application step. Promotion is a hand-edit of the affected
  `architecture/<capability>.md`. The delta format only pays off with the
  OpenSpec CLI applying and validating it; without that tool it is double-entry
  bookkeeping. Starting lightweight does not block a later upgrade — deltas
  would layer onto the same truth-home + `changes/` layout if a
  machine-checkable-spec need ever surfaces.

- **An index generator / lint check.** `planning/README.md`'s index stays
  hand-maintained, as today. A `just plans`-style generator and "frontmatter
  must parse" CI are deferred (see [`planning-conventions`](../archived/2026-06-10-planning-conventions-design.md)
  Non-goals — the reasoning is unchanged).

- **Trimming or rewriting archived prose.** Existing shipped specs/plans move
  into the new layout verbatim; only their location and frontmatter linkage
  change.

- **mkdocs-serving `planning/`.** `planning/` is not in `docs_dir` today and
  this change does not add it.

## Design

### 1. The model: two axes, never mixed

The convention rests on one distinction:

> **`architecture/` (repo root) is the present.** One file per capability,
> describing what the system does *now*. Living prose, updated whenever a change
> ships. This is exactly what it is today — the change is to *name* it as the
> truth home and *force* its update on ship.
>
> **`planning/changes/` is the past-and-pending.** One folder per change,
> describing *how* a piece of behavior got (or will get) there. Frozen once
> shipped.

Everything flows through `changes/`: a change is proposed there, executed there,
and — on ship — **promotes** its conclusions into `architecture/` before being
archived. A reader who wants current truth reads `architecture/`; a reader who
wants the rationale behind it follows the promotion back to the archived change
bundle.

The two spaces are two top-level homes (`architecture/` at root, `planning/` for
history) rather than nested under one dir. Naming the boundary — not co-locating
the dirs — is what removes the muddle.

### 2. Directory layout (portable — byte-identical structure in every repo)

```
architecture/          # LIVING TRUTH — one file per capability, at the repo root
  relay.md  timers.md  dlq.md  drain.md  metrics.md  test-broker.md

planning/
  README.md            # portable Conventions section + repo-specific Index section
  changes/
    active/
      YYYY-MM-DD.NN-<slug>/
        design.md      # spec — the thinking            (FULL lane)
        plan.md        # plan — the sequencing          (FULL lane)
        change.md      # OR this single file instead    (LIGHTWEIGHT lane)
    archive/
      YYYY-MM-DD.NN-<slug>/   # same shape, frozen, after ship
  releases/<semver>.md
  audits/<YYYY-MM-DD>-<slug>.md
  retros/<YYYY-MM-DD>-<slug>.md
  deferred.md
  _templates/
    design.md  plan.md  change.md
```

Repos that have no `architecture/` today name their own truth home in the
convention (`httpware` → `engineering.md`); the portable rule is "promotion
targets *your* truth home," not "the truth home must be `architecture/`."

Repo-specific reference docs that are not part of the portable core sit at
`planning/` root and are labelled as extras — in this repo, `lint-suppressions.md`.

### 3. Change bundle identity and the same-date problem

A change bundle is a **folder** named `YYYY-MM-DD.NN-<slug>/`:

- The folder groups the change's artifacts (`design.md` + `plan.md`, or
  `change.md`) and can hold incidental per-change files (a scratch diagram, a
  captured query plan) without polluting a flat directory.
- The **`.NN` intra-day counter** (zero-padded, `.01`, `.02`, …) is the fix for
  the same-date sort break: two changes on `2026-06-13` are
  `2026-06-13.01-foo/` and `2026-06-13.02-bar/`, which sort stably in creation
  order. The date is kept (at-a-glance recency on `ls`), and `.NN` provides the
  tiebreak the bare date lacked.
- `<slug>` is a kebab-case description, not a story ID (unchanged from today).

The shared folder name *is* the pairing — no separate cross-check between two
sibling files is needed (the `plan.md` `spec:` field is retained anyway as a
sanity field; see §6).

### 4. Three ceremony lanes

The author picks a lane when proposing the change. Triggers are documented in
the convention and mirrored in `CLAUDE.md`.

| Lane | Artifact(s) in `changes/active/<id>/` | Use when |
|------|---------------------------------------|----------|
| **Full** | `design.md` + `plan.md` | Real design judgment; a new file/module; a public-API change (add/remove/rename); cross-cutting or multi-file work; non-trivial test design |
| **Lightweight** | `change.md` (one file: goal · approach · file list · verification) | Small-but-real: ≲30 LOC net, ≤2 files, no new file, no public-API change, test is a single straightforward addition |
| **Tiny** | *none* — conventional commit only, never enters `changes/` | Typo, dep bump, linter/formatter/CI tweak, mechanical rename to satisfy a just-landed convention, single-line config |

The full-lane triggers are the inverse of the lightweight triggers, lifted
nearly verbatim from `lite-bootstrap`'s lightweight-plan template (a
field-tested boundary). If a change is ambiguous between lanes, the heavier lane
wins — under-documenting real design is the costlier error.

A lightweight change can be **promoted to full mid-flight** if it outgrows its
lane: rename `change.md`'s content into `design.md` + `plan.md`. Cheap, manual.

### 5. Lifecycle and promotion (hand-promotion, no deltas)

```
   propose ──▶ changes/active/<id>/   status: draft
                       │  spec approved → plan written
                       │                  status: approved
                       ▼  implementing PR merges
              ┌────────────────────────────────────────────┐
              │ 1. hand-edit affected architecture/<cap>.md  │
              │    to reflect the new truth                   │
              │ 2. git mv changes/active/<id>/                │
              │           → changes/archive/<id>/             │
              │    status: shipped                            │
              │ 3. fill pr: + outcome: in frontmatter         │
              │ 4. move the change's line in README index     │
              └────────────────────────────────────────────┘
```

Promotion (step 1) is the load-bearing act that keeps `architecture/` true: the
same PR that lands the code edits the capability file(s) it changed. There is no
intermediate delta artifact — the diff to `architecture/<capability>.md` *is*
the record of what the truth used to be, recoverable via `git log -p`. The
archived change bundle carries the *why*.

A change that touches no capability contract (a pure internal refactor, a docs
fix) promotes nothing and skips step 1.

### 6. Frontmatter (carried over unchanged from the current schema)

`design.md` / `change.md`:

```yaml
---
status: draft | approved | shipped | superseded
date: 2026-06-13
slug: portable-planning-convention
supersedes: null        # or <slug> of an older spec this overturns
superseded_by: null     # or <slug> of a newer spec that overturns this
pr: null                # or "NN", set when merged
outcome: null           # or "merged 2026-06-13 as #NN"
---
```

`plan.md`:

```yaml
---
status: draft | approved | shipped
date: 2026-06-13
slug: portable-planning-convention
spec: portable-planning-convention   # paired bundle slug, sanity field
pr: null
---
```

`architecture/<capability>.md` files get **no frontmatter** — they are living
prose, dated by git, with no lifecycle of their own (also unchanged from today).
`supersedes`/`superseded_by` preserve the cross-spec linkage introduced by
`planning-conventions`.

### 7. Audits and retros — dedicated dirs

Two artifact types do not map cleanly onto a single change bundle and get
first-class dirs (matching `httpware`/`modern-di` practice):

- **`audits/<date>-<slug>.md`** — a findings report from a code/docs/bug-hunt
  sweep. An audit typically *spawns* many fix changes (each its own bundle in
  `changes/`), so it cannot live as one bundle's `design.md`. The audit doc is
  the parent record; individual fixes reference it. This repo's existing
  `*-findings.md` files (the 2026-06-12 code + docs audits) move here.
- **`retros/<date>-<slug>.md`** — reflection after a body of work (a release, an
  audit cycle). Distinct from a release note (which describes *what shipped* for
  users); a retro is *what we learned* for ourselves.

Both are date-prefixed flat files (they are singular, not paired, and rarely
collide intra-day; `.NN` is available if they ever do).

### 8. `releases/`, `deferred.md`, `_templates/`

- **`releases/<semver>.md`** — unchanged. Per-release notes keyed by semver,
  as today (`0.9.0.md`, `0.9.1.md`).
- **`deferred.md`** — the long-tail register of real-but-unscheduled items with
  revisit triggers, unchanged in purpose. **Renamed** from `deferred-work.md`
  to `deferred.md` for cross-repo consistency (`modern-di` already uses this
  name; shorter).
- **`_templates/`** — gains a third template, `change.md` (the lightweight
  lane). `design.md` and `plan.md` carry over with their date placeholders and
  the path reference updated to `changes/active/`.

### 9. The convention doc itself — what makes it portable

`planning/README.md` has two sections:

1. **Conventions** — the rules in this Design (§1–§8), written repo-agnostically
   (the truth home is referred to abstractly as "the repo's truth home," with
   `architecture/` as this repo's instance). This section is **identical across
   all four repos**. It is the thing copied when adopting the convention
   elsewhere.
2. **Index** — repo-specific. Lists active changes (one line each), recent
   archive (with `pr:` + date), and a pointer to the truth home
   (`architecture/`). Hand-maintained; updated in the same PR that ships or
   archives a change.

The `_templates/` files are likewise byte-identical across repos. Adoption
elsewhere = copy `README.md`'s Conventions section + `_templates/`, create the
empty dir skeleton, and point that repo's `CLAUDE.md` Workflow + truth home at
it.

### 10. `CLAUDE.md` update

Edits confined to the `## Workflow` section — the `## Architecture` pointers are
**untouched** because `architecture/` does not move:

1. Rewrite the per-feature pipeline path strings: spec/plan now live in
   `planning/changes/active/YYYY-MM-DD.NN-<slug>/{design,plan}.md`; on merge the
   bundle moves to `planning/changes/archive/` and the affected
   `architecture/<capability>.md` is promoted.
2. Replace the tiny-change-lane paragraph with the three-lane table (§4).
3. Add one sentence naming `architecture/` as the promotion target so the
   truth ↔ history boundary is explicit in the AI-enforced instructions.

### 11. Migration of this repo (first adopter, one PR)

Performed in the same PR that ships this spec's plan. Note that because
`architecture/` stays put, there is **no mass link rewrite** — the migration is
contained to `planning/`:

1. **`architecture/` (root)** — unchanged. No move, no link rewrite.
2. **Regroup archived pairs:** each `planning/archived/<date>-<slug>-design.md`
   + `-plan.md` → `planning/changes/archive/<date>.NN-<slug>/{design,plan}.md`.
   Assign `.NN` per date in PR-merge order (several dates already collide —
   `2026-06-03`, `-04`, `-09`, `-11`, `-12` each have ≥2 pairs — so this is
   where the tiebreak first earns its keep). `git mv` preserves blame.
3. **Move findings:** `planning/archived/2026-06-12-code-audit-findings.md` and
   `…-docs-audit-findings.md` → `planning/audits/`.
4. **`planning/active/` → `planning/changes/active/`** (currently only
   `.gitkeep`; this spec's own bundle lands here during execution — see step 9).
5. **`planning/deferred-work.md` → `planning/deferred.md`** (`git mv`; update
   the inbound link in `README.md`).
6. **`planning/releases/`** unchanged. **`planning/lint-suppressions.md`** stays
   at root, labelled a repo-specific extra.
7. **Rewrite `planning/README.md`** into the Conventions + Index two-section
   form (§9). Set `superseded_by: portable-planning-convention` +
   a `## Superseded by` callout on the archived `planning-conventions` design.
8. **Add `planning/_templates/change.md`**; update `design.md` / `plan.md` path
   references to `changes/active/`.
9. **Update `CLAUDE.md`** `## Workflow` (§10). **Self-migrate** this change's
   own `design.md` + `plan.md` from
   `planning/active/2026-06-13-portable-planning-convention-*.md` into
   `planning/changes/archive/2026-06-13.01-portable-planning-convention/` when
   the PR merges, with `status: shipped`, `pr:`, `outcome:` filled. No
   `architecture/` promotion applies — this change defines the convention
   itself (which lives in `README.md`), not a capability of the library.

After migration, `planning/active/` and `planning/archived/` no longer exist;
the post-state matches the tree in §2 exactly, with `architecture/` at root
unchanged.

## Operations

None. No DNS, infra, or external-account changes. Pure in-repo file moves and
doc edits, all within `planning/`.

## Testing

Configuration + content + file moves; correctness is verified by:

- `just lint-ci` passes (markdown EOF + `ruff` on any touched config — no code
  touched, so this is the markdown/format gate only).
- `just docs-build` (`mkdocs build --strict`) passes — confirms no `docs/` link
  broke (none should, since `architecture/` and `docs/` are untouched).
- A repo-wide `grep -rn "deferred-work.md"` and `grep -rn "planning/active\|planning/archived"`
  return zero stale references outside this spec's own archived bundle.
- Every artifact under `planning/changes/**` has parseable YAML frontmatter —
  spot-checked on review.
- `planning/README.md` Index links resolve — manual click-through on PR preview.
- The post-migration tree exactly matches §2.

No new pytest, no new CI job.

## Risk

- **Convention drift on the next change / next repo.** A contributor could
  ignore the lanes or — the new failure mode — forget the `architecture/`
  promotion on ship, letting truth drift again. *Mitigation:* the promotion step
  is in the lifecycle diagram, the templates make the shape copy-pasteable, and
  `CLAUDE.md` names `architecture/` as the promotion target so PR review catches
  a missing promotion the same way it catches a missing test. The
  hand-maintained Index drifting is an accepted loss (PR-review backstop), same
  as under `planning-conventions`.

- **Three lanes add a judgment call.** Picking a lane is a per-change decision.
  *Mitigation:* the trigger table makes it near-mechanical, and "heavier lane
  wins on ambiguity" removes the agonizing case. The lanes already exist
  informally — codifying them reduces net judgment, not adds it.

- **`.NN` assignment for already-archived pairs is a judgment call.** Several
  archived dates collide; assigning `.01`/`.02` requires reading merge order.
  *Mitigation:* PR numbers in the existing `README.md` index give the order
  directly; a wrong tiebreak is cosmetic (both bundles still exist and sort
  adjacently).

- **`git mv` blame continuity.** Regrouping archived pairs into folders relies
  on `--follow`-aware tooling for blame. *Mitigation:* GitHub Web follows
  renames; `git log --follow <path>` works regardless. Low practical impact for
  planning artifacts.

- **Two top-level truth/history homes instead of one nested space.** Keeping
  `architecture/` at root means the "one space" is really two top-level dirs.
  *Mitigation:* this is the deliberate trade for `architecture/` staying put
  (the chosen, lower-cost option) — the boundary is made explicit by naming, and
  `architecture/` at the root is itself a widely understood convention.

- **Superseding `planning-conventions` mid-history.** The older spec's
  `active/`+`archived/` vocabulary is now wrong. *Mitigation:* set
  `superseded_by: portable-planning-convention` on it and a one-line
  `## Superseded by` callout, per the supersession protocol it itself defined.
  It stays in the archive — its migration *did* ship; only the layout it
  introduced is replaced.
