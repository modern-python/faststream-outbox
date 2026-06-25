---
summary: Flatten changes/ (drop active/archive), make status frontmatter the sole lifecycle state, add a summary field, and replace the hand-maintained README Index with a stdlib generator (just index).
---

# Design: Flatten changes/ and generate the index from frontmatter

## Summary

The planning convention currently encodes a change's lifecycle in **three**
places that must be hand-synced on every ship: the `status:` frontmatter, the
`changes/active/` vs `changes/archive/` directory, and the `### Active` vs
`### Archived` README Index sections. This change removes two of the three.
`changes/` becomes a flat directory; `status:` frontmatter becomes the sole
encoding of lifecycle state; and the README Index — the derived listing — is
replaced by a stdlib generator (`planning/index.py`, run via `just index`)
that prints the listing to stdout on demand and is never committed. A new
single-line `summary:` frontmatter field, backfilled from the existing curated
Index lines, supplies each entry's one-liner. The implementing PR sets
`status: shipped` in-branch, so the post-merge ritual collapses to just the
`architecture/` promotion.

## Motivation

The same lifecycle state lives in three encodings:

1. `status:` frontmatter (`draft|approved|shipped|superseded`).
2. The directory (`active/` vs `archive/`).
3. The README Index (`### Active` vs `### Archived (shipped)`).

Every ship must keep all three in sync — `git mv` the bundle, flip `status`,
move the Index line, rewrite the Index link. The evidence that this is mostly
ceremony: the **Active section is currently empty** (`_None._`) and all 15
bundles sit in `archive/`. Changes flow through fast, so `active/` spends
nearly all its life empty — its one benefit ("glance at what's in flight")
rarely pays out, while the multi-step sync cost is paid on *every* ship and
can silently drift (a forgotten `git mv` leaves `status: shipped` in
`active/`).

The directory split is the weakest encoding: it is a `shipped?` boolean that
`status: shipped` already captures losslessly (`superseded` is orthogonal —
a superseded change is still archived). The README Index is derived data that
should be a *query*, not a maintained document. Treating it as a generator
output that is never cached eliminates the entire drift class at the root:
there is no second copy to fall out of sync, because each bundle's frontmatter
becomes the only source of truth.

The user confirmed the README Index — not an `ls` of `active/` — is the
surface they actually scan, which removes the directory's last justification.

## Non-goals

- No CI staleness check — nothing is derived **and** committed, so nothing can
  go stale. The generator output is ephemeral.
- No auto-injection of the generated listing back into the README.
- No new runtime dependency — the generator is stdlib-only.
- No change to the three-lane model, the bundle naming (`YYYY-MM-DD.NN-slug`),
  or the `architecture/`-is-truth axis.

## Design

### 1. Flat `changes/` directory

Collapse `planning/changes/{active,archive}/` into a flat
`planning/changes/`. All 15 existing bundles move up one level via `git mv`;
both subdirectories and their `.gitkeep` files are deleted. After this change a
bundle's path is `planning/changes/YYYY-MM-DD.NN-<slug>/`, and `status:`
frontmatter is the **sole** encoding of lifecycle state.

This change's own bundle already lives at the flat path
(`planning/changes/2026-06-20.01-flat-changes-generated-index/`) — it eats its
own dog food.

### 2. `summary:` frontmatter field

Add a **single-line** `summary:` field to every bundle's `design.md` /
`change.md` frontmatter (and to the `_templates/` starters). The value is the
one-line description that currently lives in the README Index, moved to live
*with* the change it describes. Single-line (no YAML folding) keeps the
generator's frontmatter parser trivial — see §3.

Backfill for the 15 existing bundles is mechanical: each line already exists in
the current README Index.

### 3. The generator (`planning/index.py`)

A stdlib-only Python script, run via a new `just index` recipe. It:

- globs `planning/changes/*/`, reads each bundle's `design.md` (falling back to
  `change.md`) frontmatter;
- parses frontmatter with a ~20-line hand reader (the fields are simple
  single-line scalars — `status`, `date`, `slug`, `pr`, `summary`,
  `supersedes`, `superseded_by`), so **no PyYAML** is needed (confirmed absent
  from the env);
- groups by status into three sections — **In progress** (`draft` + `approved`),
  **Shipped**, **Superseded** — each sorted by `date` descending;
- prints a Markdown listing to **stdout only**. It never writes a file and is
  never committed.

Per-entry output shape:

```
- **<slug>** (#<pr>, <date>) — <summary>
```

with `supersedes` / `superseded_by` rendered as a trailing parenthetical link
when present. An empty group prints `_None._` (matching the current Index
idiom).

### 4. Slim README

`planning/README.md` keeps the portable **Conventions** prose and the **Other**
pointers (architecture/, audits/, deferred.md, lint-suppressions). The entire
**Index** section (current lines ~69–142) is deleted and replaced by a
one-line note: the listing is generated — run `just index`.

The Conventions prose is edited to describe the new model: a flat `changes/`
directory, `status:` as the sole lifecycle state, the `summary:` field, the
generator, and the single-step lifecycle (§5). Every "on merge, move to
`archive/` / flip to `shipped`" sentence is removed.

### 5. Single-step lifecycle

A change moves `draft` → `approved` while it is being designed (spec/plan exist
before code). `summary:` is written then, at creation — it is the change's
one-liner. The **implementing PR itself** sets `status: shipped` and fills
`pr:` / `outcome:` **in the branch**, alongside the code and the
`architecture/<capability>.md` promotion. On merge there is **no bookkeeping
step**: no folder move (gone with active/archive) and no separate status flip.

This is strictly better in one respect: `status: shipped` lands inside the diff
the reviewer reads, so it is reviewed like any other line rather than applied
as unreviewed post-merge housekeeping. The two frictions are both benign:

- `pr:` is known the moment the PR opens; fill it with one more commit to the
  same branch (or during the final review pass).
- `outcome:` is written at ship time describing what landed; it rides in the
  branch.

The only thing that inherently happens "at ship" is the
`architecture/<capability>.md` promotion — a content edit that already rides in
the implementing PR.

### 6. CLAUDE.md + internal cross-links

- Update CLAUDE.md's **Workflow** section: `changes/active/...` paths → flat
  `changes/...`; drop "on merge the bundle moves to `archive/`"; describe the
  single-step lifecycle; mention `just index` for the listing.
- Rewrite the internal `changes/archive/...` cross-links inside bundle bodies
  (`2026-06-13.01-portable-planning-convention/{design,plan}.md`,
  `2026-06-16.01-actionable-schema-drift-error/plan.md`, and any others a final
  grep finds) to `changes/...`.
- Update `planning/_templates/` starters to include the `summary:` field.

## Out of scope

- Retroactively re-deriving `outcome:` for old bundles that left it `null`.
- A `--check`/CI mode for the generator (see Non-goals).
- Porting the revised convention back to the other modern-python repos — the
  README Conventions section stays portable, but rollout elsewhere is a
  separate effort (consistent with the existing portability note).

## Testing

- `just index` prints all 15 (+1) bundles, correctly grouped and sorted, with
  no missing/garbled summaries — eyeball the output.
- **No unit test.** `planning/index.py` is portable dev tooling (it copies to
  other repos verbatim with the convention); a test in `tests/` would couple it
  to this repo's pytest + `--cov=.` gate and be unclear what to do with on copy.
  No coverage config is needed: `planning/` has no `__init__.py` and nothing
  imports the script, so `--cov=.` never traces it and the 100% gate is
  unaffected. It is verified by running it — `just index` must exit 0 and render
  correctly. If the parser breaks, the listing visibly breaks; the stakes are a
  human-read index, not runtime.
- `grep -r 'changes/archive' planning` returns no matches after the rewrite.
- `grep -rL '^summary:' planning/changes/*/design.md` (and `change.md`) is
  empty — every bundle has a summary.
- `just lint` clean (the new `.py` passes ruff/ty).

## Risk

- **Broken links to moved bundles** (medium likelihood, low impact). Any link
  using the `changes/archive/...` path breaks on flatten. Mitigation: the grep
  gate above; rewrite all hits in the same change.
- **A bundle missing `summary:`** (low × low). The generator should emit a
  visible placeholder (e.g. `— (no summary)`) rather than crash, and the
  backfill grep gate catches omissions.
- **Generator drift from frontmatter format** (low × low). Keeping `summary:`
  single-line and the field set small keeps the hand parser robust; a malformed
  bundle surfaces immediately on the next `just index` rather than silently.
- **Forgetting to flip `status: shipped` in-branch** (low × low). Symmetric
  with the old "forgot to `git mv`" risk, but now visible in the PR diff, so
  more likely to be caught in review, not less.
