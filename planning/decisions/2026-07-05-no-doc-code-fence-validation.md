---
status: accepted
summary: Do not validate Python inside docs/ code fences (no syntax/type/execute check); rely on mkdocs --strict for links/anchors and human review for sample correctness.
supersedes: null
superseded_by: null
---

# No automated validation of docs code fences

**Decision:** The docs CI guard (change `2026-07-05.01-docs-ci-drift-guard`)
will **not** parse, type-check, or execute the Python inside `docs/` code
fences. Code-sample correctness stays a human-review concern; CI guards only
links, anchors, and page structure via `mkdocs build --strict`.

## Context

The 2026-07-05 docs audit (PR #125) fixed three broken code samples, including
`docs/usage/testing.md` calling `pub.publish({...})` without the required
`session=` — valid syntax, a `TypeError` at runtime. When designing the CI
guard, three levels of fence validation were on the table:

1. **Syntax-parse all fences** (AST) — cheap, but would not have caught the
   `session=` bug (valid syntax) or stale numbers.
2. **Type-check curated fences** against the real API (assemble + `ty`) — would
   catch the `session=` class, but requires a fragment-vs-complete convention
   because most fences are intentional fragments (undefined `broker`, `...`
   bodies, bare signatures) that a type-checker floods with false positives.
3. **Execute curated fences** (doctest-style) — highest fidelity, but needs a
   harness, fixtures, mocks, and a Postgres service for integration samples.

## Decision & rationale

Rejected all three. The value/cost ratio is poor for this codebase:

- The failure it targets is **rare** — the audit was the first docs sweep to
  find broken samples, and PR #125 already fixed the backlog.
- Levels 2 and 3 both require a **fragment-vs-complete tagging convention** the
  docs do not have. Retrofitting it across ~23 pages, then maintaining it, is
  ongoing overhead on every future doc edit — exactly the "expensive" cost the
  maintainer flagged when scoping this change.
- False positives on intentional fragments would train reviewers to ignore the
  check, which is worse than no check.
- The cheap in-tool guards (`mkdocs --strict` for links/orphans + native
  `validation.anchors`) cover the **structural** drift class at zero maintained
  code, and human review already covers sample correctness on the rare edit.

So the guard covers links/anchors/structure and stops there; sample correctness
is not automated.

## Revisit trigger

Reopen if **either**:

- doc code samples break ≥3 times within a release cycle (the failure stops
  being rare), at which point curated type-checking (level 2) earns its keep; or
- a clean fragment-vs-complete convention emerges naturally (e.g. an
  `examples/` directory of runnable snippets the docs `--8<--` include), which
  removes the false-positive obstacle and makes level 2 or 3 cheap to add.
