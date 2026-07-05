---
summary: Make mkdocs --strict a PR gate (parallel docs job) with native link/anchor validation, pin docs deps, fix the make_outbox_table 63-byte docstring, and one-time-audit README.
---

# Design: Docs CI drift guard

## Summary

Add a PR-time guard against the class of documentation drift that a 2026-07-05
docs audit (`docs/audit-drift-fixes`, PR #125) surfaced. Today `mkdocs build
--strict` runs **only** in `docs.yml` on push to `main` (for deploy), so a PR
that breaks an internal link or orphans a page is not caught until after merge;
broken `#anchor` fragments are not caught at all. This change makes the strict
build a parallel PR check, turns on mkdocs' native link/anchor validation, pins
the (currently unpinned) docs toolchain, fixes one source docstring that
carries the same wording bug the audit fixed in the docs, and does a one-time
correctness pass over `README.md` (which lives outside `docs_dir` and is
therefore invisible to mkdocs).

Deliberately **excluded**: any validation of the Python inside `docs/` code
fences (syntax-parse, type-check, or execute). That was considered and rejected
for cost / false-positive reasons — recorded in
[`../../decisions/2026-07-05-no-doc-code-fence-validation.md`](../../decisions/2026-07-05-no-doc-code-fence-validation.md).

## Motivation

The docs audit (PR #125) found, among ~20 fixes: three broken code samples,
a migration snippet missing a load-bearing CHECK, several stale numbers, and a
set of `#anchor` links that happened to resolve. CI caught **none** of it,
because:

- `mkdocs build --strict` — which fails on broken internal links and orphaned
  pages — is not part of the PR `checks` workflow (`_checks.yml`). It runs only
  in `docs.yml`, which triggers on push to `main` with `paths: docs/**`. So
  link/orphan breakage fails the **deploy** after merge, not the PR.
- mkdocs does not validate `#anchor` fragments by default; the audit relied on a
  throwaway script to check them.
- `docs/requirements.txt` pins nothing (`mkdocs`, `mkdocs-material`). The
  audit's strict build already emitted the upstream "MkDocs 2.0 will break all
  plugins/themes" warning — an unpinned major bump would break the build with no
  notice.

The cheapest durable guards are all in-tool: promote the existing strict build
to a PR gate and switch on mkdocs' own link/anchor validation. No new script to
own.

## Non-goals

- No validation of Python code fences in `docs/` (syntax/type/execute) — see the
  decision file. This is the single biggest scope cut and the reason the change
  stays small.
- No ongoing link-checking of `README.md`. Its links are predominantly stable
  external badges/URLs; an external link-checker is flaky and low-value. README
  is fixed once here (§4) and not added to a recurring guard.
- No change to what `docs.yml` does at deploy time; the PR job simply runs the
  same command earlier.

## Design

### 1. Strict docs build as a parallel PR job

Add a `docs` job to `.github/workflows/_checks.yml` (the reusable workflow that
`ci.yml` calls on every PR and push to `main`). It runs the **existing**
recipe, identical to the deploy build:

```yaml
  docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: extractions/setup-just@v4
      - uses: astral-sh/setup-uv@v8.2.0
      - run: just docs-build
```

Parallel with `lint` and `pytest`; isolated from the lint runner; the same
`just docs-build` command PR-time and deploy-time, so they cannot diverge.

### 2. Native link + anchor validation in mkdocs

Add a `validation:` block to `mkdocs.yml` (supported since mkdocs 1.6; the repo
resolves 1.6.1):

```yaml
validation:
  omitted_files: warn
  absolute_links: warn
  unrecognized_links: warn
  anchors: warn
```

Under `--strict`, every `warn` is promoted to an error, so a broken `#anchor`
fragment or an unrecognized internal link now fails the build. This subsumes the
audit's throwaway anchor script with zero maintained code.

### 3. `make_outbox_table` docstring fix (folds in #2)

`faststream_outbox/schema.py`'s `make_outbox_table` docstring says it raises
`ValueError` when `table_name` pushes the NOTIFY channel `outbox_<table_name>`
past Postgres' 63-byte limit. The guard (`validate_table_identifiers`) actually
binds on the **longest derived identifier** — an index/constraint name such as
`<table_name>_pending_idx` / `<table_name>_timer_id_uq`, which is longer than
the channel prefix. This is the exact wording the audit corrected in
`docs/operations/checklist.md`; fixing the docstring closes it at the source so
docs and code agree. One-line prose change; no behavior change.

### 4. One-time README audit (folds in #3)

`README.md` is outside `docs_dir`, so neither the strict build nor mkdocs
validation ever touches it. Do a one-time correctness/consistency pass with the
same rigor as the docs audit: verify every code sample's imports/symbols/kwargs
against source, check stated numbers/claims, and check links. Fix confident
drift in place; surface any judgment calls. Findings are recorded inline in the
implementing PR (a full `planning/audits/` report is overkill for a single
file); if the pass turns up something structural, it spawns its own change.

### 5. Pin the docs toolchain

`docs/requirements.txt` gets upper-bounded pins so the strict build (now a PR
gate) is reproducible and immune to a breaking major:

```
mkdocs>=1.6,<2
mkdocs-material>=9,<10
```

(Exact lower bound for `mkdocs-material` set to the resolved version's series at
implementation time.)

## Testing

- **Negative-to-positive on the guard:** on the branch, temporarily introduce a
  broken `#anchor` link and a broken relative `.md` link in a docs page and
  confirm `just docs-build` now **fails**; revert. Confirm a clean tree
  **passes**. This proves §1+§2 actually gate.
- **README:** re-run the audit checks (imports/symbols resolve, links resolve)
  after fixes; `git diff` review.
- **Docstring:** `just lint-ci` (ruff + ty) stays green; no runtime surface.
- **Planning:** `just check-planning` validates this bundle + the decision file.
- **Full CI on the PR:** lint, the new docs job, and the pytest matrix all green.

## Risk

- **Low — new PR gate turns red on pre-existing latent breakage.** If any
  current docs link/anchor is already broken under the stricter validation, the
  first run fails. Mitigation: PR #125 already left links/anchors clean; the
  branch build is run before opening the PR.
- **Low — pinning drifts from upstream.** Upper bounds mean a manual bump to
  adopt mkdocs-material 10 / mkdocs 2. Acceptable and intended (that is the
  point of the pin); the bound documents the tested range.
- **Negligible — docstring/README are prose-only**, no behavior change.
