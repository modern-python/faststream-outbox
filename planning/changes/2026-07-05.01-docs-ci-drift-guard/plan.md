# docs-ci-drift-guard — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `mkdocs build --strict` a PR gate with native link/anchor
validation, pin the docs toolchain, fix the `make_outbox_table` 63-byte
docstring, and one-time-audit `README.md`.

**Spec:** [`design.md`](./design.md)

**Branch:** `chore/docs-ci-drift-guard`

**Commit strategy:** Per-task commits; all ship in one PR alongside this bundle.

---

### Task 1: Enable mkdocs link/anchor validation

**Files:**
- Modify: `mkdocs.yml`

Add the `validation:` block so `--strict` fails on broken internal links and
`#anchor` fragments.

- [ ] **Step 1: Add the validation block**

  Insert a top-level `validation:` key (mkdocs 1.6+):
  ```yaml
  validation:
    omitted_files: warn
    absolute_links: warn
    unrecognized_links: warn
    anchors: warn
  ```

- [ ] **Step 2: Prove it gates (negative test)**

  Temporarily append a broken anchor link (e.g. `[x](usage/basic.md#no-such-anchor)`)
  to a docs page, run `just docs-build`, confirm it **fails** with an anchor
  warning-as-error, then remove the temporary link.

- [ ] **Step 3: Confirm clean tree passes**

  `just docs-build` → "Documentation built"; no warnings-as-errors.

- [ ] **Step 4: Commit**

  ```bash
  git add mkdocs.yml
  git commit -m "docs: validate internal links and anchors under mkdocs --strict"
  ```

---

### Task 2: Pin the docs toolchain

**Files:**
- Modify: `docs/requirements.txt`

Upper-bound the docs deps so the (now gating) strict build is reproducible.

- [ ] **Step 1: Resolve current mkdocs-material series**

  `uvx --with-requirements docs/requirements.txt mkdocs --version` and note the
  installed `mkdocs-material` major (for the lower bound).

- [ ] **Step 2: Pin with upper bounds**

  ```
  mkdocs>=1.6,<2
  mkdocs-material>=<resolved-major>,<<next-major>
  ```

- [ ] **Step 3: Verify build still passes on the pins**

  `just docs-build` → passes.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/requirements.txt
  git commit -m "docs: pin mkdocs and mkdocs-material with upper bounds"
  ```

---

### Task 3: Add the strict docs build as a parallel PR job

**Files:**
- Modify: `.github/workflows/_checks.yml`

Add a `docs` job running `just docs-build`, parallel with `lint`/`pytest`.

- [ ] **Step 1: Add the job**

  Mirror the `lint` job's setup steps; final step `- run: just docs-build`.

- [ ] **Step 2: Sanity-check YAML**

  `uv run python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/_checks.yml'))"`
  (or `just` if a lint target covers workflows) → no parse error.

- [ ] **Step 3: Commit**

  ```bash
  git add .github/workflows/_checks.yml
  git commit -m "ci: run mkdocs --strict as a PR check"
  ```

---

### Task 4: Fix the `make_outbox_table` docstring (#2)

**Files:**
- Modify: `faststream_outbox/schema.py`

Correct the 63-byte-guard wording: the binding identifier is the longest
derived index/constraint name, not the NOTIFY channel.

- [ ] **Step 1: Edit the docstring**

  Reword the `Raises ValueError ...` line in `make_outbox_table` to say the
  guard trips when the **longest derived identifier** (e.g. `<table>_pending_idx`)
  exceeds 63 bytes — longer than the `outbox_<table>` channel.

- [ ] **Step 2: Lint stays green**

  `just lint-ci` → ruff + ty pass.

- [ ] **Step 3: Commit**

  ```bash
  git add faststream_outbox/schema.py
  git commit -m "docs: correct make_outbox_table 63-byte guard docstring"
  ```

---

### Task 5: One-time README audit (#3)

**Files:**
- Modify: `README.md` (as findings require)

Audit `README.md` against source + `docs/`; fix confident drift, surface
judgment calls.

- [ ] **Step 1: Verify code samples**

  For every code block in `README.md`, confirm imports/symbols/kwargs resolve
  against `faststream_outbox/` (grep `__init__.py` exports; check signatures).

- [ ] **Step 2: Verify claims, numbers, links**

  Check stated defaults/numbers against source; check every link (badges,
  docs-site links, GitHub URLs) resolves.

- [ ] **Step 3: Apply fixes; surface judgment calls**

  Fix confident drift in place. Any ambiguous/design-choice item → raise to
  maintainer, do not decide unilaterally.

- [ ] **Step 4: Re-verify**

  Re-run the import/symbol/link checks on the edited README.

- [ ] **Step 5: Commit**

  ```bash
  git add README.md
  git commit -m "docs: fix drift in README"
  ```

---

### Task 6: Finalize the bundle and open the PR

**Files:**
- Modify: `planning/changes/2026-07-05.01-docs-ci-drift-guard/design.md` (finalize `summary:`)

- [ ] **Step 1: Validate planning**

  `just check-planning` → passes (bundle + decision frontmatter, lane, spec link).

- [ ] **Step 2: Full local gate**

  `just lint-ci` and `just docs-build` both green.

- [ ] **Step 3: Push + open PR; watch CI**

  Push the branch, open the PR, and watch lint + the new `docs` job + the pytest
  matrix to green.
