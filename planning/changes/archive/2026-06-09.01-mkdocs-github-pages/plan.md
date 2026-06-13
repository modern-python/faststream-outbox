---
status: shipped
date: 2026-06-09
slug: mkdocs-github-pages
spec: mkdocs-github-pages
pr: "45"
---

# Migrate Docs Hosting from Read the Docs to GitHub Pages — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Read the Docs deploy pipeline with a GitHub Actions + GitHub Pages deploy mirroring `modern-di`, served at `faststream-outbox.modern-python.org`. Bump CI action pins to the same baseline along the way.

**Architecture:** Eight in-repo file changes (one new workflow, one new docs/CNAME, one Justfile target, one mkdocs.yml field, two file deletions/edits for RTD removal, two action-pin bumps). Out-of-repo DNS + GH Pages settings happen after merge (documented in the spec, not in this plan). All commits land on a single feature branch.

**Tech Stack:** mkdocs + mkdocs-material (existing), `mkdocs gh-deploy` (force-push to `gh-pages`), GitHub Actions, `just`, `uvx`.

**Spec:** `planning/specs/2026-06-09-mkdocs-github-pages-design.md`

**Branch:** Work on a new branch `feat/mkdocs-github-pages` off `main`. Do not commit to `main` directly.

---

## Pre-flight

- [ ] **Step 1: Create the feature branch**

```bash
git checkout -b feat/mkdocs-github-pages
git status
```

Expected: `On branch feat/mkdocs-github-pages` with clean working tree.

- [ ] **Step 2: Confirm assumptions about current state**

```bash
ls .readthedocs.yaml docs/requirements.txt docs/CNAME 2>&1
grep -c "site_url" mkdocs.yml
grep "docs-deploy" Justfile
```

Expected:
- `.readthedocs.yaml` and `docs/requirements.txt` exist.
- `docs/CNAME` does NOT exist (`ls` reports no such file).
- `site_url` grep returns `0` (currently absent).
- `docs-deploy` grep returns no match (Justfile has no docs target yet).

If any expectation fails, STOP and re-read the spec — assumptions changed.

---

## Task 1: Add the `just docs-deploy` target

**Files:**
- Modify: `Justfile` (append at end)

- [ ] **Step 1: Read the current Justfile end**

```bash
tail -5 Justfile
```

Expected: last target is `publish:` with `uv publish --token $PYPI_TOKEN`. Confirms there's no trailing blank-line surprise.

- [ ] **Step 2: Append the `docs-deploy` target**

Append these lines to the end of `Justfile` (preserve a single blank line before the new target, single trailing newline at EOF):

```just

# Force-pushes built site to gh-pages; CI runs this on push to main.
# Manual invocation from a stale checkout will roll the live site back.
docs-deploy:
    uvx --with-requirements docs/requirements.txt mkdocs gh-deploy --force
```

- [ ] **Step 3: Verify the target is discoverable**

Run: `just --list | grep docs-deploy`

Expected: one line — `    docs-deploy           # Force-pushes built site to gh-pages; CI runs this on push to main.`

- [ ] **Step 4: Smoke-test mkdocs without deploying**

Run: `uvx --with-requirements docs/requirements.txt mkdocs build --strict --site-dir /tmp/mkdocs-smoke`

Expected: build completes with no warnings (`--strict` turns warnings into errors). Output ends with `INFO    -  Documentation built in N.NNs`. The `/tmp/mkdocs-smoke` directory now contains `index.html`. Clean up with `rm -rf /tmp/mkdocs-smoke`.

If the build fails, the current `mkdocs.yml` has a latent issue unrelated to this plan — STOP and report.

- [ ] **Step 5: Commit**

```bash
git add Justfile
git commit -m "chore: add just docs-deploy target for mkdocs gh-deploy

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Add `docs/CNAME`

**Files:**
- Create: `docs/CNAME`

- [ ] **Step 1: Create the file with the custom domain**

Create `docs/CNAME` containing exactly one line with a trailing newline:

```
faststream-outbox.modern-python.org
```

- [ ] **Step 2: Verify contents**

Run: `cat docs/CNAME && wc -l docs/CNAME`

Expected:
```
faststream-outbox.modern-python.org
       1 docs/CNAME
```

(One line, one newline at EOF.)

- [ ] **Step 3: Confirm mkdocs picks it up**

Run: `uvx --with-requirements docs/requirements.txt mkdocs build --strict --site-dir /tmp/mkdocs-smoke && cat /tmp/mkdocs-smoke/CNAME && rm -rf /tmp/mkdocs-smoke`

Expected: build succeeds, `CNAME` is copied into the built site with the same single-line contents.

- [ ] **Step 4: Commit**

```bash
git add docs/CNAME
git commit -m "docs: add CNAME for faststream-outbox.modern-python.org

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Add `site_url` to `mkdocs.yml`

**Files:**
- Modify: `mkdocs.yml` (insert one line after `site_name:`)

- [ ] **Step 1: Read the first 6 lines of mkdocs.yml**

```bash
head -6 mkdocs.yml
```

Expected current top:
```yaml
site_name: faststream-outbox
repo_url: https://github.com/modern-python/faststream-outbox
docs_dir: docs
edit_uri: edit/main/docs/
nav:
  - Introduction:
```

- [ ] **Step 2: Insert `site_url` after `site_name`**

After line 1 (`site_name: faststream-outbox`), insert:

```yaml
site_url: https://faststream-outbox.modern-python.org/
```

Result: lines 1–2 are `site_name:` followed by `site_url:`; the rest of the file is unchanged.

- [ ] **Step 3: Verify the edit**

```bash
head -3 mkdocs.yml
```

Expected:
```yaml
site_name: faststream-outbox
site_url: https://faststream-outbox.modern-python.org/
repo_url: https://github.com/modern-python/faststream-outbox
```

- [ ] **Step 4: Verify mkdocs still builds**

Run: `uvx --with-requirements docs/requirements.txt mkdocs build --strict --site-dir /tmp/mkdocs-smoke && grep -c 'canonical' /tmp/mkdocs-smoke/index.html && rm -rf /tmp/mkdocs-smoke`

Expected: build succeeds, grep returns `1` (the canonical link tag is now emitted in `index.html`).

- [ ] **Step 5: Commit**

```bash
git add mkdocs.yml
git commit -m "docs: set mkdocs site_url for canonical links and sitemap

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Add `.github/workflows/docs.yml`

**Files:**
- Create: `.github/workflows/docs.yml`

- [ ] **Step 1: Create the workflow**

Create `.github/workflows/docs.yml` with exactly this content (single trailing newline at EOF):

```yaml
name: Deploy Docs

on:
  push:
    branches: [main]
    paths:
      - "docs/**"
      - "mkdocs.yml"
      - ".github/workflows/docs.yml"
  workflow_dispatch:

concurrency:
  group: docs-deploy
  cancel-in-progress: true

permissions:
  contents: write

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
        with:
          fetch-depth: 0
      - uses: extractions/setup-just@v4
      - uses: astral-sh/setup-uv@v8.2.0
      - run: just docs-deploy
```

- [ ] **Step 2: Verify YAML parses**

Run: `python3 -c "import yaml, sys; yaml.safe_load(open('.github/workflows/docs.yml')); print('OK')"`

Expected: prints `OK`. (Any YAML error throws and exits non-zero.)

- [ ] **Step 3: Verify load-bearing fields are present**

Run:
```bash
python3 -c "
import yaml
d = yaml.safe_load(open('.github/workflows/docs.yml'))
assert d['name'] == 'Deploy Docs'
assert d['concurrency']['group'] == 'docs-deploy'
assert d['concurrency']['cancel-in-progress'] is True
assert d['permissions']['contents'] == 'write'
assert d['jobs']['deploy']['steps'][0]['with']['fetch-depth'] == 0
assert d['jobs']['deploy']['steps'][-1]['run'] == 'just docs-deploy'
print('OK')
"
```

Expected: prints `OK`. If any assert fails, the YAML was mistyped — fix and re-run.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/docs.yml
git commit -m "ci: add mkdocs gh-deploy workflow

Triggers on push to main when docs/, mkdocs.yml, or this workflow
change. Concurrency group serializes deploys; contents:write needed
for gh-pages branch push; fetch-depth: 0 needed for gh-deploy history.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Delete `.readthedocs.yaml`

**Files:**
- Delete: `.readthedocs.yaml`

- [ ] **Step 1: Confirm the file exists and matches expectations**

```bash
cat .readthedocs.yaml
```

Expected: the 14-line file (version 2, ubuntu-22.04, python 3.10, mkdocs config). If contents have drifted from the spec snapshot, pause and re-read the spec.

- [ ] **Step 2: Delete the file**

```bash
git rm .readthedocs.yaml
```

Expected: `rm '.readthedocs.yaml'`, working tree shows the deletion staged.

- [ ] **Step 3: Verify**

```bash
ls .readthedocs.yaml 2>&1; git status --short
```

Expected: `ls` reports the file does not exist; `git status --short` shows `D  .readthedocs.yaml`.

- [ ] **Step 4: Commit**

```bash
git commit -m "ci: remove Read the Docs config

Docs now deploy from .github/workflows/docs.yml. RTD project will be
archived in the RTD UI after the new URL is live (out-of-repo step).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Update `README.md` URLs

**Files:**
- Modify: `README.md` (4 line edits — lines 52, 84, 90, 110)

- [ ] **Step 1: Replace the three `/en/latest/` deep links**

Three substitutions in `README.md`. Each replaces a substring; do them in order. Use Edit (string replacement), not regex.

Edit 1 — line 52, relay tutorial:
- Old: `https://faststream-outbox.readthedocs.io/en/latest/usage/relay/`
- New: `https://faststream-outbox.modern-python.org/usage/relay/`

Edit 2 — line 84, dead-letter queue:
- Old: `https://faststream-outbox.readthedocs.io/en/latest/usage/dlq/`
- New: `https://faststream-outbox.modern-python.org/usage/dlq/`

Edit 3 — line 90, "How it works":
- Old: `https://faststream-outbox.readthedocs.io/en/latest/introduction/how-it-works/`
- New: `https://faststream-outbox.modern-python.org/introduction/how-it-works/`

- [ ] **Step 2: Replace the bare-domain link**

Edit 4 — line 110, top-level Documentation header:
- Old: `https://faststream-outbox.readthedocs.io`
- New: `https://faststream-outbox.modern-python.org`

(Order matters: do this AFTER the three `/en/latest/` edits. Otherwise the bare-domain string also matches inside the deeper URLs and you'd over-replace.)

- [ ] **Step 3: Verify no `readthedocs` references remain**

Run: `grep -n readthedocs README.md`

Expected: no output (exit code 1 from grep, which is fine).

- [ ] **Step 4: Verify the four new URLs are in place**

Run: `grep -nc "modern-python.org" README.md`

Expected: `4`.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: point README links at modern-python.org

Replaces readthedocs.io/en/latest/<path>/ with
modern-python.org/<path>/ for the three deep links (relay, dlq,
how-it-works) and the bare-domain Documentation header link.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Bump action pins in `.github/workflows/ci.yml`

**Files:**
- Modify: `.github/workflows/ci.yml` (lines 17, 18, 19, 49, 50)

- [ ] **Step 1: Confirm current pin lines**

Run: `grep -n "@v[0-9]" .github/workflows/ci.yml`

Expected:
```
17:      - uses: actions/checkout@v4
18:      - uses: extractions/setup-just@v2
19:      - uses: astral-sh/setup-uv@v3
49:      - uses: actions/checkout@v4
50:      - uses: astral-sh/setup-uv@v3
```

- [ ] **Step 2: Bump `actions/checkout@v4` → `@v6`**

Two occurrences (lines 17 and 49). Use a `replace_all` for the exact string `actions/checkout@v4` → `actions/checkout@v6`.

- [ ] **Step 3: Bump `extractions/setup-just@v2` → `@v4`**

One occurrence (line 18 — pytest job doesn't use just). Replace exact string `extractions/setup-just@v2` → `extractions/setup-just@v4`.

- [ ] **Step 4: Bump `astral-sh/setup-uv@v3` → `@v8.2.0`**

Two occurrences (lines 19 and 50). Use `replace_all` for `astral-sh/setup-uv@v3` → `astral-sh/setup-uv@v8.2.0`.

- [ ] **Step 5: Verify all five pins updated**

Run: `grep -n "@v[0-9]" .github/workflows/ci.yml`

Expected:
```
17:      - uses: actions/checkout@v6
18:      - uses: extractions/setup-just@v4
19:      - uses: astral-sh/setup-uv@v8.2.0
49:      - uses: actions/checkout@v6
50:      - uses: astral-sh/setup-uv@v8.2.0
```

Also verify no stale pins linger:

```bash
grep -E "@v[234]\b" .github/workflows/ci.yml
```

Expected: no output (exit 1).

- [ ] **Step 6: Verify YAML still parses**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('OK')"`

Expected: `OK`.

- [ ] **Step 7: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: bump action pins in ci.yml to drop Node.js 20

actions/checkout v4 -> v6
extractions/setup-just v2 -> v4
astral-sh/setup-uv v3 -> v8.2.0

Matches the baseline modern-di already runs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Bump action pins in `.github/workflows/publish.yml`

**Files:**
- Modify: `.github/workflows/publish.yml` (lines 12, 13, 14)

- [ ] **Step 1: Confirm current pin lines**

Run: `grep -n "@v[0-9]" .github/workflows/publish.yml`

Expected:
```
12:      - uses: actions/checkout@v4
13:      - uses: extractions/setup-just@v2
14:      - uses: astral-sh/setup-uv@v3
```

- [ ] **Step 2: Apply the three bumps**

Three single-occurrence string replacements:

- `actions/checkout@v4` → `actions/checkout@v6`
- `extractions/setup-just@v2` → `extractions/setup-just@v4`
- `astral-sh/setup-uv@v3` → `astral-sh/setup-uv@v8.2.0`

- [ ] **Step 3: Verify**

Run: `grep -n "@v[0-9]" .github/workflows/publish.yml`

Expected:
```
12:      - uses: actions/checkout@v6
13:      - uses: extractions/setup-just@v4
14:      - uses: astral-sh/setup-uv@v8.2.0
```

And: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/publish.yml')); print('OK')"` → `OK`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/publish.yml
git commit -m "ci: bump action pins in publish.yml to drop Node.js 20

Matches the pins applied to ci.yml and docs.yml in this branch.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] **Step 1: Run `just lint-ci` to catch formatting drift**

Run: `just lint-ci`

Expected: all four sub-steps pass (`eof-fixer`, `ruff format --check`, `ruff check --no-fix`, `ty check`). If `eof-fixer` flags any of the new files, run `just lint` once, then `git add` the fix into the appropriate prior commit via `git commit --amend` — or, if multiple files are touched, fold them into a single `chore: eof-fixer cleanup` commit at the end.

- [ ] **Step 2: Verify the full diff against `main`**

Run: `git log --oneline main..HEAD && git diff --stat main..HEAD`

Expected: 8 commits (Tasks 1–8). The `--stat` summary should touch exactly these paths:

```
.github/workflows/ci.yml
.github/workflows/docs.yml
.github/workflows/publish.yml
.readthedocs.yaml
Justfile
README.md
docs/CNAME
mkdocs.yml
```

(8 entries; `.readthedocs.yaml` shows as a deletion.)

- [ ] **Step 3: One final mkdocs strict build**

Run: `uvx --with-requirements docs/requirements.txt mkdocs build --strict --site-dir /tmp/mkdocs-final && ls /tmp/mkdocs-final/CNAME && grep -q 'modern-python.org' /tmp/mkdocs-final/index.html && echo OK && rm -rf /tmp/mkdocs-final`

Expected: prints `OK`. (Verifies: build is strict-clean, `CNAME` lands in the output, and the canonical URL points at the new domain.)

- [ ] **Step 4: Push the branch**

```bash
git push -u origin feat/mkdocs-github-pages
```

Expected: branch tracks `origin/feat/mkdocs-github-pages`.

- [ ] **Step 5: Open the PR**

```bash
gh pr create --title "Migrate docs hosting from Read the Docs to GitHub Pages" --body "$(cat <<'EOF'
## Summary

- Adds `.github/workflows/docs.yml` mirroring `modern-di`'s pattern: `mkdocs gh-deploy --force` on push to `main` (paths-filtered), concurrency-serialized, `contents: write`.
- Adds `just docs-deploy` target driving the deploy via `uvx --with-requirements docs/requirements.txt mkdocs gh-deploy --force`.
- Adds `docs/CNAME` and `mkdocs.yml` `site_url` for the custom domain `faststream-outbox.modern-python.org`.
- Deletes `.readthedocs.yaml`; updates the four `readthedocs.io` links in `README.md` to `modern-python.org` equivalents.
- Bumps action pins in `ci.yml` and `publish.yml` to the same baseline the new `docs.yml` uses (`checkout@v6`, `setup-just@v4`, `setup-uv@v8.2.0`).

Spec: `planning/specs/2026-06-09-mkdocs-github-pages-design.md`

## Out-of-repo steps after merge

1. DNS: CNAME `faststream-outbox.modern-python.org` → `modern-python.github.io`.
2. After the first workflow run creates `gh-pages`, set Settings → Pages → Source = `gh-pages` branch, `/` root.
3. Tick "Enforce HTTPS" once the cert provisions (~5 min).
4. Archive the `faststream-outbox` project on readthedocs.io.

## Test plan

- [ ] PR CI is green (`ci.yml` lint + pytest jobs run with bumped pins).
- [ ] After merge, the `Deploy Docs` workflow runs and creates the `gh-pages` branch.
- [ ] `https://faststream-outbox.modern-python.org/` returns the docs site once DNS + Pages settings are in place.
- [ ] All four `README.md` links resolve to working pages on the new domain.
- [ ] A subsequent push to `main` touching `docs/` re-deploys.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: prints the PR URL.

---

## Self-review

**Spec coverage:**

| Spec section | Plan task |
| --- | --- |
| 1. `docs.yml` workflow | Task 4 |
| 2. `just docs-deploy` | Task 1 |
| 3. `docs/CNAME` | Task 2 |
| 4. `mkdocs.yml` `site_url` | Task 3 |
| 5. Delete `.readthedocs.yaml` | Task 5 |
| 6. README URL updates | Task 6 |
| 7. `ci.yml` pin bumps | Task 7 |
| 8. `publish.yml` pin bumps | Task 8 |
| Operations (out of repo) | PR description (Step 5 of Final verification) |
| Sequencing / rollback | Implicit — all 8 land as one PR; Tasks 1–4 add the new path, Task 5 removes RTD config, Task 6 swaps README links, Tasks 7–8 update pins. Reverting the PR restores RTD + old pins. |

All eight spec changes have a dedicated task; out-of-repo ops are surfaced in the PR body so the merging maintainer sees them.

**Placeholder check:** No TBDs, no "implement later", every code/YAML block is complete, every command has expected output.

**Consistency check:**
- `docs-deploy` Justfile target name matches across Task 1, Task 4 (the workflow's `run: just docs-deploy` line), and the spec.
- Action pin versions match across Tasks 4, 7, 8 (`checkout@v6`, `setup-just@v4`, `setup-uv@v8.2.0`).
- The domain string `faststream-outbox.modern-python.org` matches across Tasks 2, 3, 6, and the PR body.
