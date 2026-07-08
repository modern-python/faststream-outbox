---
summary: Docs hosting moves from Read the Docs to GitHub Pages on faststream-outbox.modern-python.org.
---

# Design: Migrate docs hosting from Read the Docs to GitHub Pages

## Summary

Move `faststream-outbox` docs hosting off Read the Docs and onto GitHub
Pages, mirroring the migration the sister `modern-di` project completed in
commit `61b9377`. Docs build via a new dedicated GH Actions workflow that
runs `mkdocs gh-deploy` on push to `main`, served at the custom domain
`faststream-outbox.modern-python.org`. Pin bumps for the two existing
workflows (`ci.yml`, `publish.yml`) come along for the ride so all three
workflows share the same action versions on landing.

No runtime code, test code, or public API touched.

## Motivation

- **Operational simplicity.** Read the Docs requires a separate account
  with its own settings, webhooks, and quirks (Python 3.10 pin in
  `.readthedocs.yaml`, separate build infrastructure). GH Pages keeps the
  docs pipeline in the same repo as code review.
- **Symmetry with `modern-di`.** The two repos already share Justfile
  layout, lint pipeline, mkdocs theme, and PyPI publish flow. Sharing
  docs hosting closes the last operational gap.
- **Action-version drift.** `ci.yml` and `publish.yml` pin `checkout@v4`,
  `setup-just@v2`, `setup-uv@v3`. `modern-di` already bumped to `@v6`,
  `@v4`, `@v8.2.0` (per its commit `4f1945c` — "drop Node.js 20"). Doing
  the bump alongside the docs workflow keeps all three workflows on the
  same baseline; landing it later would require a one-line follow-up PR.

## Design

### 1. New workflow `.github/workflows/docs.yml`

Near-verbatim copy of `modern-di`'s `docs.yml`:

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

Load-bearing pieces:

- **Paths filter.** Workflow fires only when docs sources, mkdocs config,
  or the workflow itself change. CI runs that only touch code don't
  trigger an unnecessary deploy.
- **`workflow_dispatch`.** Lets a maintainer force a redeploy from the
  Actions UI if the live site falls out of sync (e.g. after a manual
  `gh-pages` branch touch).
- **`concurrency: docs-deploy`, `cancel-in-progress: true`.** Serializes
  deploys. A rapid sequence of merges to `main` doesn't race
  force-pushes against the `gh-pages` branch.
- **`permissions: contents: write`.** Default `GITHUB_TOKEN` scope is
  read-only; `mkdocs gh-deploy` needs write to push the `gh-pages`
  branch.
- **`fetch-depth: 0`.** `mkdocs gh-deploy` reads git history to construct
  the deploy commit; a shallow checkout breaks the push.

### 2. Add `just docs-deploy` target

Append to `Justfile`:

```just
# Force-pushes built site to gh-pages; CI runs this on push to main.
# Manual invocation from a stale checkout will roll the live site back.
docs-deploy:
    uvx --with-requirements docs/requirements.txt mkdocs gh-deploy --force
```

Notes:

- `uvx --with-requirements docs/requirements.txt` runs mkdocs in an
  ephemeral environment derived from `docs/requirements.txt`
  (`mkdocs`, `mkdocs-material`). The project's `.venv` (Docker-managed)
  is not involved.
- `--force` matches `modern-di`. `gh-deploy` builds the site, commits it
  to a synthetic `gh-pages` branch, and force-pushes. The comment in
  the target body documents the "stale checkout rolls live back" gotcha
  so a contributor reading the file sees it before invoking locally.

### 3. New file `docs/CNAME`

One line:

```
faststream-outbox.modern-python.org
```

mkdocs material copies any file under `docs/` into the build output, so
the `CNAME` survives every `gh-deploy --force` push. Without this file,
the force-push would wipe the custom domain on every deploy and GH Pages
would revert to the project-page URL.

### 4. Update `mkdocs.yml`

Add a `site_url:` line near the top, alongside `site_name:` / `repo_url:`:

```yaml
site_url: https://faststream-outbox.modern-python.org/
```

Used by mkdocs for:

- Canonical link tags in every HTML page (SEO).
- The generated `sitemap.xml`.
- Material theme social cards (if enabled later).

Currently absent from `mkdocs.yml` — generated pages have no canonical
URL. This is independently worth doing; bundling it with the migration
saves a follow-up.

### 5. Delete `.readthedocs.yaml`

Removes the RTD build configuration. After this lands and the new URL is
live, the RTD project at `faststream-outbox.readthedocs.io` should be
archived or deleted in the RTD UI (out-of-repo step; see
"Operations" below).

### 6. Update `README.md` URLs

Four RTD links replaced with their `modern-python.org` equivalents:

- Three deep links of the form
  `https://faststream-outbox.readthedocs.io/en/latest/<path>/` →
  `https://faststream-outbox.modern-python.org/<path>/`. The `/en/latest/`
  segment is RTD's version/locale routing; GH Pages doesn't use it.
  Targets: relay tutorial, dead-letter queue, "How it works".
- One bare-domain link
  `https://faststream-outbox.readthedocs.io` →
  `https://faststream-outbox.modern-python.org` (the top-level
  `## 📚 Documentation` header link).

### 7. Bump action pins in `.github/workflows/ci.yml`

Three pin upgrades, no structural changes:

| Old | New | Occurrences |
| --- | --- | --- |
| `actions/checkout@v4` | `actions/checkout@v6` | 2 (one per job) |
| `extractions/setup-just@v2` | `extractions/setup-just@v4` | 1 (lint job only) |
| `astral-sh/setup-uv@v3` | `astral-sh/setup-uv@v8.2.0` | 2 (one per job) |

`enable-cache: true` and `cache-dependency-glob: "**/pyproject.toml"`
both remain valid in `setup-uv@v8` — no `with:` block changes.

Explicitly **not** in scope: restructuring into a reusable
`_checks.yml` workflow (modern-di's pattern). That's a separate refactor.

### 8. Bump action pins in `.github/workflows/publish.yml`

Same three upgrades, one occurrence each (single job).

## Operations (out of repo)

These steps cannot live in the spec/commit; they need a maintainer with
admin access:

1. **DNS.** Add a CNAME record:
   - Host: `faststream-outbox`
   - Target: `modern-python.github.io`
   - TTL: 1 hour (GitHub validates within minutes; short TTL is harmless).
2. **Trigger first build.** Either merge the PR (push to `main` runs the
   workflow) or run `workflow_dispatch` from the Actions UI. The first
   run creates the `gh-pages` branch.
3. **GH Pages source.** After the `gh-pages` branch exists:
   Settings → Pages → Source = "Deploy from a branch", branch
   `gh-pages`, folder `/` (root). The custom-domain field
   auto-populates from `docs/CNAME`.
4. **HTTPS.** Wait ~5 minutes for GitHub to provision the Let's Encrypt
   cert, then tick "Enforce HTTPS".
5. **Verify.** Hit
   `https://faststream-outbox.modern-python.org/` and confirm the
   relay/DLQ/how-it-works pages load.
6. **RTD teardown.** In the Read the Docs UI, archive or delete the
   `faststream-outbox` project. The old `readthedocs.io` URL will
   start returning 404; README links no longer point there, so user
   breakage is bounded to external links / search-engine results
   (which migrate over time).

## Sequencing and rollback

The workflow can land and run before DNS resolves. GH Pages serves at
`modern-python.github.io/faststream-outbox/` in the interim (with a
"custom domain not configured" notice in the Pages settings, harmless).
RTD continues to serve the old URL until step 6 above, so the docs are
never simultaneously down.

Rollback path if something breaks mid-migration:

- Revert the PR. RTD is still building (`.readthedocs.yaml` removal is
  part of the same PR), so reverting re-enables the RTD pipeline. The
  README points back at `readthedocs.io` after revert.
- The `gh-pages` branch can be left in place; deleting it requires no
  follow-up. GH Pages settings can be reset to "Source = None".

## Out of scope (deliberate)

- **PR-time docs build check.** `modern-di` doesn't have one and the
  symmetry is the whole point. Broken mkdocs config still fails the
  deploy workflow on `main` — visible quickly.
- **Reusable `_checks.yml` workflow split.** Modern-di's structural
  pattern; worth doing separately, not load-bearing for the docs move.
- **Theme / plugin additions.** No mkdocs plugins added (no
  `mike` for versioning, no social cards, no privacy plugin). The
  current setup is intentionally minimal; expansion is a separate
  conversation.
- **Old-URL redirects.** GH Pages can't gracefully redirect from
  `readthedocs.io`; RTD's URL behavior after archival is on RTD's side.
  Accepted as a one-time bookmark/search-engine adjustment cost.

## Testing

The spec changes are configuration; correctness is observable on the
live site:

- The first deploy workflow run completes green and creates the
  `gh-pages` branch (visible in Actions UI + branch list).
- After DNS resolves and Pages config flips, the live URL returns the
  expected docs (nav identical to current RTD site, theme intact, search
  works).
- A second push to `main` that touches a doc page (or
  `workflow_dispatch`) re-runs the workflow and updates the live site.

No new pytest / lint hooks are added. The existing `just lint-ci` run on
PRs already validates YAML formatting via ruff's `EOF` fixer and
formatters.

## Risk

- **DNS misconfiguration** → custom domain doesn't resolve. Mitigated
  by sequencing: workflow lands first and serves at the GH project URL;
  DNS is fixed before README links are pointed at the new domain (or
  the PR is split if needed).
- **Action pin upgrade regressions** → CI breaks for unrelated reasons.
  Low risk: modern-di runs the same pins green; if `setup-uv@v8` does
  drift, the fix is a one-line pin and rollback is trivial.
- **`docs-deploy` force-push from a stale clone** → site rolls back.
  Mitigated by the comment on the `just docs-deploy` target plus the
  fact that the canonical deploy is from CI; manual invocation is
  documented as a debugging escape hatch, not a workflow.
