---
status: shipped
date: 2026-06-10
slug: docs-landing-and-comparison
spec: docs-landing-and-comparison
pr: "50"
---

# docs-landing-and-comparison — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `docs/index.md` as a real landing page, reshape
`mkdocs.yml` nav into Getting-started / Concepts / Guides / Reference, and
add a new Comparison page under a new `docs/concepts/` directory.

**Spec:** [`planning/active/2026-06-10-docs-landing-and-comparison-design.md`](./2026-06-10-docs-landing-and-comparison-design.md)

**Branch:** `docs/landing-and-comparison`

**Commit strategy:** Per-task commits. Task 4 is verification-only and
produces no commit.

---

### Task 1: Branch + create the Comparison page + cross-link callouts

**Files:**
- Create: `docs/concepts/comparison.md`
- Modify: `docs/introduction/how-it-works.md`
- Modify: `docs/usage/relay.md`

Creates the new page that the rewritten landing (Task 3) and the
cross-link callouts will both reference. Doing the callouts in the same
commit keeps "the link target lands together with its links."

- [ ] **Step 1: Create the feature branch from `main`**

  Run: `git switch -c docs/landing-and-comparison`
  Expected: `Switched to a new branch 'docs/landing-and-comparison'`.

- [ ] **Step 2: Create `docs/concepts/comparison.md`**

  Create the directory `docs/concepts/` and inside it write
  `comparison.md` with the six sections defined in [spec §3
  ](./2026-06-10-docs-landing-and-comparison-design.md#3-new-file-docsconceptscomparisonmd):

  1. `faststream-outbox` vs writing your own
  2. vs CDC (Debezium, logical replication)
  3. vs Kafka transactions (or Rabbit publisher confirms)
  4. vs plain PG-NOTIFY
  5. vs Celery + DB result backend
  6. vs FastStream + `KafkaBroker` / `RabbitBroker` directly

  Each section ends with a one-line **TL;DR** verdict. Section 2 sources
  its CDC analysis from the existing memory entry
  `cdc_wal_rejected.md` (2026-05-07); reflect that the reassessment was
  done on that date.

  Cross-link inline into existing reference pages where natural —
  e.g. the "vs writing your own" section can name the
  `subscriber`/`publisher`/`dlq` pages whose mechanisms the user would
  re-implement.

- [ ] **Step 3: Add a one-line callout in `docs/introduction/how-it-works.md`**

  In the `## The transactional outbox pattern` section, add at the end
  of the final paragraph:

  > See [Comparison](../concepts/comparison.md) for when CDC or Kafka
  > transactions are the better fit.

- [ ] **Step 4: Add a one-line callout in `docs/usage/relay.md`**

  At the end of the intro paragraph (before the first `## Why an outbox
  relay`), add:

  > If you don't have a database write to atomically commit alongside,
  > use the foreign broker directly — see
  > [Comparison](../concepts/comparison.md).

- [ ] **Step 5: Smoke-build the docs locally**

  Run: `uvx --with-requirements docs/requirements.txt mkdocs build --strict`
  Expected: build completes, no warnings. The new page does not yet
  appear in the sidebar (Task 2 wires it in), but `--strict` will catch
  any broken cross-link from the two callouts.

- [ ] **Step 6: Commit**

  ```bash
  git add docs/concepts/comparison.md docs/introduction/how-it-works.md docs/usage/relay.md
  git commit -m "docs: add comparison page and cross-link callouts

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Reshape `mkdocs.yml` nav

**Files:**
- Modify: `mkdocs.yml`

Replace the two-section nav (`Introduction` / `Usage`) with the
four-section structure in [spec §2
](./2026-06-10-docs-landing-and-comparison-design.md#2-reshape-mkdocsyml-nav).
No file paths change; only the `nav:` block.

- [ ] **Step 1: Edit `mkdocs.yml`**

  Replace the existing `nav:` block (currently `Overview` + `Introduction`
  + `Usage`) with:

  ```yaml
  nav:
    - Overview: index.md
    - Getting started:
        - Installation: introduction/installation.md
        - Basic usage: usage/basic.md
    - Concepts:
        - How it works: introduction/how-it-works.md
        - Comparison: concepts/comparison.md
    - Guides:
        - FastAPI integration: usage/fastapi.md
        - Relay to Kafka / RabbitMQ / NATS: usage/relay.md
        - Timers: usage/timers.md
        - Testing: usage/testing.md
        - Schema validation: usage/schema-validation.md
    - Reference:
        - Subscriber: usage/subscriber.md
        - Publisher: usage/publisher.md
        - Router: usage/router.md
        - Dead-letter queue: usage/dlq.md
        - Observability: usage/observability.md
  ```

  Load-bearing details (per spec): "Relay to Kafka / RabbitMQ / NATS" is
  the nav *label* only — the file stays at `usage/relay.md` and its H1
  is unchanged. Don't touch `mkdocs.yml` outside the `nav:` block.

- [ ] **Step 2: Smoke-build and visually scan**

  Run: `uvx --with-requirements docs/requirements.txt mkdocs build --strict`
  Expected: build clean. Open `site/index.html` (or run `mkdocs serve`
  briefly) and confirm the sidebar shows four sections in the expected
  order, all eleven existing pages plus the new Comparison page are
  present, none dropped.

- [ ] **Step 3: Commit**

  ```bash
  git add mkdocs.yml
  git commit -m "docs: reshape nav into Getting started / Concepts / Guides / Reference

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Rewrite `docs/index.md` as a landing page

**Files:**
- Modify: `docs/index.md`

Replace today's 22-line TOC with the four-block landing per [spec §1
](./2026-06-10-docs-landing-and-comparison-design.md#1-rewrite-docsindexmd).
The new page is the front door — every other doc is reachable from one
of the four blocks.

- [ ] **Step 1: Replace `docs/index.md` contents**

  Structure:

  - **Block A** — value-prop paragraph (lightly tightened version of
    today's first paragraph).
  - **Block B** — "Use it when / Reach for something else when" two
    short bulleted lists. Concrete, binary, no hedging. See spec §1
    Block B for example wording.
  - **Block C** — decision-tree table mapping user intent → starting
    page. See spec §1 Block C for the table.
  - **Block D** — documentation map grouped by the four nav sections
    (Getting started / Concepts / Guides / Reference). One line per
    page, terse description after the link.

  All internal links use the relative path from `docs/index.md`:
  `introduction/installation.md`, `concepts/comparison.md`,
  `usage/fastapi.md`, etc.

- [ ] **Step 2: Smoke-build**

  Run: `uvx --with-requirements docs/requirements.txt mkdocs build --strict`
  Expected: clean. Block C and Block D each contain ~5–10 internal
  links; `--strict` catches any broken target.

- [ ] **Step 3: Commit**

  ```bash
  git add docs/index.md
  git commit -m "docs: rewrite landing page with value prop, decision tree, doc map

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: Verify

**Files:** none modified; no commit produced.

Final pass before opening the PR.

- [ ] **Step 1: Full strict build**

  Run: `uvx --with-requirements docs/requirements.txt mkdocs build --strict`
  Expected: clean.

- [ ] **Step 2: Lint pass**

  Run: `just lint`
  Expected: `eof-fixer`, `ruff format`, `ruff check`, `ty check` all
  pass. Markdown EOF + YAML formatting on `mkdocs.yml` are the only
  things touched in this PR.

- [ ] **Step 3: Manual sidebar scan**

  Run: `uvx --with-requirements docs/requirements.txt mkdocs serve`
  Open the served site and confirm:

  - Sidebar shows four sections in order: Getting started, Concepts,
    Guides, Reference.
  - Every page from the pre-change nav is still reachable.
  - The new Comparison page appears under Concepts and renders cleanly.
  - The decision-tree table on the landing page routes correctly into
    `usage/fastapi.md`, `usage/relay.md`, `introduction/how-it-works.md`,
    `concepts/comparison.md`, `introduction/installation.md`, and
    `usage/basic.md`.
  - The "Relay to a foreign broker" H1 inside `usage/relay.md` is
    unchanged (spec invariant — only the nav label changed).

- [ ] **Step 4: Open the PR**

  Stop. Hand off to `superpowers:requesting-code-review` /
  `superpowers:finishing-a-development-branch` per the standard
  workflow. The convention in [`planning/README.md`](../README.md):
  on merge, both this plan and its paired spec move to
  `planning/archived/` and get `status: shipped`, `pr:`, and
  `outcome:` filled.
