---
status: draft
date: 2026-06-11
slug: docs-tutorials-and-observability-split
spec: docs-tutorials-and-observability-split
pr: null
---

# docs-tutorials-and-observability-split — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two tutorials under `docs/tutorials/` and split
`docs/usage/observability.md` into a trimmed Reference + a new
How-to (`usage/setup-prometheus-opentelemetry.md`) + a new Explanation
(`concepts/instrumentation-seams.md`), with one nav reshape commit
that surfaces the four new entries.

**Spec:** [`planning/active/2026-06-11-docs-tutorials-and-observability-split-design.md`](./2026-06-11-docs-tutorials-and-observability-split-design.md)

**Branch:** `docs/tutorials-and-observability-split`

**Commit strategy:** Per-task commits. Tasks 2 (Tutorial 1) and 3
(Tutorial 2) include the literal terminal output capture step — the
plan author runs each tutorial end-to-end against a clean local
environment before committing.

---

### Task 1: Branch + commit spec + plan + README Active entry

**Files:**
- Create: `planning/active/2026-06-11-docs-tutorials-and-observability-split-design.md` (already drafted)
- Create: `planning/active/2026-06-11-docs-tutorials-and-observability-split-plan.md` (this file)
- Modify: `planning/README.md`

- [ ] **Step 1: Confirm branch + uncommitted artifacts**

  Run: `git branch --show-current && ls planning/active/`
  Expected: branch `docs/tutorials-and-observability-split`; two
  drafted files under `planning/active/`.

- [ ] **Step 2: Update `planning/README.md` Active section**

  Replace the `_None._` line:

  ```markdown
  ## Active

  - **[docs-tutorials-and-observability-split](active/2026-06-11-docs-tutorials-and-observability-split-design.md)**
    — Two new tutorials under `docs/tutorials/` plus a three-way
    split of `docs/usage/observability.md` into Reference + How-to +
    Explanation. F-min from the docs-landing-and-comparison
    follow-ons.
  ```

- [ ] **Step 3: Commit**

  ```bash
  git add planning/active/2026-06-11-docs-tutorials-and-observability-split-design.md \
          planning/active/2026-06-11-docs-tutorials-and-observability-split-plan.md \
          planning/README.md
  git commit -m "docs: spec + plan for F-min (tutorials + observability split)

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Tutorial 1 — Your first outbox app — DEFERRED

> **Deferred to follow-on PR (2026-06-12).** Spec §"Scope reduction"
> note explains: tutorial execution discipline (clean env, literal
> captured output) warrants a dedicated session. Structural work
> (T4–T7) shipped without this task.

**Files:**
- Create: `docs/tutorials/first-outbox-app.md`

Write the tutorial per [spec §1
](./2026-06-11-docs-tutorials-and-observability-split-design.md#1-tutorial-your-first-outbox-app)
**after running every step end-to-end against a clean local
environment.** Capture the literal terminal output at each step.

**Setup for execution:**

- [ ] **Step 1: Clean Postgres environment**

  Run: `docker compose down -v 2>&1; docker compose up -d postgres`
  Expected: container fresh; no leftover `outbox` table from prior
  sessions.

- [ ] **Step 2: Fresh working directory under `/tmp`**

  Create `/tmp/outbox-tutorial-1/` and `cd` into it. This is the
  tutorial reader's perspective: a directory with nothing in it.

- [ ] **Step 3: Walk each tutorial step**

  Follow the section outline in spec §1 (Install → Start Postgres →
  Declare → Schema → Handler → Publish → Run). For each step:

  - Execute the literal command the tutorial will tell the reader to
    run.
  - Capture the literal output. **Do not edit it.**
  - If a step's command fails or produces output the spec didn't
    anticipate, STOP and update the spec before re-running (the
    tutorial must reflect reality).

- [ ] **Step 4: Write `docs/tutorials/first-outbox-app.md`**

  Use the section outline in spec §1. Each step contains:

  - A one-sentence "what you're about to do" preamble.
  - The literal command or code block.
  - The literal captured output under "you should see:" (or
    equivalent phrasing). Block-quote or `output` code block.

  Voice: warm, step-by-step, "we." Use `_What's next_` footer
  linking to the Subscriber reference, Publisher reference, FastAPI
  integration guide, and Tutorial 2.

- [ ] **Step 5: Smoke-build**

  Run: `just docs-build`
  Expected: clean. The page is orphaned-not-in-nav (Task 7 wires it
  in); the warning is acceptable.

- [ ] **Step 6: Commit**

  ```bash
  git add docs/tutorials/first-outbox-app.md
  git commit -m "docs: tutorial — your first outbox app

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Tutorial 2 — Add a Kafka relay — DEFERRED

> **Deferred to follow-on PR (2026-06-12).** Same rationale as Task 2.

**Files:**
- Create: `docs/tutorials/add-kafka-relay.md`

Write the tutorial per [spec §2
](./2026-06-11-docs-tutorials-and-observability-split-design.md#2-tutorial-add-a-kafka-relay)
extending Tutorial 1's app. Same end-to-end execution requirement.

The "kill Kafka, see retry" step (spec §2 Step 6) is the
fragility risk flagged in the spec. Use Confluent's `cp-kafka`
image. If the step's repro is unstable on your environment, STOP
and reshape it as a callout that explains the at-least-once
contract without requiring the visible retry.

- [ ] **Step 1: Extend the tutorial-1 environment**

  In `/tmp/outbox-tutorial-1/`, add Kafka to `docker-compose.yml`.
  Confluent's `cp-kafka` image is the recommended choice for
  cross-platform compatibility (especially Apple Silicon).

- [ ] **Step 2: Walk each tutorial step**

  Same discipline as Task 2 — execute, capture, paste. The kill-
  Kafka step:

  ```
  docker compose stop kafka
  <publish a row>
  <observe retry logs from the outbox subscriber>
  docker compose start kafka
  <observe successful delivery>
  ```

  If the retry log frequency / shape differs from what the spec
  predicted, update the spec **before** writing the tutorial page.

- [ ] **Step 3: Write `docs/tutorials/add-kafka-relay.md`**

  Use the section outline in spec §2. Cross-link to Tutorial 1's
  "What's next" in a "Before you start" preamble. Footer links to
  Relay reference, Subscriber § Retry strategies, and Comparison §
  "vs FastStream foreign-broker direct".

- [ ] **Step 4: Smoke-build**

  Run: `just docs-build`
  Expected: clean.

- [ ] **Step 5: Commit**

  ```bash
  git add docs/tutorials/add-kafka-relay.md
  git commit -m "docs: tutorial — add a Kafka relay

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: New Explanation — `concepts/instrumentation-seams.md`

**Files:**
- Create: `docs/concepts/instrumentation-seams.md`

Write the Explanation per [spec §3c
](./2026-06-11-docs-tutorials-and-observability-split-design.md#3c-conceptsinstrumentation-seamsmd-new-explanation).
Extract the relevant content from the current `usage/observability.md`
§ "Layering: middleware seam vs. recorder seam" — same layering
table, same four "events the other seam physically cannot observe"
points, expanded narrative.

Voice: discursive, explanatory. Aimed at "why are there two seams"
readers, not "how do I wire this" readers.

- [ ] **Step 1: Read current `usage/observability.md` § Layering**

  Read the existing § "Layering: middleware seam vs. recorder seam"
  for the table + the four bullet points to extract.

- [ ] **Step 2: Write `docs/concepts/instrumentation-seams.md`**

  Per spec §3c outline (tension → middleware-only → recorder-only →
  layering table → operator implication).

- [ ] **Step 3: Smoke-build**

  Run: `just docs-build`
  Expected: clean.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/concepts/instrumentation-seams.md
  git commit -m "docs: concept page — instrumentation seams (recorder vs middleware)

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 5: New How-to — `usage/setup-prometheus-opentelemetry.md`

**Files:**
- Create: `docs/usage/setup-prometheus-opentelemetry.md`

Write the How-to per [spec §3b
](./2026-06-11-docs-tutorials-and-observability-split-design.md#3b-usagesetup-prometheus-opentelemetrymd-new-how-to).
Direct port of current `observability.md`'s adapter setup sections
(Prometheus adapter, OpenTelemetry adapter, Native middleware,
"both seams together") with cross-section references retargeted.

- [ ] **Step 1: Lift the relevant sections**

  From current `usage/observability.md`:
  - § "Prometheus adapter" (full code block + Consume vs publish
    label set + PromQL queries that show wiring, not playbook)
  - § "OpenTelemetry adapter" (full code block)
  - § "Native middleware (spans + bus parity)" (full code block)

  Lift verbatim into the new page; clean up the cross-section
  references that no longer resolve.

- [ ] **Step 2: Add a short intro**

  One paragraph: "You've decided to wire metrics. This page is the
  recipe. For the why, see [Concepts § Instrumentation seams](
  ../concepts/instrumentation-seams.md); for the event catalog and
  PromQL playbook, see [Reference § Observability](
  ./observability.md)."

- [ ] **Step 3: Smoke-build**

  Run: `just docs-build`
  Expected: clean.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/usage/setup-prometheus-opentelemetry.md
  git commit -m "docs: how-to — setup Prometheus and OpenTelemetry

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 6: Trim `usage/observability.md` to Reference shape

**Files:**
- Modify: `docs/usage/observability.md`

Strip everything that moved to Tasks 4 (Layering) and 5 (adapter
setups). Keep per [spec §3a
](./2026-06-11-docs-tutorials-and-observability-split-design.md#3a-usageobservabilitymd-kept-trimmed-to-reference):

- § "The recorder seam" — the callable signature, the event catalog,
  the "must not block" note.
- The PromQL playbook table (the *operator queries*, distinct from
  the setup-wiring PromQL in Task 5).
- § "Test broker note".

Add a top-of-page see-also pair pointing at the new How-to and
Explanation:

```markdown
*Setting it up: [Setup Prometheus and OpenTelemetry](
./setup-prometheus-opentelemetry.md). Why two seams: [Concepts §
Instrumentation seams](../concepts/instrumentation-seams.md).*
```

- [ ] **Step 1: Delete the moved sections**

  Per the spec §3a "Removed from current page (moved)" list:
  - "Prometheus adapter" (entire section) → moved to Task 5
  - "OpenTelemetry adapter" (entire section) → moved to Task 5
  - "Native middleware (spans + bus parity)" → moved to Task 5
  - "Layering: middleware seam vs. recorder seam" + table → moved to Task 4

- [ ] **Step 2: Add the top-of-page see-also pair**

- [ ] **Step 3: Smoke-build**

  Run: `just docs-build`
  Expected: clean. Inbound deep links to the page's surviving anchors
  (`#the-recorder-seam`, `#test-broker-note`) still resolve;
  anchors that moved (`#prometheus-adapter`, etc.) are now dead
  inside this page but reachable through the see-also at the top.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/usage/observability.md
  git commit -m "docs: trim observability.md to Reference shape

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 7: Nav reshape

**Files:**
- Modify: `mkdocs.yml`

Add the four new entries to the nav per [spec §4
](./2026-06-11-docs-tutorials-and-observability-split-design.md#4-nav-adjustments).
Two under Getting started, one under Concepts, one under Guides.

- [ ] **Step 1: Edit `mkdocs.yml`**

  Replace the existing `nav:` block per the spec §4 sample.

- [ ] **Step 2: Smoke-build**

  Run: `just docs-build`
  Expected: clean. Sidebar shows the four new entries.

- [ ] **Step 3: Commit**

  ```bash
  git add mkdocs.yml
  git commit -m "docs: nav reshape — surface tutorials, setup how-to, instrumentation explanation

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 8: Verify

**Files:** none modified; no commit produced.

- [ ] **Step 1: Full strict build**

  Run: `just docs-build`
  Expected: clean. All cross-links from the four new pages and the
  trimmed `observability.md` resolve.

- [ ] **Step 2: Lint pass**

  Run: `just lint`
  Expected: eof-fixer, ruff format, ruff check, ty check all pass.

- [ ] **Step 3: Manual sidebar scan**

  Run: `just docs-serve`
  Open the served site. Confirm:

  - Getting started shows: Installation, Basic usage, Tutorial 1,
    Tutorial 2.
  - Concepts shows: How it works, Comparison, Instrumentation seams.
  - Guides shows: FastAPI integration, Relay, Timers, Testing,
    Schema validation, Setup Prometheus and OpenTelemetry.
  - Reference's Observability page is now ~150 lines (trimmed
    cleanly).

- [ ] **Step 4: Re-run Tutorial 1 against a fresh checkout**

  In a temp directory, follow Tutorial 1 step-by-step using only
  what the page tells you. Every command's output should match
  what the page promised. If anything diverges, STOP and update
  the tutorial.

- [ ] **Step 5: Open the PR**

  Stop. Hand off to `superpowers:requesting-code-review` /
  `superpowers:finishing-a-development-branch`.

  On merge, both halves of the pair move to `planning/archived/`
  with `status: shipped`, `pr:`, and `outcome:` filled — same
  archive pattern PRs #52 and #54 dogfooded.
