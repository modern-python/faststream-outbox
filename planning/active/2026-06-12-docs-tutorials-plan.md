---
status: draft
date: 2026-06-12
slug: docs-tutorials
spec: docs-tutorials
pr: null
---

# docs-tutorials — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the two tutorials deferred from #56 (Your first outbox
app + Add a Kafka relay), each written from literal end-to-end
execution against a clean environment, plus the two-entry nav update
and the cross-links that surface them.

**Spec:** [`planning/active/2026-06-12-docs-tutorials-design.md`](./2026-06-12-docs-tutorials-design.md)

**Branch:** `docs/tutorials`

**Commit strategy:** Per-task commits. Tasks 2 and 3 each carry a
`docker compose down -v` smoke step before writing so the captured
output reflects a true first-run experience — no leftover state from
an earlier session of the same task.

**Sequencing note:** Tasks 2 and 3 are gated on actually running the
tutorial code end-to-end against a clean Postgres (Task 2) and a
clean Postgres + Kafka (Task 3). The plan author runs each tutorial
on a real machine before committing the page. **Hand-edited output is
a defect** — if a captured line looks "ugly" (asyncpg warning, sqlalchemy
deprecation, docker compose progress noise), it stays. The point is
that the reader's first run will match the page.

---

### Task 1: Branch + plan commit + README Active entry

**Files:**
- Modify: `planning/README.md`
- Create: `planning/active/2026-06-12-docs-tutorials-design.md` (already drafted)
- Create: `planning/active/2026-06-12-docs-tutorials-plan.md` (this file)

- [ ] **Step 1: Branch off `main`**

  ```bash
  git fetch origin
  git switch -c docs/tutorials origin/main
  ```

  Expected: clean tree on `docs/tutorials` at `origin/main`'s HEAD.
  The current `chore/archive-observability-split` branch carried the
  archive-only commit from #56's archive pass; the tutorials work
  belongs on its own branch.

- [ ] **Step 2: Update `planning/README.md` Active section**

  Replace the `_None._` line under `## Active` with:

  ```markdown
  ## Active

  - **[docs-tutorials](active/2026-06-12-docs-tutorials-design.md)**
    — The two tutorials deferred from #56: *Your first outbox app*
    (10-minute walk-through) and *Add a Kafka relay* (extends the
    first tutorial with a Kafka publisher + at-least-once
    demonstration). New `docs/tutorials/` directory; two nav entries.
  ```

- [ ] **Step 3: Smoke-build**

  Run: `just docs-build`

  Expected: clean. No new pages yet; the README change is non-docs.
  This is a sanity check that the branch baseline still builds.

- [ ] **Step 4: Commit**

  ```bash
  git add planning/active/2026-06-12-docs-tutorials-design.md \
          planning/active/2026-06-12-docs-tutorials-plan.md \
          planning/README.md
  git commit -m "$(cat <<'EOF'
  docs: spec + plan for two new tutorials (F-min part 2)

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  EOF
  )"
  ```

---

### Task 2: Tutorial 1 — Your first outbox app

**Files:**
- Create: `docs/tutorials/first-outbox-app.md`
- Working dir (not committed): `/tmp/outbox-tutorial-1/`

Write the tutorial per [spec §1
](./2026-06-12-docs-tutorials-design.md#1-tutorial-your-first-outbox-app)
**after running every step end-to-end against a clean local
environment.** Capture the literal terminal output at each step. The
captured output blocks land inside the page under "you should see:"
preambles.

**Voice contract:** warm, step-by-step, sparing use of "we." Each
step opens with a one-sentence "what you're about to do" and closes
with the literal output. No design rationale on the page; that's
Concepts. No forward references to library features the tutorial
doesn't use (no DLQ, no timers, no `OutboxResponse`).

- [ ] **Step 1: Clean Postgres environment**

  Run from the repo root:

  ```bash
  docker compose -f compose.yaml down -v
  ```

  Expected: containers and volumes removed. We will *not* use the
  repo's `compose.yaml` for the tutorial run — the reader doesn't
  have it. The teardown is to ensure no port 5432 conflict against
  the tutorial container we're about to start.

- [ ] **Step 2: Fresh working directory under `/tmp`**

  ```bash
  rm -rf /tmp/outbox-tutorial-1
  mkdir /tmp/outbox-tutorial-1
  cd /tmp/outbox-tutorial-1
  ```

  This is the tutorial reader's perspective: a directory with
  nothing in it. Every command in the tutorial body is run from
  here.

- [ ] **Step 3: Walk Step 1 — install**

  The tutorial will instruct:

  ```bash
  uv add 'faststream-outbox[asyncpg,validate]'
  ```

  (or `pip install` equivalent). Run it. Capture the literal stdout.
  The PEP 668 / venv path is tutorial-reader's problem; pick the
  flow that produces the cleanest captured output (recommendation:
  `uv init && uv add ...` to start from a real project).

  If `uv add` produces a venv-creation banner or a lock-file diff
  line, that line **stays** in the captured output. The page shows
  what a first-time reader will actually see.

- [ ] **Step 4: Walk Step 2 — start Postgres**

  The tutorial will instruct:

  ```bash
  docker run --rm -d --name outbox-postgres \
      -e POSTGRES_USER=outbox -e POSTGRES_PASSWORD=outbox -e POSTGRES_DB=outbox \
      -p 5432:5432 postgres:17
  ```

  Run it. Capture the container ID stdout. Wait for `docker logs
  outbox-postgres 2>&1 | tail -1` to show `database system is ready
  to accept connections`. Capture that one log line too.

- [ ] **Step 5: Walk Steps 3 + 4 — declare the table and create the schema**

  The tutorial will provide an `app.py` containing the
  `make_outbox_table` declaration plus a one-shot
  `metadata.create_all` script (run via `python -c` or as a small
  block at the top of `app.py` gated by `if __name__ == "__main__":`).

  Decision: use a separate `create_schema.py` script over a gated
  block. Two reasons: (1) keeps `app.py` minimal so the diff at each
  later step stays small, (2) the tutorial reader runs the schema
  creation once and never again, so a separate file is the more
  honest mental model.

  Write `create_schema.py`. Run `python create_schema.py`. Capture
  the output (likely silent on success — that's fine; the page says
  "you should see no output").

  Verify with `docker exec outbox-postgres psql -U outbox -d outbox
  -c '\d outbox'`. Capture that table definition. It goes in the
  tutorial as the "you should see this table" callout under Step 4.

- [ ] **Step 6: Walk Step 5 — define the handler**

  Add the `@broker.subscriber("orders")` block to `app.py`. The
  handler body is `print(f"got order {order_id}")` — the simplest
  thing that produces visible output on Step 7.

  No command to run at this step; the handler executes during Step 7.

- [ ] **Step 7: Walk Step 6 — publish a row**

  Add an `@app.after_startup` block to `app.py` that opens a session
  and calls `await broker.publish(1, queue="orders", session=session)`.
  No command to run yet — Step 7 is what fires it.

- [ ] **Step 8: Walk Step 7 — run the app**

  Run:

  ```bash
  faststream run app:app
  ```

  Capture every line of stdout from start until the handler fires
  (`got order 1`) and the next idle line. The tutorial shows this as
  a `text` block under "you should see:" — the literal output, INFO
  lines and all.

  Ctrl-C; capture the shutdown lines. Those go in the page too —
  the reader needs to know what a clean shutdown looks like.

- [ ] **Step 9: Write `docs/tutorials/first-outbox-app.md`**

  Use the section outline from spec §1 (What you'll build → Before
  you start → Steps 1–7 → What you just built → What's next).

  Each step contains:
  - A one-sentence "what you're about to do" preamble.
  - The literal command or `app.py` diff/full-file.
  - The literal captured output under "you should see:" (a
    fenced `text` or `bash` block).

  `What's next` footer links to:
  - [Subscriber reference](../usage/subscriber.md)
  - [Publisher reference](../usage/publisher.md)
  - [FastAPI integration](../usage/fastapi.md)
  - [Tutorial: Add a Kafka relay](./add-kafka-relay.md)

- [ ] **Step 10: Smoke-build**

  Run: `just docs-build`

  Expected: clean **or** an `not_in_nav` failure (Task 4 wires the
  page in). `mkdocs build --strict` promotes that warning to an
  error. If the build fails on it, add the nav entry now (out of
  Task-4 order) rather than disabling the strict check — `--strict`
  is load-bearing and shouldn't be relaxed for a single task.

- [ ] **Step 11: Clean up the tutorial environment**

  ```bash
  docker stop outbox-postgres
  rm -rf /tmp/outbox-tutorial-1
  ```

  Tutorial 2 starts from a fresh setup; carrying state across tasks
  would defeat the "first-run experience" capture.

- [ ] **Step 12: Commit**

  ```bash
  git add docs/tutorials/first-outbox-app.md
  git commit -m "$(cat <<'EOF'
  docs: tutorial — your first outbox app

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  EOF
  )"
  ```

---

### Task 3: Tutorial 2 — Add a Kafka relay

**Files:**
- Create: `docs/tutorials/add-kafka-relay.md`
- Working dir (not committed): `/tmp/outbox-tutorial-2/`

Write the tutorial per [spec §2
](./2026-06-12-docs-tutorials-design.md#2-tutorial-add-a-kafka-relay)
extending Tutorial 1's app. Same end-to-end execution requirement.

The "kill Kafka, see retry" step (spec §2 Step 6) is the fragility
risk flagged in the spec. Use Confluent's `cp-kafka` image. If the
step's repro is unstable on your environment, STOP and reshape it as
a callout that explains the at-least-once contract without requiring
the visible retry. **The spec authorizes this fallback** — don't
push past a flaky step to "make it work for the page."

- [ ] **Step 1: Fresh working directory; re-walk Tutorial 1 to seed it**

  ```bash
  rm -rf /tmp/outbox-tutorial-2
  mkdir /tmp/outbox-tutorial-2
  cd /tmp/outbox-tutorial-2
  ```

  Now re-walk Tutorial 1 inside this directory using only what
  `docs/tutorials/first-outbox-app.md` says. End-state: working
  `app.py` + a Postgres container + a venv with `faststream-outbox`
  installed. The captured starting state for Tutorial 2 *is* a
  reader who just finished Tutorial 1, so this re-walk is the
  cheapest way to land at exactly that state. (Bonus: if any step
  diverges from the captured output during the re-walk, you caught
  a Task 2 defect — fix Tutorial 1 before continuing.)

- [ ] **Step 2: Walk Step 1 — add Kafka via docker-compose**

  Write the `docker-compose.yml` that adds a `kafka` service using
  Confluent's `cp-kafka:7.6.0` image (or current stable at execution
  time — record the exact tag you use in the captured output). The
  recommended single-broker KRaft configuration uses `cp-kafka`'s
  built-in mode; no separate ZooKeeper service.

  Run:

  ```bash
  docker compose up -d kafka
  ```

  Capture the stdout. Wait for `docker compose logs kafka 2>&1 |
  grep -m1 'Kafka Server started'`. Capture that one line.

- [ ] **Step 3: Walk Step 2 — install faststream[kafka]**

  ```bash
  uv add 'faststream[kafka]'
  ```

  Capture stdout.

- [ ] **Step 4: Walk Steps 3 + 4 — Kafka broker + stacked decorator**

  Extend `app.py`:
  - Add `KafkaBroker(...)` and a `kafka_publisher = kafka_broker.publisher("orders.kafka")`.
  - Stack `@kafka_publisher` above the existing `@broker_outbox.subscriber("orders")`.
  - Adjust the handler to `return order_id` so the publisher
    decorator forwards a payload.

  No command to run at this step; Step 5 fires it.

- [ ] **Step 5: Walk Step 5 — run and watch a row reach Kafka**

  Run `faststream run app:app` in one terminal. In a second
  terminal, run:

  ```bash
  docker compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:9092 --topic orders.kafka --from-beginning
  ```

  Capture both: the `faststream` stdout (publish + handler + Kafka
  send) and the consumer output (the `1` arriving on the topic).

- [ ] **Step 6: Walk Step 6 — kill Kafka and watch the retry**

  In the running `faststream` process, with the consumer still
  listening:

  ```bash
  docker compose stop kafka
  ```

  Publish another row (cleanest: send `SIGUSR1` to the app or
  re-run a one-shot `python -c "import asyncio; ..."` snippet that
  calls `broker.publish` — record whichever flow the tutorial uses).
  Capture the outbox subscriber's retry log lines. Then:

  ```bash
  docker compose start kafka
  ```

  Capture the eventual successful delivery line and the consumer
  receiving the row.

  **If the retry doesn't visibly fire** (Kafka returns instantly
  with a "not ready" that the producer retries internally before the
  outbox subscriber notices), drop Step 6 from the tutorial and
  replace it with a callout that says: *"Behind the scenes, if Kafka
  were unavailable, the outbox row would be retried per the subscriber's
  retry policy. See [Subscriber § Retry strategies
  ](../usage/subscriber.md#retry-strategies)."* The spec authorizes
  this fallback.

- [ ] **Step 7: Write `docs/tutorials/add-kafka-relay.md`**

  Use the section outline from spec §2. Cross-link upward to
  Tutorial 1 in the "Before you start" preamble. `What's next`
  footer links to:
  - [Relay reference](../usage/relay.md)
  - [Subscriber § Retry strategies](../usage/subscriber.md#retry-strategies)
  - [Comparison](../concepts/comparison.md) — see the section *"vs. FastStream + `KafkaBroker` / `RabbitBroker` directly"* (the auto-generated anchor isn't stable enough to hardcode; link the page and let the reader scroll)

  If Step 6 fell back to a callout, capture that in a one-line note
  at the top: *"This tutorial originally demonstrated retry by
  killing Kafka mid-flight; that step is fragile on some
  environments and is replaced by a callout."*

- [ ] **Step 8: Smoke-build**

  Run: `just docs-build`. Expected: clean (orphan warning OK).

- [ ] **Step 9: Clean up**

  ```bash
  docker compose down -v
  rm -rf /tmp/outbox-tutorial-2
  ```

- [ ] **Step 10: Commit**

  ```bash
  git add docs/tutorials/add-kafka-relay.md
  git commit -m "$(cat <<'EOF'
  docs: tutorial — add a Kafka relay

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  EOF
  )"
  ```

---

### Task 4: Nav additions

**Files:**
- Modify: `mkdocs.yml`

Add the two `Tutorial:` entries to the `Getting started` section per
[spec §3
](./2026-06-12-docs-tutorials-design.md#3-nav-adjustments). The spec
anticipated either uncommented placeholders or fresh additions; the
post-#56 `mkdocs.yml` does **not** carry placeholders, so this task
adds fresh entries.

- [ ] **Step 1: Edit `mkdocs.yml`**

  Replace the `Getting started:` block:

  ```yaml
    - Getting started:
        - Installation: introduction/installation.md
        - Basic usage: usage/basic.md
        - 'Tutorial: Your first outbox app': tutorials/first-outbox-app.md
        - 'Tutorial: Add a Kafka relay': tutorials/add-kafka-relay.md
  ```

  Order matters — Installation first, Basic usage second, then the
  two tutorials. Tutorials come after Basic usage because some
  readers will prefer the terse reference shape; the sidebar lets
  them choose.

- [ ] **Step 2: Smoke-build**

  Run: `just docs-build`

  Expected: clean, no orphan warnings now that the pages are in the
  nav.

- [ ] **Step 3: Commit**

  ```bash
  git add mkdocs.yml
  git commit -m "$(cat <<'EOF'
  docs: nav — surface the two new tutorials under Getting started

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  EOF
  )"
  ```

---

### Task 5: Cross-links — relay reference and landing decision tree

**Files:**
- Modify: `docs/usage/relay.md`
- Modify: `docs/index.md`

Two surgical edits per [spec §4
](./2026-06-12-docs-tutorials-design.md#4-cross-link-contract):

1. `usage/relay.md` gets a one-line "Want a worked end-to-end
   example?" pointer at the top.
2. `docs/index.md`'s decision-tree row for *"Install and write the
   first publisher / subscriber"* (line ~44) repoints from
   `installation.md → basic.md` to `installation.md → first-outbox-app.md`.
   Basic usage remains discoverable via the sidebar.

- [ ] **Step 1: Add the relay pointer**

  In `docs/usage/relay.md`, immediately after the H1 title (before
  the first paragraph), insert:

  ```markdown
  > Want a worked end-to-end example? See
  > [Tutorial: Add a Kafka relay](../tutorials/add-kafka-relay.md).
  ```

- [ ] **Step 2: Repoint the decision-tree row**

  In `docs/index.md`, find the line:

  ```markdown
  | Install and write the first publisher / subscriber | [Installation](introduction/installation.md) → [Basic usage](usage/basic.md) |
  ```

  Replace with:

  ```markdown
  | Install and write the first publisher / subscriber | [Installation](introduction/installation.md) → [Tutorial: Your first outbox app](tutorials/first-outbox-app.md) |
  ```

  Leave the `### Getting started` section list below the table
  unchanged — it still mentions Installation + Basic usage, which is
  correct (Basic usage is still a Getting-started page, just no
  longer the decision-tree default).

- [ ] **Step 3: Smoke-build**

  Run: `just docs-build`. Expected: clean.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/usage/relay.md docs/index.md
  git commit -m "$(cat <<'EOF'
  docs: cross-links — relay tutorial pointer + landing decision tree

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  EOF
  )"
  ```

---

### Task 6: Verify + PR handoff

**Files:** none modified; no commit produced.

- [ ] **Step 1: Full strict build**

  Run: `just docs-build`

  Expected: clean. All cross-links from the two new pages, the
  modified `relay.md`, and the modified `index.md` resolve.

- [ ] **Step 2: Lint pass**

  Run: `just lint`

  Expected: eof-fixer, ruff format, ruff check, ty check all pass.
  (Docs-only PR, but the linter touches the planning markdown too,
  so this catches trailing whitespace and missing EOF newlines.)

- [ ] **Step 3: Manual sidebar scan**

  Run: `just docs-serve`. Open the served site. Confirm:

  - Getting started shows: Installation, Basic usage, Tutorial:
    Your first outbox app, Tutorial: Add a Kafka relay.
  - Tutorial 1's "What's next" footer links resolve (Subscriber,
    Publisher, FastAPI integration, Tutorial 2).
  - Tutorial 2's "What's next" footer links resolve (Relay,
    Subscriber § Retry strategies, Comparison § "vs FastStream
    foreign-broker direct").
  - `usage/relay.md` shows the new top-of-page Tutorial 2 pointer.
  - `index.md` decision-tree row points at Tutorial 1.

- [ ] **Step 4: Re-run Tutorial 1 against a fresh checkout**

  In a temp directory, follow Tutorial 1 step-by-step using **only
  what the page tells you**. Every command's literal output should
  match what the page promised. Reviewer-grade check: if anything
  diverges, STOP and update the tutorial. This is the single
  highest-leverage verification step in the plan — a reader who
  trips on Step 4 has had a worse first impression than one who
  found no tutorial at all.

- [ ] **Step 5: Re-run Tutorial 2 against the Tutorial-1 end state**

  Same discipline. The kill-Kafka step is allowed to be a callout if
  Task 3 Step 6 fell back; the rest of the page must reproduce.

- [ ] **Step 6: Open the PR**

  Hand off to `superpowers:requesting-code-review` /
  `superpowers:finishing-a-development-branch`.

  PR title: `docs: two new tutorials — first outbox app + Kafka relay`.

  PR body should call out:
  - Source spec link.
  - Note that both tutorials were executed end-to-end against clean
    environments; literal output is captured in-page.
  - Whether Task 3 Step 6 (kill-Kafka demo) landed as the live
    demonstration or as the callout fallback.
  - Reviewer ask: re-walk both tutorials on a clean machine.

  On merge, both halves of the planning pair move to
  `planning/archived/` with `status: shipped`, `pr:`, and `outcome:`
  filled — same archive pattern as #50 / #53 / #56.
