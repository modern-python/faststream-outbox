---
status: draft
date: 2026-06-12
slug: docs-tutorials
supersedes: null
superseded_by: null
pr: null
outcome: null
---

# Design: Two new tutorials (Diátaxis F-min, part 2)

## Summary

Add the two tutorials that were deferred from
[`docs-tutorials-and-observability-split`](../archived/2026-06-11-docs-tutorials-and-observability-split-design.md)
(#56, shipped 2026-06-12). PR #56 landed the observability split and
nav reshape but deliberately left §1 and §2 (the two tutorials) for a
dedicated session — tutorial code must be executed end-to-end against
a clean environment with literal terminal output captured into the
page, and that warrants its own pass. This spec is that pass.

Total: 2 new pages, 1 new `docs/tutorials/` directory, 2 new entries in
`mkdocs.yml`. No existing page URLs change; no existing page content
changes; no code touched.

## Motivation

- **Zero tutorials today.** Every page in `getting-started` is
  reference-shaped (`installation.md`) or reference-with-narrative
  (`basic.md`). A newcomer who lands on `docs/index.md` and clicks
  *"Install and write the first publisher / subscriber"* arrives at the
  Basic-usage page, which is a four-step section list more than a
  story. No *"let's build something together for ten minutes,"* no one
  place where the path from zero to "a row landed through a handler"
  is one continuous narrative. A tutorial in the Diátaxis sense — warm
  voice, single concrete journey, end-state recap — is the single
  highest-impact missing piece, and was always the bigger half of the
  F-min ask.

- **The architecture has a relay payoff line that's not yet a worked
  example.** The docs talk about the foreign-broker relay
  ([`usage/relay.md`](../../docs/usage/relay.md)) but never show the
  full app: `docker compose up postgres kafka`, publish in a
  transaction, watch the row land in Kafka, kill Kafka, watch the
  retry. A tutorial that does this end-to-end is more persuasive than
  the relay reference's three-line snippet — and demonstrates the
  at-least-once contract better than any prose section can.

- **The deferral note in #56 is the controlling constraint.** From the
  archived spec's scope-reduction header: *"the spec's discipline that
  tutorial code must be executed end-to-end against a clean
  environment with literal output captured warrants a dedicated
  session."* This spec exists specifically to honor that constraint.
  See §Testing — tutorial code must be **executed** during plan
  execution; pasted output must be **literal**, not hand-edited.

## Non-goals

Deliberately *not* covered here; each is a candidate follow-on:

- **A third tutorial** — *"Test handlers with `TestOutboxBroker`"*,
  *"Schedule a delayed delivery"*, *"Wire DLQ"*. Two is the right
  count for this pass; the testing one would be high-value but is
  separate scope.
- **A dedicated `Tutorials` nav sub-grouping.** Two tutorials slot
  under "Getting started" as flat entries with `Tutorial:` prefixes.
  Revisit grouping at three.
- **Renaming nav sections to Diátaxis-canonical labels.** Same
  argument as #56: existing Overview / Getting started / Concepts /
  Guides / Reference / Operations reads more naturally and already
  maps to the four quadrants. Don't churn labels.
- **Voice review of existing Reference pages.** Tutorial voice is
  warm and step-by-step; Reference voice stays terse and precise.
  Both are correct for their quadrant.
- **A CI step that runs the tutorial code on every release.** Tracked
  separately under the migration-recipe regression-test follow-on
  (see [`operator-pages`](../archived/2026-06-11-operator-pages-design.md)'s
  out-of-scope list). The right place to add it, but not here.
- **Re-shipping the observability split.** That was the #56 PR;
  shipped.

## Design

### 1. Tutorial: Your first outbox app

New file: `docs/tutorials/first-outbox-app.md`. New `docs/tutorials/`
directory at the same level as `docs/concepts/`, `docs/operations/`,
`docs/usage/`, `docs/introduction/`.

Goal — a reader with Python and Postgres familiarity follows the page
top-to-bottom in roughly ten minutes and ends with a running process
where a single `broker.publish` results in the handler running, with
no surprises along the way.

Section outline:

```
# Tutorial: Your first outbox app

What you'll build         (2 sentences: a tiny app where publishing
                           inside a DB transaction triggers a handler)

Before you start          (Python 3.13+, Postgres, ~10 minutes)

Step 1: Install           (uv add 'faststream-outbox[asyncpg,validate]'
                           'faststream[cli]' — the CLI extra is needed
                           for `faststream run`)

Step 2: Start Postgres    (one-liner Docker)

Step 3: Declare the
        outbox table      (MetaData + make_outbox_table)

Step 4: Create the schema (metadata.create_all in a one-shot script —
                           the easiest path for a tutorial; link out to
                           operations/alembic.md for the real Alembic
                           recipe)

Step 5: Define a handler  (@broker.subscriber("orders"))

Step 6: Publish a row     (inside session.begin())

Step 7: Run it            (faststream run app:app, see the handler fire)

What you just built       (recap)

What's next               (links to: Subscriber reference, Publisher
                           reference, FastAPI integration guide,
                           Tutorial: Add a Kafka relay)
```

Voice: imperative + warm. Use *"we"* sparingly. Each step starts with
a one-sentence "what you're about to do" and ends with "you should see
X." No design rationale on the page; that's Concepts. Links to
Explanation for "why this works."

Code: one file (`app.py`) grows step by step. Each step shows the
*diff* from the previous step (or the full file if the diff would be
larger than the file). Final file is ~30 lines.

### 2. Tutorial: Add a Kafka relay

New file: `docs/tutorials/add-kafka-relay.md`.

Goal — extends Tutorial #1 with a Kafka publisher decorator. Shows the
at-least-once contract end to end by deliberately killing Kafka
mid-flight and watching the retry land.

Section outline:

```
# Tutorial: Add a Kafka relay

What you'll add          (turn the Tutorial-1 handler into a relay
                          that forwards to Kafka)

Before you start         (you finished Tutorial 1; we'll extend that
                          app)

Step 1: Add Kafka         (docker compose snippet)

Step 2: Install
        faststream[kafka] (uv add 'faststream[kafka]')

Step 3: Add the Kafka
        broker            (KafkaBroker + publisher)

Step 4: Stack the
        decorator         (@publisher_kafka @broker_outbox.subscriber)

Step 5: Run it and
        watch a row reach
        Kafka             (consume from the topic via the CLI)

Step 6: Kill Kafka and
        watch the retry   (docker compose stop kafka, publish a row,
                          see the outbox subscriber retry; bring Kafka
                          back, see the row deliver)

What you just built      (recap — at-least-once relay)

What's next              (links to: Relay reference, Subscriber retry
                          strategies, Comparison page §
                          "vs FastStream foreign-broker direct")
```

Voice: same warmth as Tutorial 1; assumes familiarity from Tutorial 1.

Code: extends the same `app.py` plus the `docker-compose.yml` from
Tutorial 1.

### 3. Nav adjustments

The nav already merged in #56 (`mkdocs.yml`) carries placeholders for
the two tutorials — they were intentionally left commented out under
"Getting started" pending this follow-on. Concretely, after this spec:

```yaml
nav:
  - Overview: index.md
  - Getting started:
      - Installation: introduction/installation.md
      - Basic usage: usage/basic.md
      - 'Tutorial: Your first outbox app': tutorials/first-outbox-app.md   # NEW
      - 'Tutorial: Add a Kafka relay': tutorials/add-kafka-relay.md         # NEW
  - Concepts: ...
  - Guides: ...
  - Reference: ...
  - Operations: ...
```

Two new entries, zero file renames, zero URL changes for existing
pages. Plan must verify the actual placeholder state in `mkdocs.yml`
when starting Task 1 — if #56 did *not* leave commented-out lines,
add fresh entries instead.

### 4. Cross-link contract

- Tutorial 1's "What's next" footer points at:
  Tutorial 2, Subscriber reference, Publisher reference, FastAPI
  integration guide.
- Tutorial 2's "What's next" footer points at:
  Relay reference, Subscriber retry strategies, Comparison page §
  *"vs FastStream foreign-broker direct"*.
- `usage/relay.md` Reference page gains a one-line "Want a worked
  end-to-end example? See [Tutorial: Add a Kafka
  relay](../tutorials/add-kafka-relay.md)" pointer at the top.
- `docs/index.md` decision-tree table — no change needed. No row maps
  to "I want a 10-minute walk-through" directly; the tutorials are
  reachable via the "Install and write the first publisher /
  subscriber" row, which now resolves to either Basic usage *or* the
  Tutorial 1 page. Re-link decision: point the row at Tutorial 1
  (warmer entry for newcomers) and leave Basic usage discoverable via
  the sidebar. Plan to confirm with reviewer in PR.

### 5. The `tutorials/` directory

New top-level `docs/tutorials/`. Lives alongside `docs/concepts/`,
`docs/operations/`, `docs/usage/`, `docs/introduction/`. Consistent
with the existing flat directory layout — no nested subdirectories.

## Operations

None — in-repo. The mkdocs deploy workflow re-runs on push to `main`
whenever `docs/**` or `mkdocs.yml` changes; this PR triggers both. The
new URLs become available immediately at:

- `https://faststream-outbox.modern-python.org/tutorials/first-outbox-app/`
- `https://faststream-outbox.modern-python.org/tutorials/add-kafka-relay/`

No existing URL changes; no redirects needed.

## Out of scope

Repeat list for grep:

- Third tutorial (testing, scheduling, DLQ)
- Tutorials nav sub-grouping
- Renaming nav sections to Diátaxis-canonical labels
- Voice review of existing Reference pages
- CI step that runs tutorial code on every release
- Re-shipping anything from #56 (observability split, nav reshape,
  instrumentation-seams Explanation page)

## Testing

Content-only; correctness is observable on the live site. Critically:

- **Tutorial code must be executed end-to-end during plan execution
  against a clean machine.** Tutorial 1: from scratch (fresh checkout
  + fresh Postgres container), every step's expected output observed.
  Tutorial 2: same, including the "kill Kafka, see retry" step. The
  plan author runs these and the *literal terminal output* lands
  inside the tutorial under "you should see X" — no hand-edited
  expected output. Tutorials that haven't been run produce
  frustration when readers try them and miss a step.
- `just docs-build` (`mkdocs build --strict`) passes clean — every
  internal cross-link from the two new pages and the touched
  `usage/relay.md` resolves.
- `just lint` passes (eof-fixer, ruff format, ruff check, ty check).
- Reviewer manual sidebar scan: `just docs-serve` and confirm the two
  new entries appear in the two new sidebar positions under "Getting
  started."
- Reviewer reads both tutorials end-to-end against a fresh checkout
  and a clean Postgres / Kafka — the most valuable review move for a
  tutorial.

## Risk

- **Tutorial voice drifts from the existing reference voice and
  introduces inconsistency across the site.** Mitigated by the
  explicit voice guidance in §1 and §2 (warm, step-by-step in
  tutorials; everything else unchanged). The "What you just built"
  recap pattern, the "Before you start" preamble, and the "What's
  next" footer are intentional voice markers — they signal *"you are
  reading a tutorial"* without requiring a Diátaxis-literate reader.
  Tutorial voice is allowed to feel different from Reference voice
  because they're serving different reader needs.

- **Tutorial code goes stale faster than reference code.** Tutorial
  code embeds version-specific install commands, Docker image tags,
  and Postgres compose snippets — all of which drift faster than the
  library's public API. Mitigated by keeping the tutorials minimal
  (no premature abstractions, no library features outside the
  tutorial's narrow path) so most updates are mechanical pin bumps.
  Follow-up: tutorials could be the next thing tested by the
  migration-recipe-style regression tests scaffolded out by the
  operator-pages spec — a CI step that runs the tutorial code
  against a real Postgres on every release. Out of scope here.

- **Tutorial #2's "kill Kafka, see retry" step is the
  most-likely-to-flake step in either tutorial.** Local environments
  differ; Kafka's failure modes are platform-sensitive (especially on
  Apple Silicon). Mitigated by recommending Confluent's `cp-kafka`
  image specifically (known to work on M1+ from prior use) and by
  treating the step as *demonstrative*: the tutorial doesn't fail if
  Kafka comes back instantly with no observable retry, because the
  at-least-once property is still preserved. Reviewer flags if the
  step's reproduction is fragile and we drop it from the tutorial in
  favor of a one-paragraph callout explaining the contract.

- **The "What's next" footers create a maintenance graph.** Adding a
  new tutorial later means revisiting the footer of every existing
  tutorial to add the cross-link. At two tutorials the cost is
  trivial. Re-evaluate if we ever add a fourth.

- **Literal-output discipline regresses under iteration.** A future
  edit that touches Tutorial 1's `app.py` could miss re-running the
  tutorial and silently desync the "you should see X" output from
  what actually prints. Mitigated by §Testing's framing (running
  end-to-end is a release gate for any tutorial change) and by
  keeping output blocks small enough that a reviewer can spot a stale
  one in code review.
