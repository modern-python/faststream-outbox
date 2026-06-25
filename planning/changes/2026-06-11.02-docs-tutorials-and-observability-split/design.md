---
summary: Three-way split of usage/observability.md into Reference + How-to + Explanation; tutorials deferred to #58.
---

# Design: Add two tutorials and split observability.md

> **Scope reduction (2026-06-12).** The implementing PR shipped only
> the observability split (§3 plus the supporting nav reshape and
> cross-links). The two tutorials (§1, §2) are deferred to a
> follow-on spec — the spec's discipline that tutorial code must be
> executed end-to-end against a clean environment with literal output
> captured warrants a dedicated session. The structural pieces stand
> on their own and were unblocked first. See the plan's Tasks 2 and 3
> deferral notes.

## Summary

Pull-the-piece-that-matters F-min from the
[`docs-landing-and-comparison`](../archived/2026-06-10-docs-landing-and-comparison-design.md)
follow-ons. Two changes that close the biggest Diátaxis gap on the
existing docs without restructuring everything:

1. **Two new tutorials** — the only category of doc the site currently
   has zero of:
   - *Tutorial: Your first outbox app* — 10-minute walk-through from
     `pip install` to a row landing through a handler.
   - *Tutorial: Add a Kafka relay* — extends the first tutorial with a
     foreign-broker relay; doubles as the worked end-to-end example
     the **C** follow-on was supposed to land.
2. **Split `usage/observability.md`** (today's longest page, 327
   lines, mixing three Diátaxis quadrants) into three single-purpose
   pages: a Reference (kept at the same URL), a How-to (Prometheus +
   OTel setup), and an Explanation (the recorder-vs-middleware
   layering rationale).

Total: 4 new pages, 1 trimmed page. Nav grows from 18 pages to 22.
No existing page URL changes; the only deletions are intra-page
content moves.

## Motivation

- **Zero tutorials today.** Every page in `getting-started` is
  reference-shaped (`installation.md`) or reference-with-narrative
  (`basic.md`). A newcomer who lands on `docs/index.md` and clicks
  "Install and write the first publisher / subscriber" arrives at
  the Basic-usage page, which is a four-step section list more than
  a story. No "let's build something together for ten minutes," no
  one place where the path from zero to "a row landed through a
  handler" is one continuous narrative. A tutorial in the Diátaxis
  sense — warm voice, single concrete journey, end-state recap — is
  the single highest-impact missing piece.

- **The architecture has a relay payoff line that's not yet a worked
  example.** The docs talk about the foreign-broker relay (
  [`usage/relay.md`](../../docs/usage/relay.md)) but never show the
  full app: `docker compose up postgres kafka`, publish in a
  transaction, watch the row land in Kafka, kill Kafka, watch the
  retry. A tutorial that does this end-to-end is more persuasive
  than the relay reference's three-line snippet.

- **`observability.md` does too much.** At 327 lines it covers:
  the recorder seam API (Reference), the Prometheus adapter
  setup (How-to), the OTel adapter setup (How-to), the native
  middleware (Reference + How-to), the layering of the two seams
  (Explanation), the test-broker behavior (Reference), plus a
  PromQL playbook (Reference). A reader who lands there with one
  intent has to scan past two others to find their answer.
  Splitting along Diátaxis lines into three pages — each with one
  shape — makes each page self-contained.

- **F-min, not F-full.** The full Diátaxis rewrite (`docs-landing-
  and-comparison` non-goal F) is deferred for the reasons argued in
  that spec: existing voice is consistent, current nav already
  gestures at Diátaxis, and splitting decomposes into more pages
  than the audience needs. F-min lands the two highest-impact
  pieces (Tutorials gap + the worst mixed page) without committing
  to the full restructure.

## Non-goals

Deliberately *not* covered here; each is a candidate follow-on:

- **A third tutorial** ("Test handlers with `TestOutboxBroker`",
  "Schedule a delayed delivery", "Wire DLQ"). Two is enough for
  this pass; the testing one in particular would be high-value but
  separate scope.

- **Splitting any other mixed page** (`subscriber.md`, `dlq.md`,
  `relay.md`, `fastapi.md`). All of them mix purposes; only
  `observability.md` mixes three quadrants in 327 lines. The
  others stay as-is.

- **Renaming the existing nav sections** to Diátaxis-canonical names
  (Tutorials / How-to / Reference / Explanation). The current
  Overview / Getting started / Concepts / Guides / Reference /
  Operations naming reads more naturally to most users and already
  maps to the four quadrants conceptually. Don't churn the labels.

- **Adding the architecture deep-dives from `architecture/` to the
  public docs** as Explanation pages. That's a separate question
  about audience (operators / contributors / consumers); leave the
  current internal-only placement.

- **Voice review of existing reference pages.** Today's terse,
  precise voice on Reference pages is correct for Reference. The
  new tutorials get a different (warm, step-by-step) voice; the
  existing pages don't change.

- **A whole "Tutorials" sub-grouping in the nav.** Two tutorials
  slot under "Getting started" as flat entries with `Tutorial:`
  prefixes. If we add a third, revisit the grouping then.

## Design

### 1. Tutorial: Your first outbox app

New file: `docs/tutorials/first-outbox-app.md`. (New `tutorials/`
directory.)

Goal — a reader with Python and Postgres familiarity follows the
page top-to-bottom in roughly ten minutes and ends with a running
process where a single `broker.publish` results in the handler
running, with no surprises along the way.

Section outline:

```
# Tutorial: Your first outbox app

What you'll build         (2 sentences: a tiny app where publishing
                           inside a DB transaction triggers a handler)

Before you start          (Python 3.13+, Postgres, ~10 minutes)

Step 1: Install           (uv add 'faststream-outbox[asyncpg,validate]')

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

Voice: imperative + warm. Use "we" sparingly. Each step starts with
a one-sentence "what you're about to do" and ends with "you should
see X." No design rationale on the page; that's Concepts. Links to
Explanation for "why this works."

Code: one file (`app.py`) grows step by step. Each step shows the
*diff* from the previous step (or full file if the diff would be
larger than the file). Final file is ~30 lines.

### 2. Tutorial: Add a Kafka relay

New file: `docs/tutorials/add-kafka-relay.md`.

Goal — extends Tutorial #1 with a Kafka publisher decorator. Shows
the at-least-once contract end to end by deliberately killing Kafka
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

Voice: same warmth as Tutorial 1; assumes familiarity from Tutorial
1.

Code: extends the same `app.py` plus the `docker-compose.yml` from
Tutorial 1.

### 3. Split `usage/observability.md`

Today's `observability.md` (327 lines) becomes three pages, none of
which is named differently than today's nav entry. The existing URL
stays for SEO continuity.

**3a. `usage/observability.md` (kept, trimmed to Reference)**

Stays where it is. Becomes Reference-shaped: what events fire, what
tags each carries, what the middleware accepts, what the recorder
seam's signature is, the PromQL playbook queries. ~150 lines after
the trim.

Concretely keeps from current page:
- Section "The recorder seam" — the `Callable[[str, Mapping], None]`
  signature, the event catalog (`fetched` / `dispatched` /
  `acked` / `nacked_*` / `lease_lost` / `published` / `dlq_written`),
  the "must not block" note.
- The PromQL playbook table.
- Section "Test broker note".

Removed from current page (moved):
- "Prometheus adapter" full setup example → How-to (3b)
- "OpenTelemetry adapter" full setup example → How-to (3b)
- "Native middleware (spans + bus parity)" full setup example → How-to (3b)
- "Layering: middleware seam vs. recorder seam" + the layered
  example app + the section table → Explanation (3c)

**3b. `usage/setup-prometheus-opentelemetry.md` (new How-to)**

Goal — practical setup. Reader has decided they want metrics;
this page wires them in.

Sections:
- Install (`pip install 'faststream-outbox[prometheus,opentelemetry]'`)
- The recorder-only setup (bare seam, no middleware)
- Prometheus adapter setup (full app with `AsgiFastStream` +
  `/metrics`)
- OpenTelemetry adapter setup
- Both seams together (the "recommended setup" example with native
  middleware + recorder)

Voice: imperative, "to do X, do Y." Assumes competence — no Diátaxis
explanation of *why* there are two seams (that's 3c).

Direct port of current `observability.md`'s adapter sections, with
~10 lines of glue to remove cross-section references that no longer
apply.

**3c. `concepts/instrumentation-seams.md` (new Explanation)**

Goal — answer "why are there two instrumentation seams?" for the
curious reader.

Sections:
- The fundamental tension: events outside the bus
- What the middleware seam observes naturally
- What the middleware seam *can't* observe (the four cases:
  `fetched` ticks, `lease_lost` after `consume_scope` exits,
  `nacked_terminal(reason="max_deliveries")` before consume opens,
  empty-fetch idle counters)
- What the recorder seam observes naturally
- The layering table (rendered from current `observability.md`'s
  "Layering: middleware seam vs. recorder seam" table)
- Operator implication: pair both seams for full coverage

Voice: discursive, explanatory. No code snippets except the table.

### 4. Nav adjustments

```yaml
nav:
  - Overview: index.md
  - Getting started:
      - Installation: introduction/installation.md
      - Basic usage: usage/basic.md
      - 'Tutorial: Your first outbox app': tutorials/first-outbox-app.md
      - 'Tutorial: Add a Kafka relay': tutorials/add-kafka-relay.md
  - Concepts:
      - How it works: introduction/how-it-works.md
      - Comparison: concepts/comparison.md
      - Instrumentation seams: concepts/instrumentation-seams.md   # NEW
  - Guides:
      - FastAPI integration: usage/fastapi.md
      - Relay to Kafka / RabbitMQ / NATS: usage/relay.md
      - Timers: usage/timers.md
      - Testing: usage/testing.md
      - Schema validation: usage/schema-validation.md
      - Setup Prometheus and OpenTelemetry: usage/setup-prometheus-opentelemetry.md   # NEW
  - Reference:
      - Subscriber: usage/subscriber.md
      - Publisher: usage/publisher.md
      - Router: usage/router.md
      - Dead-letter queue: usage/dlq.md
      - Observability: usage/observability.md
  - Operations:
      - Production checklist: operations/checklist.md
      - Troubleshooting: operations/troubleshooting.md
      - Alembic migrations: operations/alembic.md
```

Four new entries (two tutorials + one how-to + one explanation),
zero file renames, zero URL changes for existing pages. Material's
sidebar handles 22 entries comfortably.

### 5. Cross-link updates

Mostly contained to the split:

- `usage/observability.md` (the kept Reference) — top of page, add
  a one-line pointer to the new how-to and the new explanation:
  > Setting it up: [Setup Prometheus and OpenTelemetry
  > ](./setup-prometheus-opentelemetry.md). Why two seams: [Concepts
  > § Instrumentation seams](../concepts/instrumentation-seams.md).
- `concepts/instrumentation-seams.md` — links back to the reference
  for the event catalog.
- `usage/setup-prometheus-opentelemetry.md` — links to the reference
  for the event catalog and to the explanation for the "why."
- `docs/index.md` decision-tree table — no change needed (no
  decision-tree row maps to the new pages; the tutorials are
  reachable via "Install and write the first publisher /
  subscriber", which they extend).

The two tutorials cross-link to each other (Tutorial 1's "What's
next" points at Tutorial 2) and to relevant Reference / Concepts /
Operations pages from each "What's next" footer.

### 6. The `tutorials/` directory

New top-level `docs/tutorials/`. Lives alongside `docs/concepts/`,
`docs/operations/`, `docs/usage/`, `docs/introduction/`. Consistent
with the existing flat directory layout.

## Operations

None — in-repo. The mkdocs deploy workflow re-runs on push to
`main` whenever `docs/**` or `mkdocs.yml` changes; this PR triggers
both. The new URLs become available immediately at:

- `https://faststream-outbox.modern-python.org/tutorials/first-outbox-app/`
- `https://faststream-outbox.modern-python.org/tutorials/add-kafka-relay/`
- `https://faststream-outbox.modern-python.org/usage/setup-prometheus-opentelemetry/`
- `https://faststream-outbox.modern-python.org/concepts/instrumentation-seams/`

`https://faststream-outbox.modern-python.org/usage/observability/`
stays at its current URL (trimmed content); inbound deep links
keep resolving.

## Out of scope (repeat list)

Already named under Non-goals; repeated for grep:

- Third tutorial (testing, scheduling, DLQ)
- Splitting `subscriber.md`, `dlq.md`, `relay.md`, `fastapi.md`
- Renaming nav sections to Diátaxis-canonical labels
- Adding `architecture/` deep-dives to public docs
- Voice review of existing Reference pages
- A dedicated Tutorials nav sub-grouping
- Migration-recipe regression tests (separate follow-on)
- `just plans` index generator (separate follow-on)

## Testing

Content-only; correctness is observable on the live site:

- `just docs-build` (`mkdocs build --strict`) passes clean — every
  internal cross-link from the four new pages and the trimmed
  `observability.md` resolves.
- `just lint` passes (eof-fixer, ruff format, ruff check, ty check).
- Tutorial code must be **executed end-to-end** during plan
  execution against a clean machine. Tutorial 1: from scratch, every
  step's expected output observed. Tutorial 2: same, including the
  "kill Kafka, see retry" step. The plan author runs these and the
  *literal terminal output* lands inside the tutorial under "you
  should see X" — no hand-edited expected output. Tutorials that
  haven't been run produce frustration when readers try them and
  miss a step.
- Reviewer manual sidebar scan: `just docs-serve` and confirm the
  four new entries appear in the four new sidebar positions.
- Reviewer reads both tutorials end-to-end against a fresh checkout
  and a clean Postgres / Kafka — the most valuable review move for
  a tutorial.

## Risk

- **Tutorial voice drifts from the existing reference voice and
  introduces inconsistency across the site.** Mitigated by the
  explicit voice guidance in §1 and §2 (warm, step-by-step in
  tutorials; everything else unchanged). The "What you just built"
  recap pattern, the "Before you start" preamble, and the
  "What's next" footer are intentional voice markers — they signal
  "you are reading a tutorial" without requiring a Diátaxis-
  literate reader. Tutorial voice is allowed to feel different from
  Reference voice because they're serving different reader needs.

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

- **Splitting `observability.md` breaks inbound deep links to its
  current anchors.** Mitigated by keeping the page at its current
  URL (only the content is trimmed) and by the URL-stable nature of
  the move: section anchors that survive the trim (`#the-recorder-seam`,
  `#test-broker-note`) keep resolving. Anchors that move to other
  pages (`#layering-middleware-seam-vs-recorder-seam`,
  `#prometheus-adapter`) break — they redirect via the new
  cross-link callout at the top of the trimmed Reference page.

- **The "What's next" footers create a maintenance graph.** Adding
  a new tutorial later means revisiting the footer of every
  existing tutorial to add the cross-link. At two tutorials the
  cost is trivial. Re-evaluate if we ever add a fourth.

- **Tutorial #2's "kill Kafka, see retry" step is the
  most-likely-to-flake step in either tutorial.** Local environments
  differ; Kafka's failure modes are platform-sensitive (especially
  on Apple Silicon). Mitigated by recommending Confluent's
  `cp-kafka` image specifically (known to work on M1+ from prior
  use) and by treating the step as *demonstrative*: the tutorial
  doesn't fail if Kafka comes back instantly with no observable
  retry, because the at-least-once property is still preserved.
  Reviewer flags if the step's reproduction is fragile and we drop
  it from the tutorial in favor of a one-paragraph callout
  explaining the contract.
