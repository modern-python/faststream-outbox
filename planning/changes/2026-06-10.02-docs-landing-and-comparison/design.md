---
status: shipped
date: 2026-06-10
slug: docs-landing-and-comparison
summary: Docs landing rewrite, four-section nav reshape, new Comparison page.
supersedes: null
superseded_by: null
pr: "50"
outcome: merged 2026-06-10 as #50
---

# Design: Rework the docs landing + nav, add a comparison page

## Summary

The current docs are reference-grade but new-user-hostile: `docs/index.md`
is a 22-line TOC, the mkdocs nav opens with "Relay to a foreign broker"
(advanced use case), the canonical "FastAPI + outbox" guide is buried at
position 7, and there is no answer to the first question a prospective
user asks: *"is this the right tool for me?"*.

This change does three things, in one pass:

1. Rewrite `docs/index.md` as a real landing page — value proposition,
   decision tree, "you're here because" jumplist into the rest of the
   docs.
2. Re-shape `mkdocs.yml` nav into four progressive-disclosure sections
   (Getting started → Concepts → Guides → Reference) without moving or
   renaming any existing files, so all URLs and external links stay
   stable.
3. Add a new `docs/concepts/comparison.md` page that names alternatives
   (raw outbox, CDC / Debezium, Kafka transactions, PG-NOTIFY,
   Celery, FastStream-only) and says when each is the better choice.

No runtime code, test code, or public API is touched. No mkdocs plugins
are added.

## Motivation

- **The landing page tells a new user nothing.** `docs/index.md` is a
  bullet list of page titles. A reader arriving from PyPI, the README,
  or search has no signal whether this library fits their problem; they
  must guess and click. Every other modern Python lib's docs index
  opens with a value-prop + decision-tree pattern (Pydantic, SQLAlchemy
  2.0, FastAPI, FastStream itself). Matching that pattern is the
  single highest-ROI docs move available.
- **The nav order is inverse to user intent.** Today's order:

  ```
  Relay  →  Basic  →  Subscriber  →  DLQ  →  Publisher  →  Router
       →  FastAPI  →  Timers  →  Testing  →  Schema  →  Observability
  ```

  Most adopters land here for "transactional outbox under FastAPI"
  (the canonical use case explicitly named in `usage/fastapi.md`),
  not for "relay outbox rows to Kafka". Putting Relay first reads as
  a topic-hierarchy mistake to anyone scanning the sidebar. The
  Basic-usage page (the actual getting-started flow) is the second
  Usage entry but lives alongside Reference material.

- **No comparison page = unanswerable "should I use this?".** The
  README does some of this work, but docs readers don't always arrive
  via the README. The team has already done the CDC / WAL analysis
  (memory: `cdc_wal_rejected.md`, 2026-05-07) — that thinking deserves
  a public home so users and contributors don't ask again.

- **Cheap to land.** This is config + two pages; nothing depends on
  it, and follow-ups (production checklist, troubleshooting, diagrams,
  DRY pass on the transactional-contract paragraph that appears in 6
  pages) layer cleanly on top.

## Non-goals (this spec)

Deliberately *not* covered here; each is a candidate follow-on spec:

- **B — Operator pages.** Production checklist, Troubleshooting,
  Alembic migration snippet. Pulls scattered operator content into
  one section. Highest-value follow-on.
- **C — Diagrams + worked examples.** Mermaid sequence diagrams for
  publish path / fetch CTE / lease lifecycle / drain phases; an
  end-to-end "checkout" worked example wiring publisher + subscriber +
  DLQ + relay.
- **D — DRY the canon.** Pull the transactional-contract paragraph
  (currently repeated near-verbatim in `index.md`, `how-it-works.md`,
  `basic.md`, `publisher.md`, `fastapi.md`, `relay.md`) into a single
  canonical source and replace duplicates with one-line links. Stops
  the docs drifting against each other.
- **F — Diátaxis rewrite.** Full restructure as Tutorial / How-to /
  Reference / Explanation. Larger commitment than this pass; the
  four-section nav this spec lands gestures at the same shape without
  committing to it.

Also out of scope:

- **File renames / moves.** Every existing page keeps its current path.
  The README, the recently-migrated GH Pages site, and any external
  inbound links all keep working. The nav reshape is metadata-only.
- **mkdocs plugins / theme changes.** No `mike` for versioning, no
  social cards, no privacy plugin. The current minimal theme is
  intentional; expansion is a separate conversation.
- **Voice / tone edits to existing pages.** The reference voice is
  already strong; this spec touches the landing and adds one new
  page only.
- **Removing the duplicated transactional-contract paragraphs.** That
  is the D follow-on. Leaving it untouched here keeps the diff small
  and the risk profile near zero.

## Design

### 1. Rewrite `docs/index.md`

The new landing has four blocks, in order:

**Block A — Value prop (one paragraph).** Same content as today's first
paragraph, lightly tightened. States what the library is and the
transactional-outbox contract in one sentence.

**Block B — "Use it when / don't use it when" (two short lists).**
Concrete and binary — no hedging. Examples:

> **Use `faststream-outbox` when**
>
> - You already have Postgres and don't want to add a message bus just
>   to get at-least-once delivery alongside your domain writes.
> - You want the row insert to commit atomically with the rest of your
>   SQLAlchemy transaction (no two-phase commit, no Sagas).
> - You're building on FastStream or FastAPI and want the same
>   subscriber / dependency-injection ergonomics for an outbox.
>
> **Reach for something else when**
>
> - You're already running Kafka / Rabbit / NATS *and* don't need
>   transactional atomicity with a DB write → use that broker directly.
> - You need sub-second scheduled-delivery precision → see
>   [Timers § latency floor](usage/timers.md#latency-floor).
> - You're on a non-Postgres database → this package is Postgres-only
>   at v0. CDC / Debezium may be a better fit (see
>   [Comparison](concepts/comparison.md)).

**Block C — Decision tree → next page.** A small table that takes a
user's intent and routes them into the right starting page:

> | If you want to… | Start at |
> |---|---|
> | See it work end-to-end on a FastAPI app | [FastAPI integration](usage/fastapi.md) |
> | Relay outbox rows to Kafka / RabbitMQ / NATS / Redis | [Relay to Kafka / RabbitMQ / NATS](usage/relay.md) |
> | Understand the architecture before adopting | [How it works](introduction/how-it-works.md) |
> | Compare against CDC / Kafka transactions / a hand-rolled outbox | [Comparison](concepts/comparison.md) |
> | Install and write the first publisher / subscriber | [Installation](introduction/installation.md) → [Basic usage](usage/basic.md) |

**Block D — Documentation map.** The structured index that today's
landing already is, but organized into the four sections the new nav
uses (Getting started / Concepts / Guides / Reference). Bullet list,
one line per page, terse description after the link. Replaces the flat
list of 11 page titles.

### 2. Reshape `mkdocs.yml` nav

Replace the current two-section nav (Introduction / Usage) with four
sections. **No file paths change.** This is purely a `nav:` block
rewrite:

```yaml
nav:
  - Overview: index.md
  - Getting started:
      - Installation: introduction/installation.md
      - Basic usage: usage/basic.md
  - Concepts:
      - How it works: introduction/how-it-works.md
      - Comparison: concepts/comparison.md          # new — see §3
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

Load-bearing choices:

- **"FastAPI integration" leads the Guides section.** It's the canonical
  use case the architecture is designed around; surfacing it first
  matches the README and the structure of `usage/fastapi.md` itself
  ("the outbox + FastAPI is the canonical use case").
- **"Relay to Kafka / RabbitMQ / NATS" replaces "Relay to a foreign
  broker"** as a nav label only — the file stays at `usage/relay.md`.
  Reasons: (a) the names of the supported brokers are what users
  search for; (b) "a foreign broker" is jargon that means nothing
  before clicking through. The H1 inside `relay.md` stays
  "Relay to a foreign broker" to avoid pointlessly churning page
  titles and inbound links to anchored sections.
- **Basic usage moves from Usage to Getting started**, sitting next to
  Installation. The page is a getting-started narrative ("1. Declare
  the outbox table → 2. Create the broker → 3. Register a subscriber
  → 4. Publish a message"), so placing it under Getting started
  matches its actual content.
- **Subscriber / Publisher / Router / DLQ / Observability move to
  Reference.** They're reference pages today — exhaustive option
  tables, label-set tables, schema columns. The label change matches
  what they actually are.
- **`how-it-works.md` stays under `introduction/`** on disk but moves
  to Concepts in the nav. The file path stays so external links
  (notably the README's "How it works" link, and the README's recent
  GH-Pages URL update per the `mkdocs-github-pages` spec) keep
  resolving. Concepts is the user-facing label.
- **`navigation.expand` already on in `mkdocs.yml`.** The four-section
  expansion stays usable in the sidebar without scrolling — Reference
  is five entries, Guides is five, Concepts is two, Getting started
  is two. Material theme's sidebar handles this size cleanly.

### 3. New file `docs/concepts/comparison.md`

New top-level file under a new `concepts/` directory. Sections:

1. **`faststream-outbox` vs writing your own.** Honest about the
   complete list of pieces you'd re-implement: lease tokens, partial
   index design, fetch-and-claim CTE shape, retry-strategy template,
   lease-loss invariant on terminal writes, `validate_schema()`,
   drain semantics, LISTEN/NOTIFY short-circuit, NOTIFY suppression
   on future-dated rows, `timer_id` dedup, DLQ atomicity CTE. Cross-
   linked into the relevant existing reference pages so the user can
   verify scope rather than take the list on faith.
2. **vs CDC (Debezium, logical replication).** Direct port of the
   reasoning from memory `cdc_wal_rejected.md`. CDC wins when you
   already need WAL-level change capture for analytics, when you want
   transparent capture of writes from non-FastStream services, or
   when you cannot tolerate the polling overhead. `faststream-outbox`
   wins when you control the producer code, when the async-Python
   tooling for logical replication is too thin (the memory's load-
   bearing reason), and when handler-level retry / DLQ / scheduling
   semantics are needed inline.
3. **vs Kafka transactions (or Rabbit publisher confirms).** Atomic
   `DB-write + bus-publish` is achievable with Kafka transactions
   plus 2PC or with idempotent producers + an inbox pattern. Trade-
   offs: requires Kafka (operational footprint, schema registry,
   consumer-group rebalancing), no native cancellation / timers /
   `timer_id` dedup, no single-tx contract with arbitrary domain
   writes.
4. **vs plain PG-NOTIFY.** PG-NOTIFY is fire-and-forget and lossy
   across listener disconnect; `faststream-outbox` keeps the row
   durable until the handler ack and uses NOTIFY only as a wake-up
   short-circuit on top of polling. Worked example: what happens to
   a NOTIFY when the listener was reconnecting at emit time, in each
   shape.
5. **vs Celery + DB result backend.** Different abstraction level —
   Celery is task queues, `faststream-outbox` is message routing
   with FastStream subscriber semantics. Use Celery when you want
   ad-hoc background jobs initiated from anywhere; use this when you
   want at-least-once dispatch of *events* tied to DB transactions
   and prefer FastStream's broker/subscriber model.
6. **vs FastStream + KafkaBroker / RabbitBroker directly.** Use the
   foreign broker directly when you don't need transactional
   atomicity with a DB write; use `faststream-outbox` plus
   [Relay](../usage/relay.md) when the producer side does need it.
   This is not an either/or — the canonical relay shape composes
   both.

Each section ends with a one-line "TL;DR" verdict so a scanning reader
can lift the answer without reading the discussion.

The page lives at `docs/concepts/comparison.md` (creates a new
`concepts/` directory under `docs/`). That keeps it discoverable on
disk under a category name that matches the nav section label, and
leaves room for future Concepts pages (e.g. the D follow-on could
land a "Transactional contract" canonical page here).

### 4. Cross-links from existing pages into `comparison.md`

Minimal, no other content changes:

- `introduction/how-it-works.md` § "The transactional outbox pattern":
  add a one-line "See [Comparison](../concepts/comparison.md) for when
  CDC or Kafka transactions are the better fit."
- `usage/relay.md` § intro paragraph: add a tail "If you don't have a
  database write to atomically commit alongside, use the foreign
  broker directly — see [Comparison](../concepts/comparison.md)."

Just two links. The comparison page is also reachable from the new
landing's decision-tree table, so deep cross-linking from every page
isn't load-bearing.

## Operations

None — this is config + new docs content, fully in-repo. The
`mkdocs-github-pages` deploy workflow (`.github/workflows/docs.yml`,
landed 2026-06-09) re-deploys on push to `main` whenever `docs/**` or
`mkdocs.yml` changes, which both halves of this spec trigger.

After landing:

- The `concepts/comparison.md` URL becomes available at
  `https://faststream-outbox.modern-python.org/concepts/comparison/`.
- All existing URLs (`usage/relay/`, `usage/fastapi/`, etc.) continue
  to resolve. The README links untouched by this spec keep working;
  the README links updated in `mkdocs-github-pages` keep working.

## Testing

This spec is content + nav config; correctness is observable on the
live site:

- `mkdocs build --strict` succeeds locally and in the deploy workflow
  (catches broken cross-links from `index.md` → new sections,
  catches a misspelled file path in the nav).
- The deploy workflow run on merge completes green and the new
  Comparison page renders at its URL.
- The reshaped sidebar renders with four sections, expanded by
  default (`navigation.expand` is already on), no entries missing.
- Spot-check that every page from the previous nav still appears in
  the new nav — this spec promises no pages are dropped, only
  re-grouped.

No new pytest hooks are added. `just lint-ci` continues to lint the
files that change (markdown formatting via the ruff EOF fixer, yaml
formatting on `mkdocs.yml`).

## Risk

- **Nav reshuffle confuses bookmarks of the *sidebar position*.** Low
  risk: deep links (the URLs people actually bookmark) don't change.
  Anyone navigating by "the page two below DLQ in the sidebar" is
  exotic enough to absorb the change. Material theme search and the
  decision-tree table on the new landing both let users re-find pages
  by intent rather than by old position.
- **Comparison page is opinionated and could age poorly.** Mitigated
  by sourcing claims from the existing architecture / memory and
  marking the CDC section as reflecting a 2026-05-07 reassessment.
  Easy to update later when the async-Python logical-replication
  tooling situation changes.
- **External SEO for "Relay to a foreign broker"** — the nav label
  change reshapes how the page appears in search results from
  mkdocs-material's social cards (if ever enabled) and breadcrumbs.
  The H1 stays "Relay to a foreign broker", so canonical SEO is
  unchanged; only the sidebar label and breadcrumb text shift. Net
  expected to be neutral-to-positive (more specific terms in the
  visible label).
- **The four-section nav grows.** Adding Concepts and Guides as
  distinct sections invites future content to be slotted in. If
  follow-on specs B/C/D land, Operations could become a fifth
  section (Production checklist, Troubleshooting). The nav scales
  fine to five sections under Material theme; the structure
  established here is the seed.
