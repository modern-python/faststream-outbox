---
status: draft
date: 2026-06-19
slug: docs-diataxis-nav
supersedes: null
superseded_by: null
pr: null
outcome: null
---

# Design: Dissolve the "Patterns" nav section into Guides

## Summary

Remove the standalone "Patterns" nav section (added at #103) and fold its one
page into the existing **Guides** section, with **no section renames**. The
messaging-service page becomes a Guide ("A messaging service, end-to-end"), and
its file moves `docs/patterns/messaging-service.md` →
`docs/usage/messaging-service.md` to sit with the other Guides. Seven top-level
sections become six; Concepts, Reference, Operations, and Getting started are
untouched.

(The slug `docs-diataxis-nav` is retained from the bundle's first draft; the
shipped scope is the minimal "merge into Guides" variant, not a full Diátaxis
rename.)

## Motivation

The recently-shipped "Patterns" section (#103) holds a single worked end-to-end
case study, which reads as overlapping with both Tutorials and Guides and made
the nav feel like it had too many similar sections. The page is, in practice, a
*how-to that composes several how-tos* — so it belongs inside Guides, not in a
section of its own. Folding it in removes the overlap with the smallest possible
change and leaves every other section name and grouping as-is.

## Non-goals

- No section renames (Getting started / Concepts / Guides / Reference /
  Operations all keep their names and order).
- No rewrite of the page body — only its title, internal relative links, and
  one nav/landing-page reference change.
- No change to any other page's content.

## Design

### 1. Nav: drop "Patterns", add the page under Guides

Delete the `Patterns:` section. Add the page as the **last item in Guides**:

```yaml
  - Guides:
      - FastAPI integration: usage/fastapi.md
      - Relay to Kafka / RabbitMQ / NATS: usage/relay.md
      - Timers: usage/timers.md
      - Testing: usage/testing.md
      - Schema validation: usage/schema-validation.md
      - Setup Prometheus and OpenTelemetry: usage/setup-prometheus-opentelemetry.md
      - 'A messaging service, end-to-end': usage/messaging-service.md
```

It sits last because it composes the relay / timer / testing guides that precede
it — a reader meets the primitives first, then the worked composition.

### 2. File move + link fixups

`git mv docs/patterns/messaging-service.md docs/usage/messaging-service.md`,
then remove the now-empty `docs/patterns/` directory. The URL only shipped at
#103 and nothing in the docs links to the page (verified by grep), so the move
is safe; no redirect needed.

Relative links inside the moved page, now resolving from `docs/usage/`:

| Line(s) | Current | After move |
|---|---|---|
| 3 | `../tutorials/first-outbox-app.md` | **unchanged** (`tutorials/` and `usage/` are siblings under `docs/`) |
| 165, 276 | `../usage/relay.md` | `relay.md` |
| 230 | `../usage/timers.md` | `timers.md` |
| 272 | `../usage/testing.md` | `testing.md` |
| 278 | `../usage/dlq.md` | `dlq.md` |
| 280 | `../usage/observability.md` | `observability.md` |

### 3. No intro retune needed

The page intro already contrasts itself against "the tutorials… and each guide
documents one feature on its own. This page is different…". That framing reads
correctly for a page that *is* a Guide — a guide that composes rather than
isolates. The page keeps its `# A messaging service, end-to-end` H1 and gets no
"Tutorial:" prefix (it is not a tutorial).

### 4. Landing page (`index.md`) sync

Add one bullet to the existing `### Guides` list (no heading changes), e.g.:

```markdown
- [A messaging service, end-to-end](usage/messaging-service.md) — the relay,
  timer, and testing guides composed in one service.
```

No other `index.md` edits — nothing is renamed.

## Testing

- `just docs-build` (`mkdocs build --strict`) passes — no broken links, no
  orphaned page. Primary gate.
- `grep -rn 'patterns/' docs/` returns nothing (no stale path).
- `grep -rn 'messaging-service' docs/` shows only `usage/messaging-service.md`
  and the `index.md` bullet.
- `docs/patterns/` no longer exists; `git` records the move as a rename.

## Risk

- **Low: the page URL changes** (`/patterns/…` → `/usage/messaging-service/`).
  Shipped minutes ago at #103, no external inbound links. No redirect needed.
- **Low: a missed relative-link fixup inside the moved page.** Caught by
  `--strict` (broken-link failure) and the explicit line table above.
