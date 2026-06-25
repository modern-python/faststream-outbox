# docs-diataxis-nav — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dissolve the standalone "Patterns" nav section by moving the
messaging-service page into the existing Guides section, with no renames.

**Spec:** [`design.md`](./design.md)

**Branch:** `docs/diataxis-nav` (already checked out; spec already committed).

**Commit strategy:** Single commit — the file move, link fixups, nav change, and
index bullet are interdependent and must land together (a partial state fails
`--strict`).

## Global constraints

- MkDocs Material; build with `just docs-build` (= `mkdocs build --strict`).
  `--strict` fails on broken internal links or orphaned pages.
- **No section renames** — Getting started / Concepts / Guides / Reference /
  Operations keep their names and order.
- No page-body rewrite. Only the moved page's relative links change; its `# A
  messaging service, end-to-end` H1 stays; no "Tutorial:" prefix.
- Destination path: `docs/usage/messaging-service.md` (where all Guides pages
  live). The `docs/patterns/` directory must not exist after the change.

---

### Task 1: Move the page into Guides and update nav + index

**Files:**
- Move: `docs/patterns/messaging-service.md` → `docs/usage/messaging-service.md`
- Modify: `docs/usage/messaging-service.md` (relative-link fixups)
- Modify: `mkdocs.yml` (drop `Patterns:` section, add page under `Guides:`)
- Modify: `docs/index.md` (add one bullet to the `### Guides` list)

This is the whole change; it is verified by a clean strict build.

- [ ] **Step 1: Move the file with git**

  Run:
  ```bash
  git mv docs/patterns/messaging-service.md docs/usage/messaging-service.md
  rmdir docs/patterns 2>/dev/null || true
  ```
  Expected: the file is staged as a rename; `docs/patterns/` no longer exists.

- [ ] **Step 2: Fix the relative links inside the moved page**

  In `docs/usage/messaging-service.md`, the `../usage/*` links must become
  same-directory links. The `../tutorials/first-outbox-app.md` link is left
  unchanged (`tutorials/` and `usage/` are both children of `docs/`).

  Apply these exact replacements (each `](../usage/X.md)` → `](X.md)`):
  - `](../usage/relay.md)` → `](relay.md)` — occurs twice (the Pattern 1 note
    and the See-also footer)
  - `](../usage/timers.md)` → `](timers.md)`
  - `](../usage/testing.md)` → `](testing.md)`
  - `](../usage/dlq.md)` → `](dlq.md)`
  - `](../usage/observability.md)` → `](observability.md)`

  Verify no `../usage/` remains and the tutorials link is intact:
  ```bash
  grep -n '](\.\./usage/' docs/usage/messaging-service.md   # expect: no output
  grep -n '](\.\./tutorials/first-outbox-app.md)' docs/usage/messaging-service.md  # expect: 1 match
  ```

- [ ] **Step 3: Update `mkdocs.yml` nav**

  Replace the `Guides:` section and the entire `Patterns:` section with the
  Guides section below (this deletes `Patterns:` and appends the page as the
  last Guides item). The block to replace currently reads:

  ```yaml
    - Guides:
        - FastAPI integration: usage/fastapi.md
        - Relay to Kafka / RabbitMQ / NATS: usage/relay.md
        - Timers: usage/timers.md
        - Testing: usage/testing.md
        - Schema validation: usage/schema-validation.md
        - Setup Prometheus and OpenTelemetry: usage/setup-prometheus-opentelemetry.md
    - Patterns:
        - 'A messaging service, end-to-end': patterns/messaging-service.md
  ```

  Replace it with:

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

- [ ] **Step 4: Add the bullet to `index.md`'s Guides list**

  In `docs/index.md`, under `### Guides`, after the
  `Setup Prometheus and OpenTelemetry` bullet (the last one before `### Reference`),
  add:

  ```markdown
  - [A messaging service, end-to-end](usage/messaging-service.md) — the relay,
    timer, and testing guides composed in one service.
  ```

- [ ] **Step 5: Strict build**

  Run: `just docs-build`
  Expected: `mkdocs build --strict` completes with no warnings or errors (in
  particular, no broken-link or orphaned-page error for
  `usage/messaging-service.md`).

- [ ] **Step 6: Verify no stale paths**

  Run:
  ```bash
  grep -rn 'patterns/' docs/        # expect: no output
  test -d docs/patterns && echo "STILL EXISTS" || echo "gone"   # expect: gone
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add -A docs/ mkdocs.yml
  git commit -m "docs: move the messaging-service page into Guides; drop Patterns section

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

## Self-review

- **Spec coverage:** nav drop+add (Step 3), file move (Step 1), link fixups
  table (Step 2), no intro retune (spec §3 — nothing to do), index bullet
  (Step 4), strict build + grep checks (Steps 5–6). All spec sections mapped.
- **Placeholders:** none — every edit is given verbatim.
- **Consistency:** destination path `docs/usage/messaging-service.md` and the
  nav target `usage/messaging-service.md` agree; the index link
  `usage/messaging-service.md` agrees.
