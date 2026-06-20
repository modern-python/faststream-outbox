---
status: shipped
date: 2026-06-19
slug: messaging-service-patterns-doc
summary: New docs/patterns/ section with one page composing the outbox in an anonymized chat/notifications service: transactional event relay, fire-unless-cancelled timer, nested test brokers.
supersedes: null
superseded_by: null
pr: 103
outcome: Shipped docs/patterns/messaging-service.md + a Patterns nav section — one anonymized service composing transactional relay, fire-unless-cancelled timer, and nested test brokers. No architecture/ change (docs only, no new invariant).
---

# Design: A "Patterns" docs page composing the outbox in a real service

## Summary

Add one docs page, `docs/patterns/messaging-service.md`, surfaced under a new
top-level **Patterns** nav section (between Guides and Reference). It walks a
single anonymized chat / notifications service that composes three outbox
primitives end-to-end — transactional event relay, a fire-unless-cancelled
timer, and testing the composed app — wiring them with a real DI container
(`modern-di-faststream`, `FromDI`). The page does not re-teach each primitive;
it shows how they fit together in one service and cross-links the existing
guides for depth. Patterns are drawn from a real production service but
presented generically.

## Motivation

The current docs teach primitives in isolation. `tutorials/first-outbox-app.md`
and `tutorials/add-kafka-relay.md` build a greenfield app one primitive at a
time; `usage/timers.md`, `usage/relay.md`, and `usage/testing.md` document each
feature abstractly. Nothing shows the primitives **composed in a real service**
— how `publish` lands inside a domain transaction beside business writes, how a
timer is armed and later cancelled by a second use case, and how all of it is
asserted in-process with nested test brokers.

The gap is sharpest for the **fire-unless-cancelled timer**. `usage/timers.md`
shows `publish(..., timer_id=, activate_in=)` and `cancel_timer(...)` as
separate API calls, but never the actual pattern: arm a delayed notification on
write, cancel it on read, accept the race window in between. That is the most
valuable real-world recipe we are not currently demonstrating.

Observed real usage (the source patterns, to be anonymized):

- Use cases write domain rows **and** `broker.publish(queue=, session=)` inside
  one transaction, then a single `commit()` — publish never flushes or commits.
- An "unread message" notification armed with `publish(timer_id=str(message_id),
  activate_in=timedelta(seconds=N))` and disarmed on read with
  `cancel_timer(queue=, timer_id=, session=)`.
- An `OutboxRouter.subscriber(queue)` handler that relays the row to Kafka via a
  DI-injected producer (hand-rolled relay, not the native decorator).
- Tests nesting `TestOutboxBroker` + `TestKafkaBroker`, plus a
  `validate_schema()` check against a real engine.

## Non-goals

- Re-teaching any primitive in depth — the page links the existing guides.
- A "production hardening" / next-steps section. Coverage is the core trio only;
  hardening features (native relay decorator, DLQ, metrics recorder) get
  one-line "See also" links, not prose.
- Referencing the private source service by name. The page is framed as a
  generic chat / notifications service.
- A migration-off-legacy-worker narrative (considered, dropped from scope).
- Runnable/CI-tested code. Snippets are illustrative but keyed to the real
  public API.

## Design

### 1. Placement and navigation

New file `docs/patterns/messaging-service.md`. New `mkdocs.yml` nav section
**Patterns**, inserted between **Guides** and **Reference**:

```yaml
  - Patterns:
      - 'A messaging service, end-to-end': patterns/messaging-service.md
```

A dedicated section (rather than tucking the page under Guides) signals "here is
what it looks like in production when the pieces are combined" and leaves room
for future case studies.

### 2. The scenario (page intro)

A generic chat / notifications service. Users post messages into chats. Two
obligations, both of which must be **atomic with the database write** and must
**never fire if the transaction rolls back**:

1. Broadcast every message / read / delete **event** to downstream consumers
   over Kafka.
2. Send an **"unread" notification** N seconds after a message arrives —
   *unless the recipient reads it first*.

The intro explains why a plain message bus cannot satisfy "commits with the
domain row," motivating the outbox, and names the two queues used throughout:
`chat-events` and `unread-timers`.

### 3. Architecture at a glance

A short ASCII diagram (~5 lines) plus a sentence each:

```
  CreateMessageUseCase  ─┐
   (one DB transaction)  ├─▶ outbox row: chat-events   ─▶ subscriber ─▶ Kafka
                         └─▶ outbox row: unread-timers  ─▶ subscriber ─▶ Kafka
                                  ▲
  ReadMessageUseCase ── cancel_timer(unread-timers, timer_id) ┘
```

### 4. Pattern 1 — Transactional event relay

- A thin producer wrapper exposing `send_chat_event(event)` that calls
  `broker.publish(event, queue="chat-events", session=session)`.
- A `CreateMessageUseCase` dataclass that, inside `async with transaction:`,
  writes the domain row, calls the producer, then `await transaction.commit()`.
  Emphasize: `publish` does not flush, commit, or open its own transaction — the
  outbox row commits atomically with the domain write.
- The relay subscriber: `@router.subscriber("chat-events")` with a handler that
  re-publishes to Kafka via a `FromDI`-injected Kafka producer.
- One line: the native relay decorator (`@kafka_pub @broker.subscriber(...)`) is
  an alternative that removes the hand-rolled hop → link `usage/relay.md`.

### 5. Pattern 2 — Fire-unless-cancelled timer

- Arm: in the same create transaction, `broker.publish(payload,
  queue="unread-timers", timer_id=str(message_id),
  activate_in=timedelta(seconds=N), session=session)`.
- Disarm: in `ReadMessageUseCase`, `broker.cancel_timer(queue="unread-timers",
  timer_id=str(message_id), session=session)`.
- Explain the semantics that make this safe and what the limits are:
  - `timer_id` gives **at-most-one-live** dedup per `(queue, timer_id)` — a
    re-arm of the same id is a no-op while a row is in flight.
  - `cancel_timer` is guarded by `acquired_token IS NULL`: it returns `False`
    if the timer is already being delivered. There is an inherent race window
    — once the timer is leased, a read can no longer cancel it; the downstream
    consumer must tolerate the occasional already-read notification.
- Link `usage/timers.md`.

### 6. Pattern 3 — Testing the composed app

- Nest `TestOutboxBroker(outbox_broker)` and `TestKafkaBroker(kafka_broker)`.
  In default sync mode, calling a use case drives outbox row → relay handler →
  Kafka test broker in-process, so a single test asserts the whole chain.
- Caveats to call out (each a real gotcha):
  - Future-dated rows fire **immediately** in sync mode; to test real delay /
    cancel timing, construct with `run_loops=True`.
  - `validate_schema()` needs a **real** engine — the fake client raises
    `NotImplementedError`. Show the schema check as a separate test against a
    real `OutboxBroker`.
- Link `usage/testing.md`.

### 7. See also (footer)

Three one-line cross-links, no prose: native relay (`usage/relay.md`),
dead-letter queue (`usage/dlq.md`), observability (`usage/observability.md`).

### 8. Code-sample conventions

- Anonymized, framework-neutral domain names: `OutboxEventProducer`,
  `CreateMessageUseCase`, `ReadMessageUseCase`; queues `chat-events` /
  `unread-timers`.
- DI shown with a **real container** — `modern-di-faststream` providers +
  `FromDI` in handlers — matching how the pattern is wired in practice.
- Every API call keyed to the verified public surface: `OutboxBroker`,
  `OutboxRouter`, `make_outbox_table`, `TestOutboxBroker`, and broker methods
  `publish` / `cancel_timer` / `fetch_unprocessed`.

## Testing

- `just docs-build` (`mkdocs build --strict`) passes — no broken internal links,
  new page resolves in nav.
- Self-review: every cross-link target file exists; no placeholders; code
  snippets use only symbols in `faststream_outbox.__all__` and real broker
  method signatures.

## Risk

- **Low: snippet drift.** Illustrative code is not CI-executed, so API changes
  could silently stale it. Mitigation: snippets are minimal and keyed to the
  public `__all__`; the same primitives are covered by executed examples in the
  tutorials.
- **Low: nav churn.** Adding a top-level section shifts the sidebar. Mitigation:
  inserted in a logical slot (Guides → Patterns → Reference); no existing pages
  move.
