# Architecture

The living truth about what `faststream-outbox` does **now** — one file per
capability, updated by hand whenever a change ships. The *why* and *how it got
here* live in [`../planning/changes/`](../planning/changes/), and decisions
deliberately taken (including options rejected) in
[`../planning/decisions/`](../planning/decisions/); this directory is the present.

Each capability file is an **implementation-detail** page. Its terse
**invariant summary** ("what Claude must not break") lives in
[`../CLAUDE.md`](../CLAUDE.md) § Architecture; the **user-facing** account lives
under `../docs/`.

These files carry **no frontmatter** — they are prose, dated by git.

## Capabilities

- [producer.md](producer.md) — the publish path and transactional contract.
- [relay.md](relay.md) — foreign-broker relay chain and guardrails.
- [timers.md](timers.md) — delayed delivery, `timer_id` dedup, `cancel_timer`.
- [schema.md](schema.md) — the user-owned table, partial indexes, `validate_schema()`.
- [dlq.md](dlq.md) — opt-in dead-letter on terminal failure.
- [subscriber.md](subscriber.md) — the two-loop subscriber and lease-token invariant.
- [drain.md](drain.md) — graceful drain on stop.
- [test-broker.md](test-broker.md) — `TestOutboxBroker`, the fake client, the client contract.
- [integration.md](integration.md) — annotations, FastAPI router, engine ownership.
- [metrics.md](metrics.md) — the recorder and native-middleware seams.
- [retry.md](retry.md) — retry strategies.

## Promotion rule

Shipping a change hand-edits the affected capability file(s) here to match the
new reality, in the same PR as the code. The change bundle stays in place under
[`../planning/changes/`](../planning/changes/) — no folder move.
