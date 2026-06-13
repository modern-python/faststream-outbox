# Deferred Work

Items raised in reviews or audits that are real but not actionable now.
Each is parked here with the reason it's deferred and the concrete trigger
that should bring it back. This is the long-tail register — not a backlog
of planned work. When an item is picked up it graduates to a spec/plan
bundle in [`changes/active/`](changes/active/); see [CLAUDE.md](../CLAUDE.md#workflow).

As of the 2026-06-12 code + docs audit closure (PRs #61–#74), the audit
backlog is empty. The items below are the remainder: technically real,
but deliberately unscheduled pending a trigger.

## Open

### FastAPI integration

- **`OutboxRouter` doesn't forward `dlq_table` / `metrics_recorder` /
  `routers`.** These three `OutboxBroker.__init__` arguments are not
  exposed on `OutboxRouter.__init__`, and the router constructs the broker
  internally with no handle to inject a pre-built one — so a FastAPI user
  **cannot** enable the dead-letter queue or the metrics-recorder seam
  through the router at all. The only path today is a standalone
  `OutboxBroker`. This is documented as a limitation in
  [`docs/usage/fastapi.md`](../docs/usage/fastapi.md) (audit improvement
  P18, #72). Forwarding the kwargs is small in `OutboxRouter.__init__` +
  the `super().__init__` passthrough, but it's a real feature with a
  design surface (defaults, AsyncAPI/typing implications, whether
  `routers` even makes sense through the FastAPI lifespan), not a
  mechanical fix — hence a spec, not a drive-by. Revisit when a concrete
  "FastAPI + DLQ" or "FastAPI + recorder seam" demand surfaces.
  (`faststream_outbox/fastapi/router.py`)
