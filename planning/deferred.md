# Deferred Work

Items raised in reviews or audits that are real but not actionable now.
Each is parked here with the reason it's deferred and the concrete trigger
that should bring it back. This is the long-tail register — not a backlog
of planned work. When an item is picked up it graduates to a spec/plan
bundle in [`changes/`](changes/); see [CLAUDE.md](../CLAUDE.md#workflow).

As of the 2026-06-12 code + docs audit closure (PRs #61–#74), the audit
backlog is empty. The items below are the remainder: technically real,
but deliberately unscheduled pending a trigger.

## Open

### FastAPI integration

- **`OutboxRouter` doesn't forward `routers`.** `dlq_table` and
  `metrics_recorder` now forward to the inner broker (pass-3 audit F8-01,
  PR #88) — a FastAPI user can enable the DLQ and the recorder seam through
  the router. The remaining unforwarded `OutboxBroker.__init__` argument is
  `routers`: its semantics through the FastAPI lifespan are unsettled
  (the router contributes its own subscribers via `app.include_router`, so
  a separate `routers` sequence's start/AsyncAPI behavior needs a design
  call). Subscribers can be registered directly on the `OutboxRouter`
  instead. Revisit if a concrete "include a sub-router under the FastAPI
  outbox router" demand surfaces. (`faststream_outbox/fastapi/router.py`)
