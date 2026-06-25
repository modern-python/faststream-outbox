# Integration: annotations, FastAPI router, engine ownership — implementation detail

User-facing: `docs/` (FastAPI integration). Invariant summary: `CLAUDE.md` § Integration.

## Annotations module (`annotations.py`)

This module is the canonical home for the `Annotated[..., Context(...)]` shortcuts — `OutboxMessage`, `OutboxBroker`, `OutboxProducer`, and `OutboxClient`. Each shortcut shadows its underlying class, which is imported via `from … import X as _X` so the public name can be re-bound to the `Annotated` form while the plain class stays available under its `_`-prefixed alias.

Two of the shortcuts resolve through non-obvious attribute paths:

- **Producer path**: `Context("broker._producer")`. This resolves via the `BrokerUsecase._producer` property, which returns `self.config.producer`.
- **Client path**: `Context("broker.config.broker_config.client")`. The client lives only on the outbox-specific config layer, so the shortcut points at it directly rather than through a broker property.

`faststream_outbox.fastapi` re-exports these shortcuts with a FastAPI-aware `Context` (sourced from `faststream._internal.fastapi.context`).

## FastAPI router (`fastapi/router.py`)

`OutboxRouter` subclasses FastStream's `StreamRouter`, which itself subclasses `APIRouter`. Calling `app.include_router(router)` auto-starts the inner `OutboxBroker` via the FastAPI lifespan.

This bridge is critical for the transactional contract. `wrap_callable_to_fastapi_compatible` (a FastStream internal) bridges FastAPI's dependency resolver into the consume pipeline, so a `Depends(get_session)` inside a handler resolves the same `AsyncSession` it would in an HTTP route — and `OutboxResponse(session=...)` commits the follow-on row together with the handler's domain writes.

`subscriber()` and `publisher()` are overridden to pin defaults for FastAPI-specific kwargs (such as `response_model=Default(None)`) that the base declares keyword-only without defaults. The outbox kwargs flow through unchanged.

`apply_types` and the broker `dependencies` are intentionally not exposed: `StreamRouter` forces `apply_types=False` (FastDepends takes over), and the broker's `Dependant` list isn't useful in this flow.

`fastapi` is an optional dependency (`faststream-outbox[fastapi]`).

## Engine ownership

The caller owns the `AsyncEngine` — the broker never disposes it. The engine lives on `OutboxBrokerConfig` (set by the broker constructor) and may be `None` until wired, so the broker can be constructed before the engine exists (used by the test broker).
