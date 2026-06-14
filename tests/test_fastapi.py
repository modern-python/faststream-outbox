"""
Tests for the FastAPI integration (``faststream_outbox.fastapi.OutboxRouter``).

Exercises the canonical lifecycle: build a FastAPI app, mount the router,
publish through the router's inner broker, and verify subscribers fire. The
``OutboxRouter`` test path uses ``TestOutboxBroker(router.broker)`` to swap in
the in-memory fake client during the FastAPI lifespan, mirroring the existing
``test_fake.py`` shape. ``Depends(get_session)`` integration is verified by
yielding a session-shaped mock from a normal FastAPI dependency.
"""

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from faststream.middlewares import AckPolicy
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox import NoRetry, OutboxMessage, OutboxResponse, make_dlq_table, make_outbox_table
from faststream_outbox.client import AbstractOutboxClient
from faststream_outbox.fastapi import (
    OutboxBroker as AnnotatedOutboxBroker,
)
from faststream_outbox.fastapi import (
    OutboxClient as AnnotatedOutboxClient,
)
from faststream_outbox.fastapi import (
    OutboxMessage as AnnotatedOutboxMessage,
)
from faststream_outbox.fastapi import (
    OutboxProducer as AnnotatedOutboxProducer,
)
from faststream_outbox.fastapi import (
    OutboxRouter,
)
from faststream_outbox.testing import FakeOutboxProducer, TestOutboxBroker


def _make_outbox_table() -> Any:
    return make_outbox_table(MetaData())


def _make_app_with_router(router: OutboxRouter) -> FastAPI:
    """Build a FastAPI app mounted with the router; wrap the broker in TestOutboxBroker."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        del app
        # Swap in the in-memory fake client for the broker the router owns,
        # then let the router's own lifespan run (it starts subscribers).
        async with TestOutboxBroker(router.broker):
            yield

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    return app


async def test_subscriber_registered_via_outbox_router_runs_when_published() -> None:
    """Mount router, register subscriber, publish; handler fires inside FastAPI lifespan."""
    t = _make_outbox_table()
    router = OutboxRouter(outbox_table=t)
    received: list[dict] = []

    @router.subscriber("orders")
    async def handle(body: dict) -> None:
        received.append(body)

    app = _make_app_with_router(router)
    with TestClient(app):
        await router.broker.publish({"x": 1}, queue="orders")  # ty: ignore[missing-argument]

    assert received == [{"x": 1}]


def test_subscriber_misconfig_warning_attributed_to_user_via_fastapi_router() -> None:
    """
    P27: through the FastAPI router (extra frames) the misconfig warning points at the user's call site.

    Old static ``stacklevel=4`` landed on a faststream-internal frame on this path; the
    ``skip_file_prefixes`` attribution lands on the user's ``@router.subscriber(...)`` line.
    """
    router = OutboxRouter(outbox_table=_make_outbox_table())
    with pytest.warns(UserWarning, match="NACK_ON_ERROR") as record:

        @router.subscriber("orders", ack_policy=AckPolicy.NACK_ON_ERROR, retry_strategy=NoRetry())
        async def handle(body: dict) -> None: ...

    assert record[0].filename == __file__  # attributed to this test (the user), not a package frame


async def test_subscriber_receives_fastapi_depends_session() -> None:
    """B-load-bearing: ``Depends(get_session)`` resolves inside an outbox subscriber handler."""
    t = _make_outbox_table()
    router = OutboxRouter(outbox_table=t)

    sentinel_session = AsyncMock(spec=AsyncSession)
    seen_session: list[AsyncSession] = []

    async def get_session() -> AsyncIterator[AsyncSession]:
        yield sentinel_session

    session_dep = Depends(get_session)

    @router.subscriber("orders")
    async def handle(
        body: dict,
        session: AsyncSession = session_dep,
    ) -> None:
        del body
        seen_session.append(session)

    app = _make_app_with_router(router)
    with TestClient(app):
        await router.broker.publish({"x": 1}, queue="orders")  # ty: ignore[missing-argument]

    assert seen_session == [sentinel_session]


async def test_http_and_subscriber_routes_coexist_on_router() -> None:
    """The router serves both HTTP endpoints and outbox subscribers from one instance."""
    t = _make_outbox_table()
    router = OutboxRouter(outbox_table=t)
    subscriber_fired: list[str] = []

    @router.get("/health")
    def health() -> dict:
        return {"ok": True}

    @router.subscriber("orders")
    async def handle(body: str) -> None:
        subscriber_fired.append(body)

    app = _make_app_with_router(router)
    with TestClient(app) as client:
        # HTTP route works
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

        # Subscriber works through the same router
        await router.broker.publish("via-subscriber", queue="orders")  # ty: ignore[missing-argument]

    assert subscriber_fired == ["via-subscriber"]


async def test_annotated_context_shortcuts_resolve_in_router_handler() -> None:
    """``OutboxBroker``/``OutboxMessage``/``OutboxProducer``/``OutboxClient`` annotations injected."""
    t = _make_outbox_table()
    router = OutboxRouter(outbox_table=t)
    captured: dict[str, object] = {}

    @router.subscriber("orders")
    async def handle(
        body: dict,
        msg: AnnotatedOutboxMessage,
        broker: AnnotatedOutboxBroker,
        producer: AnnotatedOutboxProducer,
        client: AnnotatedOutboxClient,
    ) -> None:
        del body
        captured["msg"] = msg
        captured["broker"] = broker
        captured["producer"] = producer
        captured["client"] = client

    app = _make_app_with_router(router)
    with TestClient(app):
        await router.broker.publish({"x": 1}, queue="orders")  # ty: ignore[missing-argument]

    assert isinstance(captured["msg"], OutboxMessage)
    assert captured["broker"] is router.broker
    # Test broker swaps the producer slot with FakeOutboxProducer during the lifespan.
    assert isinstance(captured["producer"], FakeOutboxProducer)
    assert isinstance(captured["client"], AbstractOutboxClient)


def test_outbox_router_construction_requires_outbox_table() -> None:
    """``outbox_table`` is keyword-only and required (mirrors ``OutboxBroker.__init__``)."""
    with pytest.raises(TypeError, match="outbox_table"):
        OutboxRouter()  # type: ignore[call-arg]  # ty: ignore[missing-argument]


async def test_outbox_router_publisher_delegates_to_broker() -> None:
    """``OutboxRouter.publisher`` is a typed forwarder onto ``router.broker.publisher``."""
    t = _make_outbox_table()
    router = OutboxRouter(outbox_table=t)

    publisher = router.publisher("orders", headers={"static": "h"})
    assert publisher.queue == "orders"

    app = _make_app_with_router(router)
    with TestClient(app):
        row_id = await publisher.publish({"v": 1}, session=AsyncMock(spec=AsyncSession))

    assert row_id is not None


async def test_fastapi_handler_chains_via_outbox_response_with_per_delivery_session() -> None:
    """
    Exercise the transactional contract end-to-end through the FastAPI wrapper.

    The Depends-resolved session flows into the chained OutboxResponse, and each delivery
    resolves its own fresh session (session-per-delivery).
    """
    t = _make_outbox_table()
    router = OutboxRouter(outbox_table=t)
    sessions_seen: list[AsyncSession] = []
    downstream: list[dict] = []

    async def get_session() -> AsyncIterator[AsyncSession]:
        s = AsyncMock(spec=AsyncSession)
        sessions_seen.append(s)
        yield s

    session_dep = Depends(get_session)

    @router.subscriber("orders")
    async def handle_order(body: dict, session: AsyncSession = session_dep) -> OutboxResponse:
        return OutboxResponse(body={"chained_from": body["id"]}, queue="downstream", session=session)

    @router.subscriber("downstream")
    async def handle_downstream(body: dict) -> None:
        downstream.append(body)

    app = _make_app_with_router(router)
    with TestClient(app):
        await router.broker.publish({"id": 1}, queue="orders")  # ty: ignore[missing-argument]
        await router.broker.publish({"id": 2}, queue="orders")  # ty: ignore[missing-argument]

    # OutboxResponse chaining works through the FastAPI wrapper: the bridged Depends session is
    # the one the follow-on row is published with.
    assert downstream == [{"chained_from": 1}, {"chained_from": 2}]
    # Session-per-delivery: each "orders" delivery resolved its own fresh session via Depends.
    assert len(sessions_seen) == 2
    assert sessions_seen[0] is not sessions_seen[1]


def test_outbox_router_forwards_broker_kwargs_to_inner_broker() -> None:
    """End-to-end forwarding: an outbox-broker kwarg passed to OutboxRouter reaches the broker."""
    router = OutboxRouter(outbox_table=_make_outbox_table(), graceful_timeout=3.5)
    assert router.broker.config.graceful_timeout == 3.5


async def test_outbox_router_forwards_dlq_table_and_metrics_recorder_to_inner_broker() -> None:
    """F8-01: dlq_table + metrics_recorder reach the inner broker (DLQ + recorder seam usable under FastAPI)."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    dlq = make_dlq_table(metadata)
    events: list[str] = []

    def recorder(event: str, tags: Mapping[str, Any]) -> None:
        del tags
        events.append(event)

    router = OutboxRouter(outbox_table=t, dlq_table=dlq, metrics_recorder=recorder)

    @router.subscriber("orders")
    async def handle(body: dict) -> None:
        del body

    # Both kwargs reached the inner broker...
    assert router.broker._dlq_table is dlq  # noqa: SLF001
    assert router.broker.config.metrics_recorder is recorder

    # ...and the forwarded recorder actually fires on the inner broker's dispatch path.
    app = _make_app_with_router(router)
    with TestClient(app):
        await router.broker.publish({"x": 1}, queue="orders")  # ty: ignore[missing-argument]

    assert events  # the recorder seam is live under the FastAPI router
