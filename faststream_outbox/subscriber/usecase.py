"""
Outbox subscriber — the consume loop that backs ``@broker.subscriber("queue")``.

Two async tasks run per subscriber:

* ``_fetch_loop`` claims available rows from Postgres and pushes them onto an
  in-process queue. A row is available iff its lease is unset *or* expired
  (``acquired_at < now() - lease_ttl_seconds``); the fetch CTE reclaims both cases
  in one round-trip. Adaptive idle backoff with jitter when the queue is empty —
  but the sleep is short-circuited by ``LISTEN/NOTIFY`` when the asyncpg driver
  is in use, dropping idle dispatch latency from up to ``max_fetch_interval`` to
  ~10ms. Polling stays as the fallback if ``LISTEN`` setup fails.
* ``_worker_loop`` (one per ``max_workers``) pulls rows, dispatches via
  ``consume()``, and writes the terminal state back through the client. Every
  terminal write is filtered by ``acquired_token`` so a row whose lease was
  reclaimed by a newer fetch doesn't get clobbered by the stale handler.

The fetch loop owns a long-lived ``AsyncConnection`` (used for the fetch CTE) and,
when asyncpg is available, a separate raw asyncpg connection dedicated to LISTEN.
On any error the connections are closed, the loop backs off, then both are reopened
in the next iteration.
"""

import asyncio
import logging
import random
import typing
from collections.abc import Sequence
from contextlib import suppress

import anyio
from faststream._internal.endpoint.subscriber import SubscriberSpecification, SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.parser.parser import OutboxParser
from faststream_outbox.subscriber.config import OutboxSubscriberConfig, OutboxSubscriberSpecificationConfig


try:
    import asyncpg as _asyncpg
except ImportError:  # pragma: no cover
    _asyncpg = None  # ty: ignore[invalid-assignment]


_BACKOFF_EXP_CAP = 30


if typing.TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from faststream_outbox.client import OutboxClient
    from faststream_outbox.configs import OutboxBrokerConfig


class OutboxSubscriberSpecification(SubscriberSpecification["OutboxBrokerConfig", OutboxSubscriberSpecificationConfig]):
    @property
    def name(self) -> str:
        joined = ",".join(self.config.queues)
        return f"{joined}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        return {
            self.name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(self.get_payloads()),
                    ),
                    bindings=None,
                ),
                bindings=None,
            )
        }


class OutboxSubscriber(TasksMixin, SubscriberUsecase[OutboxInnerMessage]):
    _outer_config: "OutboxBrokerConfig"

    def __init__(
        self,
        config: OutboxSubscriberConfig,
        specification: OutboxSubscriberSpecification,
        calls: "CallsCollection[typing.Any]",
    ) -> None:
        parser = OutboxParser()
        config.parser = parser.parse_message
        config.decoder = parser.decode_message
        super().__init__(config, specification, calls)
        self._config = config
        self._inflight: asyncio.Queue[OutboxInnerMessage] = asyncio.Queue(
            maxsize=config.fetch_batch_size,
        )
        # Set by the LISTEN callback to wake _fetch_loop early; cleared after each
        # wakeup. When LISTEN is unavailable the event simply never fires and the
        # loop sleeps the full adaptive interval.
        self._notify_event: asyncio.Event = asyncio.Event()

    @property
    def _client(self) -> "OutboxClient":
        client = self._outer_config.client
        if client is None:
            msg = "OutboxSubscriber is not connected; the broker has no client."
            raise RuntimeError(msg)
        return client

    @property
    def _queues(self) -> list[str]:
        return self._config.queues

    @typing.override
    async def start(self) -> None:
        await super().start()
        self._post_start()
        if not self.calls:
            return
        for _ in range(self._config.max_workers):
            self.add_task(self._worker_loop)
        self.add_task(self._fetch_loop)

    @typing.override
    async def stop(self) -> None:
        with anyio.move_on_after(self._outer_config.graceful_timeout):
            await super().stop()

    @property
    def _notify_channel(self) -> str:
        """
        LISTEN channel name.

        One channel per outbox table; subscribers ignore queues they don't care
        about (cheap — wake-up does an empty fetch and goes back to sleep).
        """
        return f"outbox_{self._client.table.name}"

    async def _fetch_loop(self) -> None:
        """
        Outer loop: own connection lifecycle, back off and reconnect on error.

        When the client has no real engine (test broker), drives the inner loop without
        a persistent connection — uses ``client.fetch(...)`` for each iteration and never
        sets up LISTEN. The ``_notify_event`` simply never fires; behavior is polling-only.
        """
        error_attempt = 0
        while self.running:
            # Read client lazily inside the loop: in the test broker path the client is
            # patched in/out via mock.patch, so it can be None after teardown. Returning
            # cleanly (rather than raising RuntimeError) prevents FastStream's supervisor
            # from restarting the task and leaking a pending coroutine at GC time.
            client = self._outer_config.client
            if client is None:  # pragma: no cover  # defensive teardown race; hard to deterministically hit
                return
            engine = client.engine
            try:
                if engine is None:
                    await self._fetch_inner(fetch_conn=None)
                else:
                    async with engine.connect() as fetch_conn:
                        listen_conn = await self._open_listen_connection(engine)
                        try:
                            await self._fetch_inner(fetch_conn=fetch_conn)
                        finally:
                            if listen_conn is not None:
                                await listen_conn.close()
            except Exception as e:  # noqa: BLE001
                self._log(
                    log_level=logging.ERROR,
                    message=f"Outbox fetch loop error: {e!r}; reconnecting",
                    exc_info=e,
                )
                error_attempt = min(error_attempt + 1, _BACKOFF_EXP_CAP)
                delay = min(2.0 ** (error_attempt - 1) * random.uniform(0.5, 1.5), 30.0)  # noqa: S311
                await anyio.sleep(delay)

    async def _fetch_inner(self, *, fetch_conn: "AsyncConnection | None") -> None:
        """
        Fetch + adaptive backoff, with NOTIFY-driven wakeup.

        Returns when ``self.running`` goes False, or raises on any DB error so the outer
        loop can rebuild the connection.
        """
        base = self._config.min_fetch_interval
        max_idle = self._config.max_fetch_interval
        idle_count = 0
        while self.running:
            free = self._inflight.maxsize - self._inflight.qsize()
            if free <= 0:
                await self._wait_for_notify_or_timeout(base)
                continue
            limit = min(free, self._config.fetch_batch_size)
            if fetch_conn is None:
                rows = await self._client.fetch(
                    self._queues,
                    limit=limit,
                    lease_ttl_seconds=self._config.lease_ttl_seconds,
                )
            else:
                rows = await self._client.fetch_with_conn(
                    fetch_conn,
                    self._queues,
                    limit=limit,
                    lease_ttl_seconds=self._config.lease_ttl_seconds,
                )
            if rows:
                idle_count = 0
                for row in rows:
                    await self._inflight.put(row)
            else:
                idle_count = min(idle_count + 1, _BACKOFF_EXP_CAP)
                delay = min(base * (2.0 ** (idle_count - 1)) * random.uniform(0.5, 1.5), max_idle)  # noqa: S311
                await self._wait_for_notify_or_timeout(delay)

    async def _wait_for_notify_or_timeout(self, timeout: float) -> None:  # noqa: ASYNC109
        """Sleep up to *timeout* seconds, but wake immediately on a NOTIFY."""
        with suppress(TimeoutError):
            await asyncio.wait_for(self._notify_event.wait(), timeout=timeout)
        self._notify_event.clear()

    async def _open_listen_connection(self, engine: "AsyncEngine") -> "_asyncpg.Connection | None":
        """
        Open a dedicated raw asyncpg connection and register LISTEN on it.

        Returns the connection on success, ``None`` on any failure (asyncpg not installed,
        non-asyncpg driver, permission error, network problem). The fetch loop falls back
        to polling-only behavior in that case.

        A separate connection is required because asyncpg's ``add_listener`` makes the
        connection's reader task monopolize it — interleaving normal queries breaks
        notification delivery.
        """
        if _asyncpg is None or "asyncpg" not in (engine.url.drivername or ""):
            return None
        # SQLAlchemy URL with the +asyncpg suffix isn't a valid raw asyncpg DSN; strip it.
        # ``str(url)`` hides the password — use ``render_as_string(hide_password=False)``
        # so asyncpg.connect actually sees the credentials.
        dsn = engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
        try:
            conn = await _asyncpg.connect(dsn)
            await conn.add_listener(self._notify_channel, self._on_notify)
        except Exception as e:  # noqa: BLE001
            self._log(
                log_level=logging.WARNING,
                message=f"LISTEN setup failed; falling back to polling: {e!r}",
                exc_info=e,
            )
            return None
        return conn

    def _on_notify(self, *_args: object) -> None:
        """
        Asyncpg notification callback: ``(connection, pid, channel, payload)``.

        We only need the wake-up signal; payload is ignored. Setting an ``asyncio.Event``
        from the asyncpg reader task is safe — it runs on the same event loop.
        """
        self._notify_event.set()

    async def _worker_loop(self) -> None:
        while self.running:
            row = await self._inflight.get()
            try:
                await self.dispatch_one(row)
            finally:
                self._inflight.task_done()

    async def dispatch_one(self, row: OutboxInnerMessage) -> None:
        """
        Run a single already-leased row through the full consume pipeline.

        Mirrors the per-row body of ``_worker_loop`` so ``TestOutboxBroker`` can drive
        the handler synchronously from ``broker.publish``, matching the FastStream
        test-broker idiom (``TestKafkaBroker`` / ``TestRabbitBroker``). The caller is
        responsible for having acquired the row's lease before invoking this.
        """
        logger = self._outer_config.logger.logger.logger if self._outer_config.logger else None
        try:
            row.retry_strategy = self._config.retry_strategy
            if not row.allow_delivery(max_deliveries=self._config.max_deliveries, logger=logger):
                await self._flush_terminal(row)
                return
            # AckPolicy middleware catches handler exceptions; _CaptureExceptionMiddleware
            # stashes exc onto row.last_exception before nack runs, so retry strategies
            # can branch on exception type.
            try:
                await self.consume(row)
            finally:
                await row.assert_state_set(logger)
            await self._flush_result(row)
        except Exception as e:  # noqa: BLE001
            self._log(log_level=logging.ERROR, message=f"Outbox worker error: {e!r}", exc_info=e)

    async def _flush_result(self, row: OutboxInnerMessage) -> None:
        if row.to_delete:
            await self._flush_terminal(row)
        else:
            await self._flush_retry(row)

    async def _flush_terminal(self, row: OutboxInnerMessage) -> None:
        if row.acquired_token is None:
            return
        deleted = await self._client.delete_with_lease(row.id, row.acquired_token)
        if not deleted:
            self._log(
                log_level=logging.INFO,
                message=f"Outbox row {row} lease expired before delete; skipping",
            )

    async def _flush_retry(self, row: OutboxInnerMessage) -> None:
        if row.acquired_token is None or row.pending_delay_seconds is None:
            return
        updated = await self._client.mark_pending_with_lease(
            row.id,
            row.acquired_token,
            delay_seconds=row.pending_delay_seconds,
            attempts_count=row.attempts_count,
            first_attempt_at=row.first_attempt_at,  # ty: ignore[invalid-argument-type]
            last_attempt_at=row.last_attempt_at,  # ty: ignore[invalid-argument-type]
        )
        if not updated:
            self._log(
                log_level=logging.INFO,
                message=f"Outbox row {row} lease expired before retry update; skipping",
            )

    @typing.override
    async def get_one(self, *, timeout: float = 5.0) -> typing.NoReturn:
        msg = "OutboxBroker does not support get_one()"
        raise NotImplementedError(msg)

    def _make_response_publisher(
        self,
        message: "StreamMessage[OutboxInnerMessage]",  # noqa: ARG002
    ) -> Sequence["PublisherProto"]:
        return ()

    def get_log_context(
        self,
        message: "StreamMessage[OutboxInnerMessage] | None",
    ) -> dict[str, str]:
        with suppress(Exception):
            if message and message.raw_message:
                return {
                    "queue": message.raw_message.queue,
                    "message_id": getattr(message, "message_id", ""),
                }
        return {"queue": ",".join(self._config.queues), "message_id": ""}
