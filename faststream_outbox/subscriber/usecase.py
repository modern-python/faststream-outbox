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
import time
import typing
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress

import anyio
from faststream._internal.endpoint.subscriber import SubscriberSpecification, SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.parser.parser import OutboxParser
from faststream_outbox.publisher.fake import OutboxFakePublisher
from faststream_outbox.subscriber.config import OutboxSubscriberConfig, OutboxSubscriberSpecificationConfig


try:
    import asyncpg as _asyncpg
except ImportError:  # pragma: no cover
    _asyncpg = None  # ty: ignore[invalid-assignment]


_BACKOFF_EXP_CAP = 30
_BACKOFF_MAX_SECONDS = 30.0

_UNSUPPORTED_PEEK_MSG = (
    "OutboxBroker does not support get_one() / async iteration. "
    "Use `broker.fetch_unprocessed(session=..., queue=...)` for lease-free read access."
)
# Periodic probe of the LISTEN connection so silent drops (firewall RST, NAT idle timeout,
# asyncpg reader-task death) surface as exceptions and the outer loop reconnects. The
# bounded `wait_for` timeout is load-bearing: an unwrapped SELECT 1 against a half-dead
# TCP socket can hang on the kernel keepalive default (~2.5h on Linux) before failing.
_LISTEN_HEALTH_CHECK_INTERVAL = 30.0
_LISTEN_HEALTH_CHECK_TIMEOUT = 5.0


def _compute_backoff(attempt: int, ceiling: float, *, base: float = 1.0) -> float:
    """
    Exponential backoff with ±50% jitter, capped at *ceiling*.

    *attempt* is 1-based — the first attempt sleeps ~``base * U(0.5, 1.5)``.
    """
    return min(base * (2.0 ** (attempt - 1)) * random.uniform(0.5, 1.5), ceiling)  # noqa: S311


if typing.TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from faststream_outbox.client import AbstractOutboxClient
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
    def _client(self) -> "AbstractOutboxClient":
        client = self._outer_config.client
        if client is None:
            msg = "OutboxSubscriber is not connected; the broker has no client."
            raise RuntimeError(msg)
        return client

    @property
    def _queues(self) -> list[str]:
        return self._config.queues

    def _base_tags(self, queue: str) -> dict[str, typing.Any]:
        # ``self.specification.call_name`` is the subscriber's handler name (the
        # decorated function's ``__name__``); we expose it as the ``subscriber``
        # tag so adapters can map it to FastStream's ``handler`` label.
        return {"queue": queue, "subscriber": self.specification.call_name}

    def _emit_metric(self, event: str, tags: Mapping[str, typing.Any]) -> None:
        try:
            self._outer_config.metrics_recorder(event, tags)
        except Exception as exc:  # noqa: BLE001
            # Match the producer's swallow-and-log shape (with ``exc_info``) so
            # operators see the same traceback whether the recorder raised on a
            # subscriber or publisher event.
            self._log(
                log_level=logging.DEBUG,
                message="metrics recorder raised",
                exc_info=exc,
            )

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
        """Thin wrapper around :meth:`_run_with_reconnect` for the fetch path."""
        await self._run_with_reconnect(
            name="fetch",
            open_resources=self._open_fetch_resources,
            inner=self._fetch_inner,
        )

    @asynccontextmanager
    async def _open_fetch_resources(
        self,
        engine: "AsyncEngine | None",
    ) -> AsyncIterator[Mapping[str, object]]:
        """
        Yield the kwargs ``_fetch_inner`` needs, owning fetch_conn + listen_conn lifetimes.

        Production path opens a long-lived ``AsyncConnection`` for the fetch CTE and a
        separate raw asyncpg connection for LISTEN. Test-broker path (``engine is None``)
        skips both and lets ``_fetch_inner`` fall back to ``client.fetch(...)`` per tick.
        """
        if engine is None:
            yield {"fetch_conn": None, "listen_conn": None}
            return
        async with engine.connect() as fetch_conn:
            listen_conn = await self._open_listen_connection(engine)
            try:
                yield {"fetch_conn": fetch_conn, "listen_conn": listen_conn}
            finally:
                if listen_conn is not None:
                    await listen_conn.close()

    async def _fetch_inner(
        self,
        *,
        fetch_conn: "AsyncConnection | None",
        listen_conn: "_asyncpg.Connection | None",
    ) -> None:
        """
        Fetch + adaptive backoff, with NOTIFY-driven wakeup.

        Returns when ``self.running`` goes False, or raises on any DB error so the outer
        loop can rebuild the connection. Periodically probes ``listen_conn`` with a bounded
        ``SELECT 1`` so silent disconnects surface as exceptions rather than degrading
        dispatch latency to ``max_fetch_interval`` for the life of the process.
        """
        base = self._config.min_fetch_interval
        max_idle = self._config.max_fetch_interval
        idle_count = 0
        last_listen_check = time.monotonic()
        while self.running:
            if listen_conn is not None:
                now = time.monotonic()
                if now - last_listen_check >= _LISTEN_HEALTH_CHECK_INTERVAL:
                    await asyncio.wait_for(
                        listen_conn.fetchval("SELECT 1"),
                        timeout=_LISTEN_HEALTH_CHECK_TIMEOUT,
                    )
                    last_listen_check = now
            free = self._inflight.maxsize - self._inflight.qsize()
            if free <= 0:
                await self._wait_for_notify_or_timeout(base)
                continue
            limit = min(free, self._config.fetch_batch_size)
            # fetch_conn is None only in the test-broker path against FakeOutboxClient
            # (which ignores conn); the real OutboxClient raises if conn is None. The
            # AbstractOutboxClient surface admits None so both implementations type-check.
            rows = await self._client.fetch(
                fetch_conn,
                self._queues,
                limit=limit,
                lease_ttl_seconds=self._config.lease_ttl_seconds,
            )
            # Emit a fetched event per tick (count=0 on idle), tagged by the first
            # configured queue. Subscribers may listen to multiple queues; for the
            # tag we surface the primary one rather than fan out a tag-per-queue
            # batch — adapters that want a per-queue breakdown can use the
            # ``queue`` tag from row-level events instead.
            self._emit_metric(
                "fetched",
                {**self._base_tags(self._queues[0] if self._queues else ""), "count": len(rows)},
            )
            if rows:
                idle_count = 0
                for row in rows:
                    await self._inflight.put(row)
            else:
                idle_count = min(idle_count + 1, _BACKOFF_EXP_CAP)
                await self._wait_for_notify_or_timeout(_compute_backoff(idle_count, max_idle, base=base))

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
        # Delegate URL → asyncpg kwargs translation to SQLAlchemy's dialect so multi-host
        # URLs (``?host=h1:5432&host=h2:5432``) become ``host=[...], port=[...]``. Round-
        # tripping through ``render_as_string`` URL-encodes ``host:port`` into one token
        # asyncpg can't parse and fails for failover/replica clusters.
        _, opts = engine.dialect.create_connect_args(engine.url)
        for sa_only_key in ("prepared_statement_cache_size", "async_fallback", "async_creator_fn"):
            opts.pop(sa_only_key, None)
        try:
            conn = await _asyncpg.connect(**opts)
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
        """Thin wrapper around :meth:`_run_with_reconnect` for the worker path."""
        await self._run_with_reconnect(
            name="worker",
            open_resources=self._open_worker_resources,
            inner=self._worker_inner,
        )

    @asynccontextmanager
    async def _open_worker_resources(
        self,
        engine: "AsyncEngine | None",
    ) -> AsyncIterator[Mapping[str, object]]:
        """
        Yield ``writer_conn`` for ``_worker_inner``, owning its lifetime across all flushes.

        One long-lived ``AsyncConnection`` per outer reconnect cycle — every terminal/retry
        write reuses it, so a drain of N rows costs O(workers) pool checkouts, not O(rows).
        Test-broker path (``engine is None``) yields ``None`` and the worker takes the
        one-shot client-wrapper path.
        """
        if engine is None:
            yield {"writer_conn": None}
            return
        async with engine.connect() as writer_conn:
            # Each terminal/retry write is a single statement; the explicit per-row
            # BEGIN/COMMIT in delete_with_lease / mark_pending_with_lease would add
            # two Postgres round-trips per row with no benefit. Autocommit collapses
            # the per-row cost to one round-trip; the lease guard rides on the WHERE
            # clause, not the transaction wrapping it.
            autocommit_conn = await writer_conn.execution_options(isolation_level="AUTOCOMMIT")
            yield {"writer_conn": autocommit_conn}

    async def _run_with_reconnect(
        self,
        *,
        name: str,
        open_resources: Callable[["AsyncEngine | None"], AbstractAsyncContextManager[Mapping[str, object]]],
        inner: Callable[..., Awaitable[None]],
    ) -> None:
        """
        Reconnect-with-backoff scaffold shared by ``_fetch_loop`` and ``_worker_loop``.

        Reads the client lazily inside the loop (the test broker patches it in/out via
        ``mock.patch``, so it can be ``None`` after teardown — returning cleanly avoids
        leaking a pending coroutine via FastStream's task supervisor). On any exception
        from ``inner`` or resource open/close, logs, backs off (exponential with jitter,
        capped at ``_BACKOFF_MAX_SECONDS``), and reopens.
        """
        error_attempt = 0
        while self.running:
            client = self._outer_config.client
            if client is None:  # pragma: no cover  # defensive teardown race
                return
            try:
                async with open_resources(client.engine) as kwargs:
                    await inner(**kwargs)
            except Exception as e:  # noqa: BLE001
                self._log(
                    log_level=logging.ERROR,
                    message=f"Outbox {name} loop error: {e!r}; reconnecting",
                    exc_info=e,
                )
                error_attempt = min(error_attempt + 1, _BACKOFF_EXP_CAP)
                await anyio.sleep(_compute_backoff(error_attempt, _BACKOFF_MAX_SECONDS))

    async def _worker_inner(self, *, writer_conn: "AsyncConnection | None") -> None:
        """
        Pull rows from the inflight queue and dispatch each, threading *writer_conn* through.

        Returns when ``self.running`` goes False, or raises on any DB error from the
        terminal write so :meth:`_worker_loop` can rebuild the connection.
        """
        while self.running:
            row = await self._inflight.get()
            try:
                await self.dispatch_one(row, writer_conn=writer_conn)
            finally:
                self._inflight.task_done()

    async def dispatch_one(
        self,
        row: OutboxInnerMessage,
        *,
        writer_conn: "AsyncConnection | None" = None,
    ) -> None:
        """
        Run a single already-leased row through the full consume pipeline.

        Mirrors the per-row body of ``_worker_loop`` so ``TestOutboxBroker`` can drive
        the handler synchronously from ``broker.publish``, matching the FastStream
        test-broker idiom (``TestKafkaBroker`` / ``TestRabbitBroker``). The caller is
        responsible for having acquired the row's lease before invoking this.

        Handler exceptions are logged and swallowed. Terminal-write (flush) exceptions
        propagate when *writer_conn* is not None so :meth:`_worker_loop` can close &
        rebuild the cached connection — a poisoned writer connection would otherwise keep
        failing on every row. When *writer_conn* is None (test broker / one-shot dispatch),
        flush exceptions are swallowed like the legacy behavior since there's no shared
        connection to rebuild.
        """
        logger = self._outer_config.logger.logger.logger if self._outer_config.logger else None
        row.retry_strategy = self._config.retry_strategy
        base = self._base_tags(row.queue)
        if not row.allow_delivery(max_deliveries=self._config.max_deliveries, logger=logger):
            self._emit_metric(
                "nacked_terminal",
                {**base, "deliveries_count": row.deliveries_count, "reason": "max_deliveries"},
            )
            await self._safe_flush(row, terminal=True, writer_conn=writer_conn)
            return
        # AckPolicy middleware catches handler exceptions; _CaptureExceptionMiddleware
        # stashes exc onto row.last_exception before nack runs, so retry strategies
        # can branch on exception type. We still wrap to log any escapes (manual-ack
        # fallback that itself raises, etc.) so the dispatch contract is robust.
        self._emit_metric(
            "dispatched",
            {**base, "deliveries_count": row.deliveries_count, "size_bytes": len(row.payload)},
        )
        start_perf = time.perf_counter()
        try:
            await self.consume(row)
        except Exception as e:  # noqa: BLE001
            # No metric emitted here intentionally: the row was never marked
            # terminal/retry, so its state is undefined — flushing or emitting an
            # ack/nack would lie. The lease will expire and the row will be
            # reclaimed; the ERROR log is the operator signal.
            self._log(log_level=logging.ERROR, message=f"Outbox worker error: {e!r}", exc_info=e)
            return
        # Shutdown race: SubscriberUsecase.consume() returns None without invoking
        # process_message when self.running has been flipped to False by stop().
        # Detecting that here lets us preserve the row instead of falling through
        # to assert_state_set → reject() → _safe_flush → DELETE. The row's lease
        # expires after lease_ttl_seconds and is reclaimed on next start.
        if not row.state_set and not self.running:
            return
        await row.assert_state_set(logger)
        duration_seconds = time.perf_counter() - start_perf
        common = {**base, "deliveries_count": row.deliveries_count, "duration_seconds": duration_seconds}
        if row.last_exception is None:
            self._emit_metric("acked", common)
        elif row.pending_delay_seconds is not None:
            self._emit_metric(
                "nacked_retried",
                {
                    **common,
                    "next_delay_seconds": row.pending_delay_seconds,
                    "exception_type": type(row.last_exception).__name__,
                },
            )
        elif row.to_delete:
            self._emit_metric(
                "nacked_terminal",
                {
                    **common,
                    "reason": "retry_terminal",
                    "exception_type": type(row.last_exception).__name__,
                },
            )
        await self._safe_flush(row, terminal=row.to_delete, writer_conn=writer_conn)

    async def _safe_flush(
        self,
        row: OutboxInnerMessage,
        *,
        terminal: bool,
        writer_conn: "AsyncConnection | None",
    ) -> None:
        """
        Run the terminal/retry write, propagating errors only when ``writer_conn`` is set.

        When ``writer_conn`` is None (test broker / sync dispatch), flush errors are logged
        and swallowed — there's no shared connection to rebuild and the legacy test-broker
        contract is "publish never raises from a flush failure". When ``writer_conn`` is
        set, flush errors propagate so :meth:`_worker_loop` can close the poisoned
        connection and reopen a fresh one.
        """
        flush = self._flush_terminal if terminal else self._flush_retry
        if writer_conn is None:
            try:
                await flush(row, writer_conn=None)
            except Exception as e:  # noqa: BLE001
                self._log(log_level=logging.ERROR, message=f"Outbox worker error: {e!r}", exc_info=e)
            return
        await flush(row, writer_conn=writer_conn)

    async def _flush_terminal(
        self,
        row: OutboxInnerMessage,
        *,
        writer_conn: "AsyncConnection | None",
    ) -> None:
        if row.acquired_token is None:
            return
        deleted = await self._client.delete_with_lease(writer_conn, row.id, row.acquired_token)
        if not deleted:
            self._log(
                log_level=logging.WARNING,
                message=f"Outbox row {row} lease expired before delete; skipping",
                extra={
                    "event": "lease_lost",
                    "phase": "terminal",
                    "row_id": row.id,
                    "queue": row.queue,
                    "deliveries_count": row.deliveries_count,
                },
            )
            self._emit_metric(
                "lease_lost",
                {
                    **self._base_tags(row.queue),
                    "phase": "terminal",
                    "row_id": row.id,
                    "deliveries_count": row.deliveries_count,
                },
            )

    async def _flush_retry(
        self,
        row: OutboxInnerMessage,
        *,
        writer_conn: "AsyncConnection | None",
    ) -> None:
        if row.acquired_token is None or row.pending_delay_seconds is None:
            return
        updated = await self._client.mark_pending_with_lease(
            writer_conn,
            row.id,
            row.acquired_token,
            delay_seconds=row.pending_delay_seconds,
            attempts_count=row.attempts_count,
            first_attempt_at=row.first_attempt_at,  # ty: ignore[invalid-argument-type]
            last_attempt_at=row.last_attempt_at,  # ty: ignore[invalid-argument-type]
        )
        if not updated:
            self._log(
                log_level=logging.WARNING,
                message=f"Outbox row {row} lease expired before retry update; skipping",
                extra={
                    "event": "lease_lost",
                    "phase": "retry",
                    "row_id": row.id,
                    "queue": row.queue,
                    "deliveries_count": row.deliveries_count,
                },
            )
            self._emit_metric(
                "lease_lost",
                {
                    **self._base_tags(row.queue),
                    "phase": "retry",
                    "row_id": row.id,
                    "deliveries_count": row.deliveries_count,
                },
            )

    @typing.override
    async def get_one(self, *, timeout: float = 5.0) -> typing.NoReturn:
        raise NotImplementedError(_UNSUPPORTED_PEEK_MSG)

    @typing.override
    async def __aiter__(self) -> AsyncIterator["StreamMessage[OutboxInnerMessage]"]:
        # Native FakeStream subscribers (e.g. redis ListSubscriber.__aiter__) implement
        # this against a blocking pop; for the outbox, a true peek would acquire a lease
        # and bump deliveries_count — surprising semantics for a "look but don't touch"
        # API. Route operators at ``broker.fetch_unprocessed`` instead, which is
        # lease-free and doesn't mutate row state. Matches the base's no-yield shape so
        # the override stays a coroutine returning AsyncIterator (not an async generator).
        raise NotImplementedError(_UNSUPPORTED_PEEK_MSG)

    def _make_response_publisher(
        self,
        message: "StreamMessage[OutboxInnerMessage]",  # noqa: ARG002
    ) -> Sequence["PublisherProto"]:
        # OutboxFakePublisher gates internally on ``isinstance(cmd, OutboxPublishCommand)``,
        # so plain handler returns (None, dict, etc.) become no-ops here. Only handlers
        # that explicitly ``return OutboxResponse(...)`` produce a published row.
        # ty diagnostic: OutboxFakePublisher implements ``_publish`` (the only method
        # SubscriberUsecase.process_message calls on a response publisher) but skips
        # the ``publish``/``request`` boilerplate from the native FakePublisher base.
        # Safe to ignore — those methods are unreachable for response publishers.
        return (OutboxFakePublisher(producer=self._outer_config.producer),)  # ty: ignore[invalid-return-type]

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
