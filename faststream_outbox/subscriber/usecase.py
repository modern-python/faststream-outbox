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
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager, suppress
from itertools import chain

import anyio
from faststream._internal.endpoint.subscriber import SubscriberSpecification, SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream.exceptions import StopConsume, SubscriberNotFound
from faststream.response.utils import ensure_response
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.parser.parser import OutboxParser
from faststream_outbox.publisher.fake import OutboxFakePublisher
from faststream_outbox.response import OutboxResponse
from faststream_outbox.subscriber.config import OutboxSubscriberConfig, OutboxSubscriberSpecificationConfig


try:
    import asyncpg as _asyncpg
except ImportError:  # pragma: no cover
    _asyncpg = None  # ty: ignore[invalid-assignment]


_BACKOFF_EXP_CAP = 30
_BACKOFF_MAX_SECONDS = 30.0


# Marker exception raised by programming guards inside ``process_message`` (e.g.
# the OutboxResponse + foreign-publisher dual-fire guard). Inherits from
# ``RuntimeError`` so ``pytest.raises(RuntimeError, ...)`` in tests catches it, but
# is a distinct subclass so ``consume()`` and ``dispatch_one`` can re-raise it while
# still swallowing plain handler-raised ``RuntimeError``s.
class _OutboxConfigError(RuntimeError): ...


# Cap for the ``last_exception`` string written to the DLQ. Some exceptions carry
# huge payloads (validation errors with the full request body, asyncpg ``DataError``
# with the rejected row, etc.). An unbounded ``repr`` would extend the writer
# round-trip on a poison row by hundreds of ms and bloat the DLQ table. 8 KiB is
# generous enough to keep tracebacks and structured detail intact while bounding
# worst-case write cost. Truncation appends ``…[truncated]``.
_LAST_EXCEPTION_MAX_CHARS = 8192
_TRUNCATION_SUFFIX = "…[truncated]"

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


def _truncate_exception(exc: BaseException | None) -> str | None:
    """Render *exc* via ``repr`` and bound it to ``_LAST_EXCEPTION_MAX_CHARS``."""
    if exc is None:
        return None
    rendered = repr(exc)
    if len(rendered) <= _LAST_EXCEPTION_MAX_CHARS:
        return rendered
    keep = _LAST_EXCEPTION_MAX_CHARS - len(_TRUNCATION_SUFFIX)
    return rendered[:keep] + _TRUNCATION_SUFFIX


if typing.TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage
    from faststream.response.response import Response
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
        # Set by stop() to halt _fetch_inner before tasks are cancelled. Distinct
        # from self.running: running must stay True during drain so FastStream's
        # SubscriberUsecase.consume() doesn't early-exit on rows already in _inflight.
        self._stopping: bool = False

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
        # Strict-bound drain. We intentionally DON'T call super().stop() because
        # SubscriberUsecase.stop's MultiLock.wait_release(graceful_timeout) would
        # either return instantly (healthy path; we already waited via
        # _inflight.join, which is a stricter wait — it covers the whole
        # dispatch_one including the handler that holds the MultiLock entry) or
        # re-wait the same stuck handlers for another full budget (wedged path;
        # 2x regression vs today). Inline TasksMixin's cleanup instead.
        #
        # Why two flags: flipping running=False first would defeat drain via
        # SubscriberUsecase.consume()'s early-exit (queued rows would skip the
        # handler). Halt fetch with _stopping, kick idle sleep with _notify_event,
        # drain _inflight within graceful budget, then flip running and cancel
        # any stragglers.
        #
        # Upstream equivalent (replaced):
        #   TasksMixin.stop -> faststream/_internal/endpoint/subscriber/mixins.py
        #   SubscriberUsecase.stop -> faststream/_internal/endpoint/subscriber/usecase.py
        self._stopping = True
        self._notify_event.set()
        with anyio.move_on_after(self._outer_config.graceful_timeout):
            await self._inflight.join()
        self.running = False
        for task in self.tasks:
            if not task.done():
                task.cancel()
        self.tasks.clear()

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
        while self.running and not self._stopping:
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
        except _OutboxConfigError:
            # _OutboxConfigError escaping consume() is a programming guard (e.g.
            # the OutboxResponse + foreign-publisher dual-fire guard). Re-raise so
            # callers see the error immediately — sync test dispatch surfaces it
            # from outbox.publish; the worker loop's outer except re-raises it via
            # _run_with_reconnect so it's logged at ERROR level with a reconnect
            # backoff rather than silently ignored.
            raise
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
        # Branch on ``terminal_failure_reason`` (set by ``_nack``/``_reject``) before
        # ``last_exception`` so manual ``msg.reject()`` (no exception raised) still
        # surfaces as ``nacked_terminal(reason="rejected")``, and ack-policy-driven
        # rejects (REJECT_ON_ERROR) carry ``reason="rejected"`` rather than the
        # incorrect ``"retry_terminal"`` they got under the old ``last_exception``-first
        # ordering. Successful handlers leave ``terminal_failure_reason=None``.
        if row.terminal_failure_reason is not None:
            terminal_tags: dict[str, typing.Any] = {**common, "reason": row.terminal_failure_reason}
            if row.last_exception is not None:
                terminal_tags["exception_type"] = type(row.last_exception).__name__
            self._emit_metric("nacked_terminal", terminal_tags)
        elif row.pending_delay_seconds is not None:
            retry_tags: dict[str, typing.Any] = {**common, "next_delay_seconds": row.pending_delay_seconds}
            if row.last_exception is not None:
                retry_tags["exception_type"] = type(row.last_exception).__name__
            self._emit_metric("nacked_retried", retry_tags)
        else:
            self._emit_metric("acked", common)
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
        # Build the DLQ payload only when this row is terminal-by-failure AND the
        # broker is configured with a DLQ table. Success-by-ack rows reach this
        # method too (terminal=True via to_delete) but carry
        # ``terminal_failure_reason is None`` and must not land in the DLQ.
        dlq_payload: dict[str, typing.Any] | None = None
        if row.terminal_failure_reason is not None and self._outer_config.dlq_table is not None:
            dlq_payload = {
                "failure_reason": row.terminal_failure_reason,
                "last_exception": _truncate_exception(row.last_exception),
            }
        deleted = await self._client.delete_with_lease(
            writer_conn,
            row.id,
            row.acquired_token,
            dlq_payload=dlq_payload,
        )
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
            return
        if dlq_payload is not None:
            self._emit_metric(
                "dlq_written",
                {
                    **self._base_tags(row.queue),
                    "deliveries_count": row.deliveries_count,
                    "failure_reason": row.terminal_failure_reason,
                    "exception_type": (type(row.last_exception).__name__ if row.last_exception is not None else None),
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

    @typing.override
    async def consume(self, msg: OutboxInnerMessage) -> typing.Any:
        """
        Override to propagate ``_OutboxConfigError`` from programming guards.

        ``SubscriberUsecase.consume`` swallows all ``Exception`` subclasses except
        ``StopConsume`` / ``SystemExit``. Programming guards (e.g. the
        ``OutboxResponse + foreign-publisher`` dual-fire check in
        ``process_message``) raise ``_OutboxConfigError`` (a ``RuntimeError``
        subclass) so the guard is distinguishable from handler-raised
        ``RuntimeError``s. Re-raising here lets ``dispatch_one`` surface it to
        the caller (sync test dispatch exposes it from ``outbox.publish``; the
        worker loop re-raises so the error is logged and the lease expires rather
        than silently swallowing a configuration mistake).

        # Upstream equivalent (extended):
        #   SubscriberUsecase.consume
        #   -> faststream/_internal/endpoint/subscriber/usecase.py
        """
        if not self.running:
            return None
        try:
            return await self.process_message(msg)
        except _OutboxConfigError:
            raise
        except StopConsume:  # pragma: no cover
            # Upstream-mirrored; outbox handlers do not raise StopConsume.
            await self.stop()
        except SystemExit:  # pragma: no cover
            # Upstream-mirrored; outbox handlers do not raise SystemExit.
            await self.stop()
            if app := self._outer_config.fd_config.context.get("app"):
                app.exit()
        except Exception:  # noqa: BLE001, S110
            # All other exceptions were logged by CriticalLogMiddleware
            pass
        return None

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

    @typing.override
    async def process_message(self, msg: OutboxInnerMessage) -> "Response":  # noqa: C901
        """
        Outbox-specific process_message — header propagation (G3) hook.

        Optionally fills empty Response headers with the inbound message's
        headers when ``propagate_inbound_headers=True``. Task 5 adds the
        OutboxResponse + foreign-publisher dual-fire guard (G1) here too.

        # Upstream equivalent (replaced):
        #   SubscriberUsecase.process_message
        #   -> faststream/_internal/endpoint/subscriber/usecase.py

        Divergence from upstream is strictly additive — the chain composition,
        middleware ordering, parsing-error rethrow, and AckPolicy semantics are
        preserved verbatim. Any new cleanup added upstream to process_message
        must be mirrored here.
        """
        context = self._outer_config.fd_config.context
        logger_state = self._outer_config.logger

        async with AsyncExitStack() as stack:
            stack.enter_context(self.lock)
            stack.enter_context(context.scope("handler_", self))
            stack.enter_context(context.scope("logger", logger_state.logger.logger))
            for k, v in self._outer_config.extra_context.items():
                stack.enter_context(context.scope(k, v))

            middlewares: list[typing.Any] = []
            for base_m in self._SubscriberUsecase__build__middlewares_stack():  # ty: ignore[unresolved-attribute]
                middleware = base_m(msg, context=context)
                middlewares.append(middleware)
                await middleware.__aenter__()

            cache: dict[typing.Any, typing.Any] = {}
            parsing_error: Exception | None = None
            for h in self.calls:
                try:
                    message = await h.is_suitable(msg, cache)
                except Exception as e:  # noqa: BLE001  # pragma: no cover
                    # Upstream-mirrored; OutboxParser does not raise from is_suitable.
                    parsing_error = e
                    break

                if message is not None:
                    stack.enter_context(
                        context.scope("log_context", self.get_log_context(message)),
                    )
                    stack.enter_context(context.scope("message", message))

                    for m in middlewares:
                        stack.push_async_exit(m.__aexit__)

                    result_msg = ensure_response(
                        await h.call(
                            message=message,
                            _extra_middlewares=(m.consume_scope for m in middlewares[::-1]),
                        ),
                    )

                    if not result_msg.correlation_id:
                        result_msg.correlation_id = message.correlation_id

                    if self._config.propagate_inbound_headers and not result_msg.headers:
                        result_msg.headers = dict(message.headers)

                    self._reject_outbox_response_with_foreign_publisher(result_msg, h.handler)

                    for p in chain(
                        self._SubscriberUsecase__get_response_publisher(message),  # ty: ignore[unresolved-attribute]
                        h.handler._publishers,  # noqa: SLF001
                    ):
                        await p._publish(  # noqa: SLF001
                            result_msg.as_publish_command(),
                            _extra_middlewares=(m.publish_scope for m in middlewares[::-1]),
                        )

                    return result_msg

            for m in middlewares:  # pragma: no cover
                # Upstream-mirrored no-matching-handler fall-through; the OutboxParser
                # always matches a handler so this branch is unreachable in normal flow.
                stack.push_async_exit(m.__aexit__)

            if parsing_error:  # pragma: no cover
                raise parsing_error  # pragma: no cover

            error_msg = f"There is no suitable handler for {msg=}"  # pragma: no cover
            raise SubscriberNotFound(error_msg)  # pragma: no cover

        return ensure_response(None)  # pragma: no cover

    @staticmethod
    def _reject_outbox_response_with_foreign_publisher(
        result_msg: "Response",
        handler: typing.Any,
    ) -> None:
        """
        Refuse the dual-fire combination: OutboxResponse + foreign publisher.

        OutboxResponse(body=..., queue=..., session=...) writes to the outbox in
        the caller's transaction; a foreign-publisher decorator also publishes
        the relayed body. Both would fire from the chain. That is almost
        certainly not intended — pick one.
        """
        if not isinstance(result_msg, OutboxResponse):
            return
        foreign = [
            p
            for p in handler._publishers  # noqa: SLF001
            if not isinstance(p, OutboxFakePublisher)
        ]
        if not foreign:
            return
        msg = (
            "Handler returned OutboxResponse and is also decorated by a foreign-broker "
            "publisher — this would dual-fire (insert a row into the outbox AND publish "
            "to the foreign broker). Pick one: return a plain value to use the foreign "
            "publisher as a relay, or remove the foreign publisher decorator and keep "
            "OutboxResponse for outbox fan-out."
        )
        raise _OutboxConfigError(msg)

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
