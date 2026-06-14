"""
OutboxBroker — a FastStream broker whose queue is a Postgres table.

Producers call ``broker.publish(body, queue=..., session=session)`` inside their
own SQLAlchemy transaction; the row commits with their domain writes. The broker
owns subscribers on the consumer side.
"""

import asyncio
import datetime as _dt
import logging
import typing
import warnings
from collections.abc import Iterable, Sequence
from types import TracebackType

import anyio
from faststream import BaseMiddleware
from faststream._internal.basic_types import LoggerProto
from faststream._internal.broker import BrokerUsecase
from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream._internal.di import FastDependsConfig
from faststream._internal.logger import DefaultLoggerStorage, make_logger_state
from faststream._internal.logger.logging import get_broker_logger
from faststream._internal.types import BrokerMiddleware, CustomCallable
from faststream.exceptions import IncorrectState
from faststream.specification.schema import BrokerSpec
from faststream.specification.schema.extra import Tag, TagDict
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox._time import utcnow
from faststream_outbox.client import AbstractOutboxClient, OutboxClient, _row_to_message
from faststream_outbox.configs import OutboxBrokerConfig
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.metrics import MetricsRecorder, _noop_recorder
from faststream_outbox.publisher.producer import OutboxProducer
from faststream_outbox.registrator import OutboxRegistrator
from faststream_outbox.response import _REQUEST_UNSUPPORTED_MSG, OutboxPublishCommand, _validate_publish_args


_logger = logging.getLogger(__name__)


if typing.TYPE_CHECKING:
    from weakref import WeakSet

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.context.repository import ContextRepo
    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncEngine

    from faststream_outbox.subscriber.usecase import OutboxSubscriber


def _validate_activate_args(
    method_name: str,
    activate_in: _dt.timedelta | None,
    activate_at: _dt.datetime | None,
) -> None:
    """Mutex + tz-aware checks shared by the test fakes. Real broker delegates to ``OutboxPublishCommand``."""
    if activate_in is not None and activate_at is not None:
        msg = f"{method_name} accepts at most one of activate_in / activate_at"
        raise ValueError(msg)
    if activate_at is not None and activate_at.tzinfo is None:
        msg = f"{method_name} requires activate_at to be timezone-aware"
        raise ValueError(msg)


def _compute_next_at_client_side(
    activate_in: _dt.timedelta | None,
    activate_at: _dt.datetime | None,
) -> _dt.datetime | None:
    """Resolve activate_in / activate_at to a single ``next_attempt_at`` value (client clock)."""
    if activate_in is not None:
        return utcnow() + activate_in
    return activate_at


def _spec_url(engine: "AsyncEngine | None", outbox_table: "Table") -> list[str]:
    """
    AsyncAPI server URL(s) for the broker spec.

    **Must be non-empty.** Upstream's AsyncAPI generator only emits channels/operations
    for brokers whose spec carries a non-empty ``url`` (it populates ``broker_servers``
    inside ``for url in specification.url``); an empty list yields a structurally blank
    document — no servers, channels, or operations — silently discarding every
    per-subscriber/per-publisher schema. Derive a password-masked DSN from the engine
    when wired, else a stable placeholder keyed on the table name (test broker /
    pre-connect construction, where the engine isn't available yet).
    """
    if engine is not None:
        return [engine.url.render_as_string(hide_password=True)]
    return [f"postgresql://outbox/{outbox_table.name}"]


class _CaptureExceptionMiddleware(BaseMiddleware):
    """
    Stash the handler exception on the inner row before AckMiddleware nacks.

    FastStream's AcknowledgementMiddleware catches the handler exception in its
    own ``after_processed`` and calls ``message.nack()`` directly — the exception
    never propagates back to the worker loop. Without this middleware,
    ``OutboxInnerMessage._nack`` sees ``last_exception=None`` and retry strategies
    that branch on exception type can't work. We sit one step closer to the handler
    in the middleware stack so our ``after_processed`` runs before AckMiddleware's,
    capturing ``exc_val`` onto the row.
    """

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,  # noqa: ARG002
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,  # noqa: ARG002
    ) -> bool | None:
        if exc_val is not None and isinstance(self.msg, OutboxInnerMessage):
            self.msg.last_exception = exc_val
        return False


class OutboxParamsStorage(DefaultLoggerStorage):
    def get_logger(self, *, context: "ContextRepo") -> LoggerProto:
        if logger := self._get_logger_ref():
            return logger
        logger = get_broker_logger(
            name="outbox",
            default_context={"queue": "", "message_id": ""},
            message_id_ln=-1,
            fmt="%(asctime)s %(levelname)-8s - %(queue)-7s | %(message_id)s - %(message)s",
            context=context,
            log_level=self.logger_log_level,
        )
        self._logger_ref.add(logger)
        return logger


class OutboxBroker(
    OutboxRegistrator,
    BrokerUsecase[OutboxInnerMessage, "AsyncEngine", OutboxBrokerConfig],
):
    """FastStream broker backed by a Postgres outbox table."""

    # P25: the runtime container is a WeakSet (set by the upstream Registrator), not a
    # list — annotate it accurately so the _subscribers/subscribers distinction is clear.
    _subscribers: "WeakSet[OutboxSubscriber]"

    def __init__(  # noqa: PLR0913
        self,
        engine: "AsyncEngine | None" = None,
        *,
        outbox_table: "Table",
        dlq_table: "Table | None" = None,
        decoder: CustomCallable | None = None,
        parser: CustomCallable | None = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence[type[BaseMiddleware] | BrokerMiddleware[OutboxInnerMessage]] = (),
        graceful_timeout: float | None = 15.0,
        routers: Sequence[Registrator[OutboxInnerMessage]] = (),
        # Metrics
        metrics_recorder: MetricsRecorder | None = None,
        # Logging
        logger: LoggerProto | None = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends
        apply_types: bool = True,
        serializer: "SerializerProto | None" = EMPTY,
        # AsyncAPI
        description: str | None = None,
        tags: Iterable[Tag | TagDict] = (),
    ) -> None:
        self._outbox_table = outbox_table
        self._dlq_table = dlq_table
        client = OutboxClient(engine, outbox_table, dlq_table=dlq_table) if engine is not None else None
        fd_config = FastDependsConfig(use_fastdepends=apply_types, serializer=serializer)
        recorder: MetricsRecorder = metrics_recorder or _noop_recorder
        producer = OutboxProducer(
            table=outbox_table,
            parser=parser,
            decoder=decoder,
            metrics_recorder=recorder,
        )
        broker_config = OutboxBrokerConfig(
            engine=engine,
            client=client,
            metrics_recorder=recorder,
            dlq_table=dlq_table,
            broker_middlewares=(_CaptureExceptionMiddleware, *middlewares),
            broker_parser=parser,
            broker_decoder=decoder,
            logger=make_logger_state(
                logger=logger,
                log_level=log_level,
                default_storage_cls=OutboxParamsStorage,
            ),
            fd_config=fd_config,
            broker_dependencies=dependencies,
            graceful_timeout=graceful_timeout,
            extra_context={"broker": self},
            producer=producer,
        )
        # Serializer lives on fd_config — wire it onto the producer so encoded
        # bodies use the same path as the broker's own publish flow.
        producer.serializer = fd_config._serializer  # noqa: SLF001
        specification = BrokerSpec(
            url=_spec_url(engine, outbox_table),
            protocol="postgresql",
            protocol_version=None,
            description=description,
            tags=tags,
            security=None,
        )
        super().__init__(config=broker_config, specification=specification, routers=routers)  # ty: ignore[unknown-argument]
        # Track which foreign-broker config ids we've already warned about so
        # repeated start() calls (e.g. the test harness calls start() twice) each
        # only emit the warning once.
        self._warned_foreign_config_ids: set[int] = set()

    @property
    def client(self) -> AbstractOutboxClient:
        client = self.config.broker_config.client
        if client is None:
            msg = "OutboxBroker is not connected; pass an AsyncEngine to the constructor."
            raise RuntimeError(msg)
        return client

    @typing.override
    async def _connect(self) -> "AsyncEngine":
        engine = self.config.broker_config.engine
        if engine is None:
            msg = "Engine not available. Pass an AsyncEngine to OutboxBroker(...)."
            raise IncorrectState(msg)
        return engine

    @typing.override
    async def __aenter__(self) -> typing.Self:
        # Upstream equivalent (replaced):
        #   BrokerUsecase.__aenter__ -> faststream/_internal/broker/broker.py
        # Upstream's __aenter__ only connects; we upgrade to a full start() so the
        # subscriber loops spin up under `async with broker:`. Re-check this if upstream
        # changes __aenter__/__aexit__ pairing (P26).
        await self.start()
        return self

    @typing.override
    async def start(self) -> None:
        await self.connect()
        await super().start()
        self._warn_on_unstarted_foreign_publishers()
        self._warn_on_duplicate_queues()

    def _warn_on_duplicate_queues(self) -> None:
        # P22: the registration-time overlap warning only sees the broker's own
        # _subscribers, so duplicates introduced via include_router slip through. Re-check
        # at start() over the full ``subscribers`` property — but warn only for queues a
        # router contributed to, since same-broker overlaps were already flagged at
        # registration time (avoids double-warning one mistake).
        direct = set(self._subscribers)
        counts: dict[str, int] = {}
        router_queues: set[str] = set()
        for sub in self.subscribers:
            from_router = sub not in direct
            for q in getattr(sub, "_queues", []):
                counts[q] = counts.get(q, 0) + 1
                if from_router:
                    router_queues.add(q)
        duplicated = sorted(q for q, n in counts.items() if n > 1 and q in router_queues)
        if duplicated:
            warnings.warn(
                f"Multiple subscribers serve queue(s) {duplicated}: their workers compete "
                f"for the same rows (nondeterministic via SKIP LOCKED). Use one subscriber "
                f"per queue, or attach multiple handlers to a single subscriber.",
                UserWarning,
                stacklevel=2,
            )

    def _warn_on_unstarted_foreign_publishers(self) -> None:
        """
        Emit one WARNING per foreign-publisher broker that has not been started.

        Foreign-publisher decorators stacked on outbox subscribers only work if
        the foreign broker's producer is wired. When it is not, the first
        relayed row fails deep inside the foreign publisher with an opaque
        AttributeError; this preflight pushes the diagnostic up to start() so
        operators see the cause immediately.

        The broker-level ``_warned_foreign_config_ids`` set deduplicates across
        repeated start() calls (the test harness calls start() twice).
        """
        for sub in self.subscribers:
            for call in sub.calls:
                for pub in call.handler._publishers:  # noqa: SLF001
                    outer = pub._outer_config  # noqa: SLF001  # ty: ignore[unresolved-attribute]
                    # An internal outbox publisher's ``outer`` is this broker's own config,
                    # which carries a (truthy) ``producer`` — so the wired-producer check
                    # below already skips it; no separate ``outer is self.config`` branch needed.
                    producer = getattr(outer, "producer", None)
                    if producer:
                        continue  # already wired / started
                    key = id(outer)
                    if key in self._warned_foreign_config_ids:
                        continue
                    self._warned_foreign_config_ids.add(key)
                    # P21: name the queue(s) of the subscriber actually decorated by this
                    # foreign publisher, not every subscriber on the broker.
                    queues = sorted(getattr(sub, "_queues", []))
                    _logger.warning(
                        "Foreign publisher %r is decorated on outbox subscriber(s) for "
                        "queue(s) %s, but its broker has not been started yet. The first "
                        "relay attempt will fail and the row will retry until the broker "
                        "starts. Call `await foreign_broker.start()` or "
                        "`foreign_broker.connect` in your app's startup hook.",
                        pub,
                        queues,
                    )

    @typing.override
    async def stop(self, *_args: object, **_kwargs: object) -> None:
        # Concurrent subscriber stop. Sequential parent stop (BrokerUsecase.stop's
        # ``for sub in subscribers: await sub.stop()``) would give a total bound of
        # N x graceful_timeout, exceeding K8s default terminationGracePeriodSeconds=30s
        # for N>=2 subscribers under the default 15s budget. Gather collapses that
        # to ~max(per-sub) ~ graceful_timeout.
        #
        # return_exceptions=True so one stuck subscriber doesn't block the others
        # from draining; failures are logged but never re-raised — shutdown must
        # complete even if individual subscribers misbehave.
        #
        # Upstream equivalent (replaced):
        #   BrokerUsecase.stop -> faststream/_internal/broker/broker.py
        # P20: snapshot once — re-evaluating the ``subscribers`` property after the await
        # (e.g. a mid-shutdown include_router) would desync the strict zip and raise out of
        # stop(), defeating its never-raise contract.
        subs = list(self.subscribers)
        results = await asyncio.gather(
            *(sub.stop() for sub in subs),
            return_exceptions=True,
        )
        for sub, result in zip(subs, results, strict=True):
            if isinstance(result, BaseException):
                self._log_subscriber_stop_error(sub, result)
        self.running = False

    def _log_subscriber_stop_error(self, sub: object, exc: BaseException) -> None:
        logger_state = self.config.broker_config.logger
        log = logger_state.logger.logger if logger_state is not None else None
        if log is not None:
            log.log(
                logging.ERROR,
                "Outbox subscriber %s stop raised: %r",
                sub,
                exc,
                exc_info=exc,
            )

    @typing.override
    async def ping(self, timeout: float | None = None) -> bool:
        # ``move_on_after(None)`` is an unbounded scope, so threading the caller's
        # timeout keeps the historical "wait forever" default while honoring a bound
        # when given — a black-holed TCP or a stuck pool checkout can no longer hang
        # the probe past *timeout*, which is the exact partition ``ping`` exists to
        # detect (upstream brokers wrap their probe the same way).
        with anyio.move_on_after(timeout):
            client = self.config.broker_config.client
            if client is None:
                return False
            if not await client.ping():
                return False
            # Walk the ``subscribers`` property, not the ``_subscribers`` list, so
            # router-registered subscribers (the FastAPI pattern) are health-checked
            # too — a dead worker task on a router subscriber must fail the probe.
            for subscriber in self.subscribers:
                outbox_sub = typing.cast("OutboxSubscriber", subscriber)
                for task in outbox_sub.tasks:
                    if task.done():
                        return False
            return True
        # Only reached when the timeout scope cancelled the probe.
        return False

    async def validate_schema(self) -> None:
        """Validate the user's table matches what the package expects. Opt-in."""
        await self.client.validate_schema()

    async def publish(  # ty: ignore[invalid-method-override]
        self,
        body: typing.Any,
        *,
        queue: str,
        session: AsyncSession,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        activate_in: _dt.timedelta | None = None,
        activate_at: _dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        """
        Insert one outbox row using *session*'s open transaction.

        Must be called inside a transaction the caller owns (typically inside an
        ``async with session.begin():`` block). ``publish`` does not flush, commit,
        or open its own transaction — that is the whole point of the transactional
        outbox pattern: the row commits atomically with the caller's domain writes.

        Schedule a delayed delivery by passing exactly one of *activate_in* (relative)
        or *activate_at* (absolute, tz-aware). Pass *timer_id* to deduplicate per
        ``(queue, timer_id)`` — re-publishing with the same id is a no-op (returns
        ``None``). Cancel a not-yet-leased timer with :meth:`cancel_timer`.

        Returns the inserted row's ``id`` (BigInt PK), or ``None`` if a timer with
        the same ``(queue, timer_id)`` already exists.
        """
        cmd = OutboxPublishCommand(
            body,
            queue=queue,
            session=session,
            headers=headers,
            correlation_id=correlation_id,
            activate_in=activate_in,
            activate_at=activate_at,
            timer_id=timer_id,
        )
        result = await self._basic_publish(cmd, producer=self.config.producer)
        return typing.cast("int | None", result)

    async def publish_batch(  # ty: ignore[invalid-method-override]
        self,
        *bodies: typing.Any,
        queue: str,
        session: AsyncSession,
        headers: dict[str, str] | None = None,
        activate_in: _dt.timedelta | None = None,
        activate_at: _dt.datetime | None = None,
    ) -> None:
        """
        Insert multiple outbox rows via *session*. Same transactional contract as ``publish``.

        Each row gets its own auto-generated ``correlation_id``; pass *headers* to
        share static headers across all rows. *activate_in* / *activate_at* schedule
        every row in the batch identically — per-row timer dedup is not supported,
        use :meth:`publish` for that.
        """
        # P1: validate the session type up front so an empty batch fails the same way a
        # non-empty one does (the command constructor that checks it is only built below).
        if not isinstance(session, AsyncSession):
            msg = "broker.publish_batch requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        if not bodies:
            # Validate queue + activate args even when there's no work so an empty
            # batch rejects the same misconfigurations a non-empty one does (F4-06).
            # (session is already checked above; the re-check here is a no-op.)
            _validate_publish_args(
                "broker.publish_batch",
                queue=queue,
                session=session,
                activate_in=activate_in,
                activate_at=activate_at,
            )
            return
        first, *rest = bodies
        cmd = OutboxPublishCommand(
            first,
            *rest,
            queue=queue,
            session=session,
            headers=headers,
            activate_in=activate_in,
            activate_at=activate_at,
        )
        await self._basic_publish_batch(cmd, producer=self.config.producer)

    async def cancel_timer(
        self,
        *,
        queue: str,
        timer_id: str,
        session: AsyncSession,
    ) -> bool:
        """
        Delete a not-yet-leased timer row. Idempotent at the ``(queue, timer_id)`` level.

        Same transactional contract as :meth:`publish` — runs on the caller's session and
        commits with their transaction.

        Returns ``True`` if *this call* deleted the row. ``False`` means the row is no
        longer cancelable by you, which covers three cases:

        1. The row's handler is already in flight (``acquired_token IS NOT NULL``). The
           ``acquired_token IS NULL`` guard prevents this call from clobbering the
           in-flight lease; the delivery completes normally.
        2. Another caller already canceled the same ``(queue, timer_id)`` in a concurrent
           transaction.
        3. No row exists with that ``(queue, timer_id)`` — either the timer was never
           scheduled, the original ``publish(..., timer_id=...)`` hit the
           ``ON CONFLICT DO NOTHING`` path and returned ``None``, or (most common in
           long-running deployments) the row was already delivered and removed by a
           worker before this call.

        Treat ``False`` as "no cancellation needed from your transaction", not as
        "cancellation failed".

        Like :meth:`publish`, this runs on your session **without committing** — a returned
        ``True`` only becomes durable when your transaction commits. If you branch on it and
        then roll back, the cancellation rolls back with you.
        """
        if not isinstance(session, AsyncSession):
            msg = "broker.cancel_timer requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        t = self._outbox_table
        stmt = delete(t).where(
            t.c.queue == queue,
            t.c.timer_id == timer_id,
            t.c.acquired_token.is_(None),
        )
        result = await session.execute(stmt)
        return (result.rowcount or 0) > 0  # ty: ignore[unresolved-attribute]

    async def fetch_unprocessed(
        self,
        *,
        session: AsyncSession,
        queue: str | None = None,
        limit: int = 1000,
    ) -> list[OutboxInnerMessage]:
        """
        Return outbox rows currently in the table — pending, in-flight, or future-dated.

        Intended for test assertions and lease-free operator inspection (the lease-free
        read path `get_one()`/`__aiter__()` point you here): a successful delivery deletes
        the row, so anything still in the table is "unprocessed". Pass *queue* to filter to a single queue;
        omit it to return rows across all queues. *limit* caps the result set
        (default 1000) so an accidental call against a backlogged production table
        does not OOM the process. Runs on the caller's session (same transactional
        contract as :meth:`publish`); does not acquire a lease and does not mutate
        row state, so it is safe to call alongside running subscribers.
        """
        if not isinstance(session, AsyncSession):
            msg = "broker.fetch_unprocessed requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        if limit < 1:
            # F4-04: a non-positive limit otherwise hits SQL (LIMIT -1 → DB error) or
            # silently returns nothing (LIMIT 0); reject it up front, consistently with
            # the fake.
            msg = f"limit must be >= 1, got {limit}"
            raise ValueError(msg)
        t = self._outbox_table
        stmt = select(*t.c).order_by(t.c.id).limit(limit)
        if queue is not None:
            stmt = stmt.where(t.c.queue == queue)
        result = await session.execute(stmt)
        return [_row_to_message(dict(row)) for row in result.mappings().all()]

    async def request(
        self,
        message: "SendableMessage" = None,
        queue: str = "",
        /,
        timeout: float = 0.5,  # noqa: ASYNC109  # mirrors upstream BrokerUsecase.request; never used (always raises)
    ) -> typing.NoReturn:
        # Mirror upstream BrokerUsecase.request's signature (message, queue, /, timeout)
        # so callers and IDEs see the real contract — the outbox is fire-and-forget, so
        # the only surprise is the NotImplementedError, not an opaque (*args, **kwargs).
        raise NotImplementedError(_REQUEST_UNSUPPORTED_MSG)
