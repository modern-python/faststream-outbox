"""
Outbox subscriber — the consume loop that backs ``@broker.subscriber("queue")``.

Three async tasks run per subscriber:

* ``_fetch_loop`` claims due rows from Postgres and pushes them onto an
  in-process queue. Adaptive idle backoff with jitter when the queue is empty.
* ``_worker_loop`` (one per ``max_workers``) pulls rows, dispatches via
  ``consume()``, and writes the terminal state back through the client. Every
  terminal write is filtered by ``acquired_token`` so a row whose lease was
  released doesn't get clobbered.
* ``_release_stuck_loop`` periodically flips ``processing`` rows older than
  ``release_stuck_timeout`` back to ``pending`` (advisory-locked, idempotent).
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


_BACKOFF_EXP_CAP = 30


if typing.TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage

    from faststream_outbox.client import OutboxClient
    from faststream_outbox.configs import OutboxBrokerConfig


class OutboxSubscriberSpecification(SubscriberSpecification["OutboxBrokerConfig", OutboxSubscriberSpecificationConfig]):
    @property
    def name(self) -> str:
        prefix = getattr(self._outer_config, "prefix", "")
        joined = ",".join(self.config.queues)
        return f"{prefix}{joined}:{self.call_name}"

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

    @property
    def _client(self) -> "OutboxClient":
        client = self._outer_config.client
        if client is None:
            msg = "OutboxSubscriber is not connected; the broker has no client."
            raise RuntimeError(msg)
        return client

    @property
    def _queues(self) -> list[str]:
        return self._config.full_queues

    @typing.override
    async def start(self) -> None:
        await super().start()
        self._post_start()
        if not self.calls:
            return
        for _ in range(self._config.max_workers):
            self.add_task(self._worker_loop)
        self.add_task(self._fetch_loop)
        self.add_task(self._release_stuck_loop)

    @typing.override
    async def stop(self) -> None:
        with anyio.move_on_after(self._outer_config.graceful_timeout):
            await super().stop()

    async def _fetch_loop(self) -> None:
        base = self._config.min_fetch_interval
        max_idle = self._config.max_fetch_interval
        idle_count = 0
        error_attempt = 0

        while self.running:
            free = self._inflight.maxsize - self._inflight.qsize()
            if free <= 0:
                await anyio.sleep(base)
                continue
            try:
                rows = await self._client.fetch(self._queues, limit=min(free, self._config.fetch_batch_size))
            except Exception as e:  # noqa: BLE001
                self._log(log_level=logging.ERROR, message=f"Outbox fetch error: {e!r}", exc_info=e)
                error_attempt = min(error_attempt + 1, _BACKOFF_EXP_CAP)
                delay = min(2.0 ** (error_attempt - 1) * random.uniform(0.5, 1.5), 30.0)  # noqa: S311
                await anyio.sleep(delay)
                continue

            error_attempt = 0
            if rows:
                idle_count = 0
                for row in rows:
                    await self._inflight.put(row)
            else:
                idle_count = min(idle_count + 1, _BACKOFF_EXP_CAP)
                delay = min(base * (2.0 ** (idle_count - 1)) * random.uniform(0.5, 1.5), max_idle)  # noqa: S311
                await anyio.sleep(delay)

    async def _worker_loop(self) -> None:
        logger = self._outer_config.logger.logger.logger if self._outer_config.logger else None
        while self.running:
            row = await self._inflight.get()
            try:
                row.retry_strategy = self._config.retry_strategy
                if not row.allow_delivery(max_deliveries=self._config.max_deliveries, logger=logger):
                    await self._flush_terminal(row)
                    continue
                try:
                    await self.consume(row)
                except BaseException as exc:
                    row.last_exception = exc
                    raise
                finally:
                    await row.assert_state_set(logger)
                await self._flush_result(row)
            except Exception as e:  # noqa: BLE001
                self._log(log_level=logging.ERROR, message=f"Outbox worker error: {e!r}", exc_info=e)
            finally:
                self._inflight.task_done()

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
        if row.acquired_token is None:
            return
        updated = await self._client.mark_pending_with_lease(
            row.id,
            row.acquired_token,
            next_attempt_at=row.next_attempt_at,
            attempts_count=row.attempts_count,
            first_attempt_at=row.first_attempt_at,  # ty: ignore[invalid-argument-type]
            last_attempt_at=row.last_attempt_at,  # ty: ignore[invalid-argument-type]
        )
        if not updated:
            self._log(
                log_level=logging.INFO,
                message=f"Outbox row {row} lease expired before retry update; skipping",
            )

    async def _release_stuck_loop(self) -> None:
        interval = self._config.release_stuck_interval
        timeout = self._config.release_stuck_timeout
        while self.running:
            try:
                released = await self._client.release_stuck(timeout_seconds=timeout)
            except Exception as e:  # noqa: BLE001
                self._log(log_level=logging.ERROR, message=f"release_stuck error: {e!r}", exc_info=e)
            else:
                if released:
                    self._log(
                        log_level=logging.WARNING,
                        message=f"release_stuck reset {released} stale rows back to pending",
                    )
            await anyio.sleep(interval)

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
