import functools
import typing

import faststream.asgi.factories.asyncapi.try_it_out
from faststream._internal.broker import BrokerUsecase
from faststream._internal.testing.broker import TestBroker

from faststream_outbox.broker import OutboxBroker
from faststream_outbox.retry import (
    ConstantRetry,
    ExponentialRetry,
    LinearRetry,
    NoRetry,
    RetryStrategyProto,
)
from faststream_outbox.router import OutboxRouter
from faststream_outbox.schema import make_outbox_table
from faststream_outbox.testing import TestOutboxBroker


__all__ = [
    "ConstantRetry",
    "ExponentialRetry",
    "LinearRetry",
    "NoRetry",
    "OutboxBroker",
    "OutboxRouter",
    "RetryStrategyProto",
    "TestOutboxBroker",
    "make_outbox_table",
]

try:
    original_get_broker_registry = faststream.asgi.factories.asyncapi.try_it_out._get_broker_registry  # noqa: SLF001

    @functools.lru_cache(maxsize=1)
    def get_broker_registry() -> dict[type[BrokerUsecase[typing.Any, typing.Any]], type[TestBroker[typing.Any]]]:
        return {**original_get_broker_registry(), OutboxBroker: TestOutboxBroker}  # ty: ignore[invalid-return-type]

    faststream.asgi.factories.asyncapi.try_it_out._get_broker_registry = get_broker_registry  # noqa: SLF001
except Exception:  # noqa: BLE001, S110  # pragma: no cover
    pass
