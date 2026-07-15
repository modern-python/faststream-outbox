import functools
import typing

from faststream._internal.broker import BrokerUsecase
from faststream._internal.testing.broker import TestBroker

from faststream_outbox.autovacuum import check_outbox_autovacuum, outbox_autovacuum_ddl
from faststream_outbox.broker import OutboxBroker
from faststream_outbox.message import OutboxMessage
from faststream_outbox.metrics import MetricsRecorder
from faststream_outbox.publisher.usecase import OutboxPublisher
from faststream_outbox.response import OutboxResponse
from faststream_outbox.retry import (
    ConstantRetry,
    ExponentialRetry,
    LinearRetry,
    NoRetry,
    RetryStrategyProto,
)
from faststream_outbox.router import OutboxRouter
from faststream_outbox.schema import make_dlq_table, make_outbox_table
from faststream_outbox.testing import TestOutboxBroker


__all__ = [
    "ConstantRetry",
    "ExponentialRetry",
    "LinearRetry",
    "MetricsRecorder",
    "NoRetry",
    "OutboxBroker",
    "OutboxMessage",
    "OutboxPublisher",
    "OutboxResponse",
    "OutboxRouter",
    "RetryStrategyProto",
    "TestOutboxBroker",
    "check_outbox_autovacuum",
    "make_dlq_table",
    "make_outbox_table",
    "outbox_autovacuum_ddl",
]

try:
    # S4: import inside the guard too — if upstream moves/removes the module, this
    # raises ImportError here and is tolerated, instead of breaking ``import
    # faststream_outbox`` from an unguarded top-level import.
    import faststream.asgi.factories.asyncapi.try_it_out

    original_get_broker_registry = faststream.asgi.factories.asyncapi.try_it_out._get_broker_registry  # noqa: SLF001

    @functools.lru_cache(maxsize=1)
    def get_broker_registry() -> dict[
        type[BrokerUsecase[typing.Any, typing.Any]],
        type[TestBroker[typing.Any, typing.Any]],
    ]:
        return {**original_get_broker_registry(), OutboxBroker: TestOutboxBroker}  # ty: ignore[invalid-return-type]

    faststream.asgi.factories.asyncapi.try_it_out._get_broker_registry = get_broker_registry  # noqa: SLF001
except (AttributeError, ImportError):  # pragma: no cover
    # FastStream's private ASGI try-it-out registry is best-effort wiring;
    # tolerate breakage if upstream renames/moves the symbol but surface other
    # errors (config, type) loudly so we notice them in CI.
    pass
