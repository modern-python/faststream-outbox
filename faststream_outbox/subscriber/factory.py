import typing
import warnings

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.middlewares import AckPolicy

from faststream_outbox.retry import NoRetry
from faststream_outbox.subscriber.config import OutboxSubscriberConfig, OutboxSubscriberSpecificationConfig
from faststream_outbox.subscriber.usecase import OutboxSubscriber, OutboxSubscriberSpecification


if typing.TYPE_CHECKING:
    from faststream_outbox.configs import OutboxBrokerConfig
    from faststream_outbox.retry import RetryStrategyProto


def create_subscriber(
    *,
    queues: list[str],
    max_workers: int,
    retry_strategy: "RetryStrategyProto | None",
    fetch_batch_size: int,
    min_fetch_interval: float,
    max_fetch_interval: float,
    lease_ttl_seconds: float,
    max_deliveries: int | None,
    config: "OutboxBrokerConfig",
    ack_policy: AckPolicy | None = None,
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
) -> OutboxSubscriber:
    _validate_subscriber_config(
        max_workers=max_workers,
        fetch_batch_size=fetch_batch_size,
        min_fetch_interval=min_fetch_interval,
        max_fetch_interval=max_fetch_interval,
        lease_ttl_seconds=lease_ttl_seconds,
        max_deliveries=max_deliveries,
        ack_policy=ack_policy,
        retry_strategy=retry_strategy,
    )
    usecase_config = OutboxSubscriberConfig(
        _outer_config=config,
        _ack_policy=ack_policy if ack_policy is not None else EMPTY,
        queues=queues,
        max_workers=max_workers,
        retry_strategy=retry_strategy,
        fetch_batch_size=fetch_batch_size,
        min_fetch_interval=min_fetch_interval,
        max_fetch_interval=max_fetch_interval,
        lease_ttl_seconds=lease_ttl_seconds,
        max_deliveries=max_deliveries,
    )
    specification_config = OutboxSubscriberSpecificationConfig(
        queues=queues,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )
    calls: CallsCollection[typing.Any] = CallsCollection()
    specification = OutboxSubscriberSpecification(
        _outer_config=config,
        specification_config=specification_config,
        calls=calls,
    )
    return OutboxSubscriber(
        config=usecase_config,
        specification=specification,
        calls=calls,
    )


def _validate_subscriber_config(
    *,
    max_workers: int,
    fetch_batch_size: int,
    min_fetch_interval: float,
    max_fetch_interval: float,
    lease_ttl_seconds: float,
    max_deliveries: int | None,
    ack_policy: AckPolicy | None,
    retry_strategy: "RetryStrategyProto | None",
) -> None:
    """
    Reject impossible knob values, warn on combos that silently misbehave.

    Errors are raised here (not deferred to runtime) so the user gets a
    traceback pointing at the ``@broker.subscriber(...)`` decorator. Warnings
    use ``stacklevel=4`` so they point at the same line (frames: user →
    ``subscriber()`` → ``create_subscriber()`` → ``_validate_subscriber_config()``).
    """
    if max_workers <= 0:
        msg = f"max_workers must be >= 1, got {max_workers}"
        raise ValueError(msg)
    if fetch_batch_size <= 0:
        msg = f"fetch_batch_size must be >= 1, got {fetch_batch_size}"
        raise ValueError(msg)
    if min_fetch_interval > max_fetch_interval:
        msg = (
            f"min_fetch_interval ({min_fetch_interval}) must be <= max_fetch_interval "
            f"({max_fetch_interval}); the adaptive backoff treats min as a floor and "
            f"max as the ceiling."
        )
        raise ValueError(msg)
    is_no_retry = isinstance(retry_strategy, NoRetry)
    if ack_policy is AckPolicy.ACK_FIRST:
        msg = (
            "ack_policy=AckPolicy.ACK_FIRST is not supported by the outbox broker: it "
            "deletes the row before the handler runs, so a handler crash silently drops "
            "the message — defeating the outbox reliability guarantee. Use NACK_ON_ERROR "
            "(default, retries via retry_strategy), REJECT_ON_ERROR (delete on first "
            "failure, no retry), or MANUAL (handler calls msg.ack()/nack()/reject() itself)."
        )
        raise ValueError(msg)
    if ack_policy is AckPolicy.REJECT_ON_ERROR and retry_strategy is not None and not is_no_retry:
        warnings.warn(
            "ack_policy=REJECT_ON_ERROR rejects on the first handler error; the "
            "retry_strategy is ignored. Pass ack_policy=NACK_ON_ERROR (default) to "
            "honor retry, or drop retry_strategy if you really want first-error deletion.",
            UserWarning,
            stacklevel=4,
        )
    if ack_policy is AckPolicy.NACK_ON_ERROR and is_no_retry:
        warnings.warn(
            "ack_policy=NACK_ON_ERROR with retry_strategy=NoRetry() has the same effect "
            "as REJECT_ON_ERROR (one attempt, then delete). Pick one for clarity.",
            UserWarning,
            stacklevel=4,
        )
    if max_deliveries is not None and (retry_strategy is None or is_no_retry):
        warnings.warn(
            "max_deliveries is set but no retry_strategy is configured (or NoRetry was "
            "passed); the delivery cap is unreachable on the happy path since the row "
            "is deleted after the first attempt.",
            UserWarning,
            stacklevel=4,
        )
    if lease_ttl_seconds <= max_fetch_interval:
        warnings.warn(
            f"lease_ttl_seconds ({lease_ttl_seconds}) <= max_fetch_interval "
            f"({max_fetch_interval}): a lease can expire during a single idle wait "
            f"before the next fetch even runs, causing spurious lease-expiry reclaim "
            f"of healthy in-flight rows. Recommended: lease_ttl_seconds >= "
            f"2 * max_fetch_interval + P99(handler).",
            UserWarning,
            stacklevel=4,
        )
