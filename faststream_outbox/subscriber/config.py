import sys
import typing
import warnings
from dataclasses import dataclass
from pathlib import Path

import faststream
from faststream._internal.configs import SubscriberSpecificationConfig, SubscriberUsecaseConfig
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy

from faststream_outbox.retry import NoRetry


if typing.TYPE_CHECKING:
    from faststream_outbox.configs import OutboxBrokerConfig
    from faststream_outbox.retry import RetryStrategyProto


# Frames to walk past when attributing a subscriber-config warning to the user's call site:
# this package, faststream, and the dataclass-generated ``__init__`` (its ``co_filename`` is
# the literal ``"<string>"``) that calls ``__post_init__``.
_PKG_DIR = str(Path(__file__).parent.parent)  # the faststream_outbox package dir
_FASTSTREAM_DIR = str(Path(faststream.__file__).parent)  # the faststream package dir


def _is_internal_frame(filename: str) -> bool:
    return filename == "<string>" or filename.startswith((_PKG_DIR, _FASTSTREAM_DIR))


def _subscriber_warn(message: str) -> None:
    """Attribute a subscriber-config ``UserWarning`` to the user's ``@subscriber`` call (P27).

    Computes ``stacklevel`` by walking out to the first non-internal frame instead of using
    ``warnings.warn(skip_file_prefixes=...)``: the 3.13 C ``warn`` does not skip the
    ``"<string>"`` dataclass-``__init__`` frame between ``__post_init__`` and the user (it
    works on 3.14), and a static ``stacklevel`` can't be right for both the direct and the
    FastAPI-router paths (they differ in frame count).
    """
    stacklevel = 2  # frame above this helper's caller (``_validate``)
    frame: typing.Any = sys._getframe(1)  # noqa: SLF001  # the caller, ``_validate``
    while frame is not None and _is_internal_frame(frame.f_code.co_filename):
        frame = frame.f_back
        stacklevel += 1
    warnings.warn(message, UserWarning, stacklevel=stacklevel)


@dataclass(kw_only=True)
class OutboxSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "OutboxBrokerConfig"
    queues: list[str]
    max_workers: int
    retry_strategy: "RetryStrategyProto | None"
    fetch_batch_size: int
    min_fetch_interval: float
    max_fetch_interval: float
    lease_ttl_seconds: float
    max_deliveries: int | None
    propagate_inbound_headers: bool

    def __post_init__(self) -> None:
        # Validation lives here (not in the factory) so *every* construction path —
        # ``@broker.subscriber`` / ``@router.subscriber`` / direct construction — is
        # validated; there is no way to build a misconfigured config that skips the guards.
        # Upstream-divergence watch: ``SubscriberUsecaseConfig`` has no ``__post_init__``
        # today, but dataclasses call only the most-derived one — if faststream adds init
        # logic there, this guarded call keeps it running instead of silently shadowing it.
        parent_post_init = getattr(super(), "__post_init__", None)
        if parent_post_init is not None:  # pragma: no cover  # defensive: base has none today
            parent_post_init()
        self._validate()

    def _validate(self) -> None:  # noqa: C901  # flat sequence of independent knob checks
        """Reject impossible knob values, warn on combos that silently misbehave.

        Errors are raised here (not deferred to runtime) so the user gets a traceback
        pointing at the ``@broker.subscriber(...)`` decorator. Warnings use
        ``skip_file_prefixes`` (see ``_WARN_SKIP_PREFIXES``) so they are attributed to the
        user's call site on both the direct and FastAPI-router paths (P27).
        """
        # EMPTY means "ack_policy not passed" — map it back to None so the checks below
        # match on the *explicitly-passed* policy exactly as the factory-side validation
        # did (e.g. the NACK+NoRetry warning fires only on an explicit NACK_ON_ERROR, not
        # on the default that resolves to NACK_ON_ERROR via the ``ack_policy`` property).
        ack_policy = None if self._ack_policy is EMPTY else self._ack_policy
        if self.max_workers <= 0:
            msg = f"max_workers must be >= 1, got {self.max_workers}"
            raise ValueError(msg)
        if self.fetch_batch_size <= 0:
            msg = f"fetch_batch_size must be >= 1, got {self.fetch_batch_size}"
            raise ValueError(msg)
        # P12: non-positive intervals/TTL turn the adaptive backoff into a busy-poll (or an
        # instantly-expiring lease). Reject up front rather than spin a hot loop at runtime.
        if self.min_fetch_interval <= 0:
            msg = f"min_fetch_interval must be > 0, got {self.min_fetch_interval}"
            raise ValueError(msg)
        if self.max_fetch_interval <= 0:
            msg = f"max_fetch_interval must be > 0, got {self.max_fetch_interval}"
            raise ValueError(msg)
        if self.lease_ttl_seconds <= 0:
            msg = f"lease_ttl_seconds must be > 0, got {self.lease_ttl_seconds}"
            raise ValueError(msg)
        if self.min_fetch_interval > self.max_fetch_interval:
            msg = (
                f"min_fetch_interval ({self.min_fetch_interval}) must be <= max_fetch_interval "
                f"({self.max_fetch_interval}); the adaptive idle backoff grows from ~min_fetch_interval "
                f"(the base interval, with ±50% jitter) up to max_fetch_interval (the ceiling)."
            )
            raise ValueError(msg)
        is_no_retry = isinstance(self.retry_strategy, NoRetry)
        if ack_policy is AckPolicy.ACK_FIRST:
            msg = (
                "ack_policy=AckPolicy.ACK_FIRST is not supported by the outbox broker: it "
                "deletes the row before the handler runs, so a handler crash silently drops "
                "the message — defeating the outbox reliability guarantee. Use NACK_ON_ERROR "
                "(default, retries via retry_strategy), REJECT_ON_ERROR (delete on first "
                "failure, no retry), or MANUAL (handler calls msg.ack()/nack()/reject() itself)."
            )
            raise ValueError(msg)
        if ack_policy is AckPolicy.REJECT_ON_ERROR and self.retry_strategy is not None and not is_no_retry:
            _subscriber_warn(
                "ack_policy=REJECT_ON_ERROR rejects on the first handler error; the "
                "retry_strategy is ignored. Pass ack_policy=NACK_ON_ERROR (default) to "
                "honor retry, or drop retry_strategy if you really want first-error deletion.",
            )
        if ack_policy is AckPolicy.NACK_ON_ERROR and is_no_retry:
            _subscriber_warn(
                "ack_policy=NACK_ON_ERROR with retry_strategy=NoRetry() has the same effect "
                "as REJECT_ON_ERROR (one attempt, then delete). Pick one for clarity.",
            )
        if self.max_deliveries is not None and (self.retry_strategy is None or is_no_retry):
            _subscriber_warn(
                "max_deliveries is set but no retry_strategy is configured (or NoRetry was "
                "passed); the delivery cap is unreachable on the happy path since the row "
                "is deleted after the first attempt.",
            )
        if self.lease_ttl_seconds <= self.max_fetch_interval:
            _subscriber_warn(
                f"lease_ttl_seconds ({self.lease_ttl_seconds}) <= max_fetch_interval "
                f"({self.max_fetch_interval}): a lease can expire during a single idle wait "
                f"before the next fetch even runs, causing spurious lease-expiry reclaim "
                f"of healthy in-flight rows. Recommended: lease_ttl_seconds >= "
                f"2 * max_fetch_interval + P99(handler).",
            )

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            return AckPolicy.NACK_ON_ERROR
        return self._ack_policy


@dataclass(kw_only=True)
class OutboxSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    queues: list[str]
