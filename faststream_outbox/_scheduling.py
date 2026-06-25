"""Pure activate-args resolution + validation, shared by the real and fake publish paths.

``activate_in`` / ``activate_at`` are the user-facing scheduling knobs. Turning them
into a single ``next_attempt_at`` (client clock) and deciding whether a row is
future-dated (so NOTIFY can be skipped) are the same rules everywhere, so they live
here as pure functions a caller invokes with its own ``now``.

Leaf module: depends only on the stdlib so ``producer``, ``broker`` and ``testing``
can all import *from* it without a cycle. The single-publish real path computes
``next_attempt_at`` server-side via ``make_interval`` (clock-skew-safe) and does not
use ``resolve_next_attempt_client_side`` — only the batch and fake paths do.
"""

import datetime as _dt


def is_future_dated(
    activate_in: _dt.timedelta | None,
    activate_at: _dt.datetime | None,
    now: _dt.datetime,
) -> bool:
    """Whether a row is genuinely future-dated (so NOTIFY is skipped — polling fires it at the gate)."""
    if activate_in is not None:
        return activate_in > _dt.timedelta(0)
    if activate_at is not None:
        return activate_at > now
    return False


def resolve_next_attempt_client_side(
    activate_in: _dt.timedelta | None,
    activate_at: _dt.datetime | None,
    now: _dt.datetime,
) -> _dt.datetime | None:
    """Resolve activate_in / activate_at to a single ``next_attempt_at`` value (client clock)."""
    if activate_in is not None:
        return now + activate_in
    return activate_at


def validate_activate_args(
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
