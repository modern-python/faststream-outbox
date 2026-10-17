"""Pure activate-args resolution, shared by the real and fake insert paths.

``activate_in`` / ``activate_at`` are the user-facing scheduling knobs. Turning them
into a single ``next_attempt_at`` (client clock) and deciding whether a row is
future-dated (so NOTIFY can be skipped) are the same rules everywhere, so they live
here as pure functions a caller invokes with its own ``now``.

Leaf module: depends only on the stdlib so ``client`` and ``testing``
can both import *from* it without a cycle. The single-publish real path computes
``next_attempt_at`` server-side via ``make_interval`` (clock-skew-safe) and does not
use ``resolve_next_attempt_client_side`` — only the batch and fake paths do.
"""

import datetime as dt


def is_future_dated(
    activate_in: dt.timedelta | None,
    activate_at: dt.datetime | None,
    now: dt.datetime,
) -> bool:
    """Whether a row is genuinely future-dated (so NOTIFY is skipped — polling fires it at the gate)."""
    if activate_in is not None:
        return activate_in > dt.timedelta(0)
    if activate_at is not None:
        return activate_at > now
    return False


def resolve_next_attempt_client_side(
    activate_in: dt.timedelta | None,
    activate_at: dt.datetime | None,
    now: dt.datetime,
) -> dt.datetime | None:
    """Resolve activate_in / activate_at to a single ``next_attempt_at`` value (client clock)."""
    if activate_in is not None:
        return now + activate_in
    return activate_at
