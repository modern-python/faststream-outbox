"""Shared time helper — single source so the five former ``_utcnow`` copies can't drift."""

import datetime as _dt


def utcnow() -> _dt.datetime:
    """Timezone-aware current UTC time."""
    return _dt.datetime.now(tz=_dt.UTC)
