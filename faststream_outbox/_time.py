"""Shared time helper — single source so the five former ``_utcnow`` copies can't drift."""

import datetime as dt


def utcnow() -> dt.datetime:
    """Timezone-aware current UTC time."""
    return dt.datetime.now(tz=dt.UTC)
