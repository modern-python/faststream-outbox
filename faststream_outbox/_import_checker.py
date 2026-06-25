"""Centralized probes for optional-extra imports.

Each ``is_*_installed`` is a module-level boolean derived from
``importlib.util.find_spec`` so consumers can guard runtime imports without a
``try/except ImportError`` ladder, and so diagnostic surfaces (e.g. a `/health`
endpoint) can report which extras are available. Modeled after the
``import_checker`` module in the sister project ``lite-bootstrap``.

The booleans are evaluated at import time and cached — extras can't be
installed mid-process, so re-probing would be wasted work.
"""

from importlib.util import find_spec


is_alembic_installed = find_spec("alembic") is not None
is_asyncpg_installed = find_spec("asyncpg") is not None
is_fastapi_installed = find_spec("fastapi") is not None
is_opentelemetry_installed = find_spec("opentelemetry") is not None
is_prometheus_client_installed = find_spec("prometheus_client") is not None


def missing_extra_message(component: str, extra: str) -> str:
    """Build the friendly "this needs an optional extra" install hint.

    Single source of truth for the message text so the import-time guard and the
    ``__init__`` probe guard in each middleware module stay in sync (B13).
    """
    return f"{component} requires the '{extra}' extra: pip install 'faststream-outbox[{extra}]'"


__all__ = [
    "is_alembic_installed",
    "is_asyncpg_installed",
    "is_fastapi_installed",
    "is_opentelemetry_installed",
    "is_prometheus_client_installed",
    "missing_extra_message",
]
