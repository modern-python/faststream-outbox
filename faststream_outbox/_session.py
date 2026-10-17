"""The caller-owned handle an outbox row is written on; a runtime union, so it also works with ``isinstance``."""

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession


SessionOrConnection = AsyncSession | AsyncConnection


def require_session_or_connection(session: object, context: str) -> None:
    if not isinstance(session, SessionOrConnection):
        msg = f"{context} requires an sqlalchemy.ext.asyncio.AsyncSession or AsyncConnection"
        raise TypeError(msg)
