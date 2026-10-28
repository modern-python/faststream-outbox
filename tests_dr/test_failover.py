"""Primary/replica failover drills. Run via ``just test-dr``, one fresh cluster per scenario."""

import asyncio
import json
import os
import socket
import time
from collections.abc import Awaitable, Callable

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from faststream_outbox import OutboxBroker, make_outbox_table


PRIMARY_DSN = os.environ.get("PRIMARY_DSN", "")
REPLICA_DSN = os.environ.get("REPLICA_DSN", "")
DOCKER_SOCKET = "/var/run/docker.sock"
PRIMARY_CONTAINER = "outbox-dr-primary"
PRIMARY_HOST = "pg-primary"
QUEUE = "dr"
BEFORE = 50
AFTER = 50
QUERY_TIMEOUT_SECONDS = 10.0
CONNECT_TIMEOUT_SECONDS = 3.0
COMMAND_TIMEOUT_SECONDS = 3.0
RECOVERY_BUDGET_SECONDS = 90.0
PUBLISH_ATTEMPT_TIMEOUT_SECONDS = 5.0
TEST_BUDGET_SECONDS = 240.0

pytestmark = pytest.mark.skipif(not PRIMARY_DSN, reason="needs the docker-compose.dr.yml cluster")


async def _connect_to_primary() -> asyncpg.Connection:
    """Try each host under its own timeout; asyncpg's ``timeout`` covers all hosts at once."""
    errors: list[Exception] = []
    for dsn in (PRIMARY_DSN, REPLICA_DSN):
        try:
            async with asyncio.timeout(CONNECT_TIMEOUT_SECONDS):
                return await asyncpg.connect(
                    dsn, target_session_attrs="primary", command_timeout=COMMAND_TIMEOUT_SECONDS
                )
        except (OSError, asyncpg.TargetServerAttributeNotMatched) as e:
            errors.append(e)
    msg = "no primary reachable"
    raise ExceptionGroup(msg, errors)


async def _docker(method: str, path: str, body: dict[str, object] | None = None) -> None:
    payload = json.dumps(body).encode() if body is not None else b""
    reader, writer = await asyncio.open_unix_connection(DOCKER_SOCKET)
    head = (
        f"{method} {path} HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n"
        f"Content-Type: application/json\r\nContent-Length: {len(payload)}\r\n\r\n"
    )
    writer.write(head.encode() + payload)
    await writer.drain()
    status_line = await reader.readline()
    writer.close()
    assert int(status_line.split(b" ", 2)[1]) < 300, status_line.decode(errors="replace")


async def _kill_primary() -> None:
    await _docker("POST", f"/containers/{PRIMARY_CONTAINER}/kill")


async def _blackhole_primary() -> None:
    """Drop every packet to and from the primary; its name still resolves, so connects hang."""
    infos = await asyncio.get_running_loop().getaddrinfo(
        PRIMARY_HOST, None, family=socket.AF_INET, type=socket.SOCK_STREAM
    )
    address = str(infos[0][4][0])
    for rule in (["OUTPUT", "-d", address], ["INPUT", "-s", address]):
        proc = await asyncio.create_subprocess_exec("iptables", "-A", *rule, "-j", "DROP")
        assert await proc.wait() == 0, rule


async def _wait_until(predicate: Callable[[], bool], *, deadline: float, what: str) -> None:
    while not predicate():
        if time.monotonic() > deadline:
            msg = f"timed out waiting for {what}"
            raise AssertionError(msg)
        await asyncio.sleep(0.1)


async def _wait_replica_caught_up() -> None:
    primary = await asyncpg.connect(PRIMARY_DSN)
    replica = await asyncpg.connect(REPLICA_DSN)
    try:
        target = await primary.fetchval("SELECT pg_current_wal_lsn()")
        deadline = time.monotonic() + 30
        while await replica.fetchval("SELECT pg_last_wal_replay_lsn() < $1::pg_lsn", target):
            assert time.monotonic() < deadline, "replica did not catch up"
            await asyncio.sleep(0.1)
    finally:
        await primary.close()
        await replica.close()


async def _promote_replica() -> None:
    replica = await asyncpg.connect(REPLICA_DSN)
    try:
        assert await replica.fetchval("SELECT pg_promote()")
    finally:
        await replica.close()


@pytest.mark.parametrize("fail_primary", [_kill_primary, _blackhole_primary], ids=["kill", "blackhole"])
async def test_subscriber_recovers_after_failover(fail_primary: Callable[[], Awaitable[None]]) -> None:
    engine = create_async_engine("postgresql+asyncpg://", async_creator=_connect_to_primary)
    metadata = MetaData()
    table = make_outbox_table(metadata, table_name="dr_outbox")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    broker = OutboxBroker(engine, outbox_table=table)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    handled_at: dict[int, float] = {}
    deliveries: list[int] = []

    @broker.subscriber(QUEUE, query_timeout_seconds=QUERY_TIMEOUT_SECONDS)
    async def handle(body: dict[str, int]) -> None:
        deliveries.append(body["n"])
        handled_at.setdefault(body["n"], time.monotonic())

    async def publish(n: int) -> None:
        async with sessions() as session, session.begin():
            await broker.publish({"n": n}, queue=QUEUE, session=session)

    async def publish_until_accepted(n: int, deadline: float) -> None:
        while True:
            try:
                async with asyncio.timeout(PUBLISH_ATTEMPT_TIMEOUT_SECONDS):
                    await publish(n)
            except Exception:
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(0.5)
            else:
                return

    async with asyncio.timeout(TEST_BUDGET_SECONDS), broker:
        for n in range(BEFORE):
            await publish(n)
        await _wait_until(
            lambda: len(handled_at) == BEFORE,
            deadline=time.monotonic() + 30,
            what="pre-failover messages",
        )
        await _wait_replica_caught_up()

        failed_at = time.monotonic()
        await fail_primary()
        await _promote_replica()
        deadline = failed_at + RECOVERY_BUDGET_SECONDS
        for n in range(BEFORE, BEFORE + AFTER):
            await publish_until_accepted(n, deadline)
        publish_recovered = time.monotonic() - failed_at
        await _wait_until(
            lambda: all(n in handled_at for n in range(BEFORE, BEFORE + AFTER)),
            deadline=deadline,
            what="post-failover messages",
        )
        subscriber_recovered = max(handled_at[n] for n in range(BEFORE, BEFORE + AFTER)) - failed_at

        # Idle past max_fetch_interval so only a live LISTEN connection can deliver within 3s.
        await asyncio.sleep(12)
        await publish(BEFORE + AFTER)
        published_at = time.monotonic()
        await _wait_until(lambda: BEFORE + AFTER in handled_at, deadline=published_at + 3, what="NOTIFY wakeup")

        print(  # noqa: T201
            f"\npublish recovered {publish_recovered:.1f}s, subscriber recovered {subscriber_recovered:.1f}s, "
            f"redeliveries {len(deliveries) - len(handled_at)}",
        )
