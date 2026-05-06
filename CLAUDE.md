# faststream-outbox

FastStream broker integration where a Postgres table is the message queue (transactional outbox pattern).

Postgres-only at v0; polling-only (no LISTEN/NOTIFY); no `broker.publish()` (users insert via SQLAlchemy ORM); user-owned schema (Alembic) via `make_outbox_table` factory; built-in retry strategies; no archive (DELETE on terminal); lease-token guards against slow-handler-vs-release_stuck races.

## Commands

- `just test` — full suite via docker-compose Postgres
- `just lint` — format and lint
- `just install` — sync deps

## Tests

- `tests/test_unit.py` — unit tests, no Postgres
- `tests/test_fake.py` — `TestOutboxBroker` with `FakeOutboxClient`, no Postgres
- `tests/test_integration.py` — real Postgres
