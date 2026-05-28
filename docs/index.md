# faststream-outbox

Welcome to the `faststream-outbox` documentation!

`faststream-outbox` is a [FastStream](https://faststream.airt.ai) broker integration for the **transactional outbox pattern** — a Postgres table is the message queue.

A producer writes a domain entity and an outbox row in the *same* SQLAlchemy transaction. A subscriber polls the table with `FOR UPDATE SKIP LOCKED`, runs the handler, and deletes the row on success. No separate message bus, no relay process — the table *is* the queue.

---

- [Installation](introduction/installation.md)
- [How it works](introduction/how-it-works.md)
- [Basic usage](usage/basic.md)
- [Subscriber](usage/subscriber.md)
- [Publisher](usage/publisher.md)
- [Router](usage/router.md)
- [FastAPI integration](usage/fastapi.md)
- [Timers](usage/timers.md)
- [Testing](usage/testing.md)
- [Schema validation](usage/schema-validation.md)
- [Observability](usage/observability.md)
