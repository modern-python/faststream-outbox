# messaging-service-patterns-doc — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Patterns" docs page that shows one anonymized chat /
notifications service composing the transactional event relay, the
fire-unless-cancelled timer, and the in-process test of both.

**Spec:** [`design.md`](./design.md)

**Branch:** `docs/messaging-service-patterns` (already checked out; spec already
committed there).

**Commit strategy:** Per-task commits.

## Global constraints

- Docs are MkDocs Material; build with `just docs-build` (= `mkdocs build
  --strict`). `--strict` fails on broken internal links, so every cross-link
  must resolve.
- Page is **anonymized** — a generic chat / notifications service, never the
  private source service by name.
- Coverage is the **core trio only** (relay, timer, testing). No
  production-hardening / next-steps prose — hardening features get one-line
  "See also" links only.
- Code snippets use a **real DI container** (`modern-di` / `modern-di-faststream`)
  and only the public API: `OutboxBroker`, `OutboxRouter`, `make_outbox_table`,
  `TestOutboxBroker`, and broker methods `publish` / `cancel_timer` /
  `fetch_unprocessed`. Snippets are illustrative (not CI-executed).
- Two queues throughout: `chat-events` and `unread-timers`.

---

### Task 1: Create the page and add the Patterns nav section

**Files:**
- Create: `docs/patterns/messaging-service.md`
- Modify: `mkdocs.yml` (insert a `Patterns` nav section between `Guides` and
  `Reference`)

This task delivers the whole page plus its nav entry, verified by a strict
docs build.

- [ ] **Step 1: Create `docs/patterns/messaging-service.md` with exactly this content**

  Write the file with the content inside the four-backtick fence below (the
  inner three-backtick blocks are part of the file):

````markdown
# A messaging service, end-to-end

The [tutorials](../tutorials/first-outbox-app.md) build a greenfield app one
primitive at a time, and each guide documents one feature on its own. This page
is different: it walks a single service that **composes** three outbox
primitives — a transactional event relay, a fire-unless-cancelled timer, and an
in-process test of the whole chain.

The service is a generic chat / notifications backend. Users post messages into
chats. It has two obligations, and both must be **atomic with the database
write** — they commit with the domain row and must never fire if the
transaction rolls back:

1. **Broadcast events.** Every message created, read, or deleted is published to
   downstream consumers over Kafka.
2. **Unread notifications.** If a message is still unread `N` seconds after it
   arrives, notify the recipient — *unless they read it first*.

A plain message bus can't give you "commits with the domain row": publishing to
Kafka and committing to Postgres are two systems, so a crash between them either
drops the event or emits one for a transaction that rolled back. The outbox
makes the event a *row* written in the same transaction. Two queues carry the
two obligations: `chat-events` and `unread-timers`.

## Architecture at a glance

```text
  CreateMessageUseCase  ─┐
   (one DB transaction)  ├─▶ outbox row: chat-events    ─▶ subscriber ─▶ Kafka
                         └─▶ outbox row: unread-timers   ─▶ subscriber ─▶ Kafka
                                  ▲
  ReadMessageUseCase ── cancel_timer("unread-timers", timer_id) ┘
```

- **Use cases** write domain rows and outbox rows in one transaction.
- **The broker** is an `OutboxBroker` over the application's `AsyncEngine`; the
  outbox table lives on the app's own `MetaData` via `make_outbox_table`, so
  Alembic owns its migrations.
- **Subscribers** poll each queue and relay the row onward to Kafka.

```python title="tables.py"
from faststream_outbox import make_outbox_table
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


OUTBOX_TABLE = make_outbox_table(Base.metadata, table_name="outbox")
```

The broker and routers are wired with a real DI container
([modern-di](https://modern-di.readthedocs.io/) here, but any container works):

```python title="ioc.py"
from modern_di import Group, Scope, providers
from faststream_outbox import OutboxBroker

from app.tables import OUTBOX_TABLE


class Resources(Group):
    outbox_broker = providers.Factory(
        scope=Scope.APP,
        creator=lambda engine: OutboxBroker(engine, outbox_table=OUTBOX_TABLE),
        kwargs={"engine": Resources.database_engine},
    )
```

## Pattern 1 — Transactional event relay

A thin producer wraps `broker.publish`. Note the contract: `publish` inserts the
outbox row through the caller's `AsyncSession` but **does not flush, commit, or
open its own transaction** — the row commits with your domain writes.

```python title="producers.py"
import dataclasses

import pydantic
from faststream_outbox import OutboxBroker
from sqlalchemy.ext.asyncio import AsyncSession


class ChatEvent(pydantic.BaseModel):
    type: str  # "created" | "read" | "deleted"
    message_id: int
    chat_id: int


@dataclasses.dataclass(kw_only=True, slots=True, frozen=True)
class OutboxEventProducer:
    outbox_broker: OutboxBroker
    session: AsyncSession

    async def send_chat_event(self, event: ChatEvent) -> None:
        await self.outbox_broker.publish(
            event, queue="chat-events", session=self.session,
        )
```

The use case calls the producer **inside** its transaction, beside the domain
write, and commits once. If the commit fails, no event row exists; if it
succeeds, the event is guaranteed durable:

```python title="use_cases.py"
import dataclasses

from app.producers import ChatEvent, OutboxEventProducer
from app.repositories import MessagesRepository
from app.transaction import Transaction


@dataclasses.dataclass(kw_only=True, slots=True, frozen=True)
class CreateMessageUseCase:
    transaction: Transaction
    messages_repository: MessagesRepository
    producer: OutboxEventProducer

    async def __call__(self, command: "CreateMessageCommand") -> None:
        async with self.transaction:
            message = await self.messages_repository.create(command.payload)
            await self.producer.send_chat_event(
                ChatEvent(type="created", message_id=message.id, chat_id=message.chat_id),
            )
            await self.producer.arm_unread_timer(message)  # Pattern 2, below
            await self.transaction.commit()
```

A subscriber on `chat-events` reads the row and relays it to Kafka via a
DI-injected Kafka producer:

```python title="handlers.py"
from faststream_outbox import OutboxRouter
from modern_di_faststream import FromDI

from app.kafka import KafkaEventProducer
from app.producers import ChatEvent

ROUTER = OutboxRouter()


@ROUTER.subscriber("chat-events")
async def relay_chat_event(
    event: ChatEvent,
    kafka_producer: KafkaEventProducer = FromDI(KafkaEventProducer),
) -> None:
    await kafka_producer.publish_event(event)
```

Register the router on the broker with `broker.include_routers(ROUTER)`.

> This service hand-rolls the Kafka hop through a DI'd producer, which keeps the
> Kafka client fully under your control. If you'd rather stack the relay as a
> single decorator over the subscriber, see
> [Relay to Kafka / RabbitMQ / NATS](../usage/relay.md).

## Pattern 2 — Fire-unless-cancelled timer

The unread notification is a **delayed** outbox row, armed in the same create
transaction. `timer_id` makes it idempotent; `activate_in` defers it:

```python title="producers.py (continued)"
import datetime

# inside OutboxEventProducer:

UNREAD_DELAY = datetime.timedelta(seconds=30)

async def arm_unread_timer(self, message: "Message") -> None:
    await self.outbox_broker.publish(
        ChatEvent(type="unread", message_id=message.id, chat_id=message.chat_id),
        queue="unread-timers",
        timer_id=str(message.id),
        activate_in=UNREAD_DELAY,
        session=self.session,
    )

async def cancel_unread_timer(self, message_id: int) -> None:
    await self.outbox_broker.cancel_timer(
        queue="unread-timers",
        timer_id=str(message_id),
        session=self.session,
    )
```

When the recipient reads the message, a second use case **cancels** the timer in
its own transaction:

```python title="use_cases.py (continued)"
@dataclasses.dataclass(kw_only=True, slots=True, frozen=True)
class ReadMessageUseCase:
    transaction: Transaction
    messages_repository: MessagesRepository
    producer: OutboxEventProducer

    async def __call__(self, command: "ReadMessageCommand") -> None:
        async with self.transaction:
            message = await self.messages_repository.mark_read(command.message_id)
            await self.producer.send_chat_event(
                ChatEvent(type="read", message_id=message.id, chat_id=message.chat_id),
            )
            await self.producer.cancel_unread_timer(message.id)
            await self.transaction.commit()
```

Two properties make this safe, and one is a limit worth knowing:

- **At-most-one-live.** `timer_id` deduplicates per `(queue, timer_id)`. Arming
  the same id twice while a row is in flight is a no-op, so retries don't
  produce two notifications.
- **Cancel is lease-guarded.** `cancel_timer` only deletes a row that is not yet
  being delivered (it filters on an unheld lease) and returns `False` otherwise.
- **The race window is real.** Once the timer is leased for delivery, a read can
  no longer cancel it — the notification fires. Downstream consumers should
  tolerate the occasional already-read notification.

More on scheduling semantics: [Timers](../usage/timers.md).

## Pattern 3 — Testing the composed app

Nest `TestOutboxBroker` and `TestKafkaBroker`. In the default **sync mode**,
`broker.publish` drives the subscriber in-process, so one call to a use case
runs the whole chain — outbox row → relay handler → Kafka — and you assert on
the Kafka test broker without any background loop:

```python title="test_messaging.py"
from faststream.kafka import TestKafkaBroker
from faststream_outbox import TestOutboxBroker


async def test_create_message_relays_event_to_kafka(
    outbox_broker, kafka_broker, create_message_use_case, command, kafka_publisher,
) -> None:
    async with TestOutboxBroker(outbox_broker), TestKafkaBroker(kafka_broker):
        await create_message_use_case(command)

        kafka_publisher.mock.assert_called_once()
```

Two caveats specific to this composition:

- **Future-dated rows fire immediately in sync mode.** The 30-second
  `unread-timers` row is dispatched at once, so a sync-mode test sees the
  notification without waiting. To test the *delay* and the cancel race for
  real, construct `TestOutboxBroker(outbox_broker, run_loops=True)` — that runs
  the real fetch/worker loops against the in-memory store.
- **`validate_schema()` needs a real engine.** The fake client raises
  `NotImplementedError`, so put the schema check in its own test against a real
  `OutboxBroker`:

```python title="test_schema.py"
from faststream_outbox import OutboxBroker


async def test_outbox_schema(outbox_broker: OutboxBroker) -> None:
    await outbox_broker.validate_schema()
```

More on the test broker's two modes: [Testing](../usage/testing.md).

## See also

- [Relay to Kafka / RabbitMQ / NATS](../usage/relay.md) — the native relay
  decorator, an alternative to the hand-rolled hop above.
- [Dead-letter queue](../usage/dlq.md) — archive terminal failures instead of
  deleting them.
- [Observability](../usage/observability.md) — the metrics recorder and the
  Prometheus / OpenTelemetry middleware.
````

- [ ] **Step 2: Add the Patterns nav section to `mkdocs.yml`**

  Insert this block between the `Guides:` section and the `Reference:` section
  (i.e. after the `Setup Prometheus and OpenTelemetry` line, before `- Reference:`):

  ```yaml
    - Patterns:
        - 'A messaging service, end-to-end': patterns/messaging-service.md
  ```

- [ ] **Step 3: Build the docs strictly**

  Run: `just docs-build`
  Expected: `mkdocs build --strict` completes with no warnings or errors — in
  particular no "contains a link to ... which is not found" and no "not found in
  the documentation files" for `patterns/messaging-service.md`.

- [ ] **Step 4: Commit**

  ```bash
  git add docs/patterns/messaging-service.md mkdocs.yml
  git commit -m "docs: add Patterns page composing the outbox in a service

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Verify cross-links and snippet API accuracy

**Files:**
- Modify (only if a check fails): `docs/patterns/messaging-service.md`

Audit the page against the real docs tree and the real public API, fix any
drift, and confirm a clean strict build.

- [ ] **Step 1: Confirm every cross-link target exists**

  Run:
  ```bash
  for f in tutorials/first-outbox-app.md usage/relay.md usage/timers.md \
           usage/testing.md usage/dlq.md usage/observability.md; do
    test -f "docs/$f" && echo "OK  $f" || echo "MISSING  $f"
  done
  ```
  Expected: six `OK` lines, no `MISSING`. If any is missing, fix the link in the
  page to the correct existing path.

- [ ] **Step 2: Confirm every API symbol in the page is public**

  Run:
  ```bash
  python -c "import faststream_outbox as o; print(sorted(o.__all__))"
  ```
  Confirm the page only references symbols that are exported (`OutboxBroker`,
  `OutboxRouter`, `make_outbox_table`, `TestOutboxBroker`) plus the documented
  broker methods `publish`, `cancel_timer`, `fetch_unprocessed`. Fix any symbol
  that isn't.

- [ ] **Step 3: Final strict build**

  Run: `just docs-build`
  Expected: clean `mkdocs build --strict`.

- [ ] **Step 4: Commit any fixes (skip if the working tree is clean)**

  ```bash
  git add docs/patterns/messaging-service.md
  git commit -m "docs: fix link/API drift on Patterns page

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

## Self-review

- **Spec coverage:** scenario + why-outbox (Task 1 intro), architecture diagram
  (Task 1), Pattern 1 relay (Task 1), Pattern 2 fire-unless-cancelled timer
  (Task 1), Pattern 3 testing with both caveats + schema note (Task 1), See-also
  footer (Task 1), Patterns nav section (Task 1 Step 2), strict-build + link/API
  audit (Tasks 1 & 2). All spec sections map to a task.
- **Placeholders:** none — the full page content is inlined verbatim in Task 1.
- **Type/name consistency:** `OutboxEventProducer`, `ChatEvent`,
  `CreateMessageUseCase`, `ReadMessageUseCase`, queues `chat-events` /
  `unread-timers`, methods `send_chat_event` / `arm_unread_timer` /
  `cancel_unread_timer` are used consistently across every snippet.
