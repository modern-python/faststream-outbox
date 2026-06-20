---
status: shipped
date: 2026-06-04
slug: foreign-broker-relay
summary: OutboxSubscriber officially supports the FastStream-native decorator relay to Kafka/Rabbit/NATS/Redis with three guardrails.
supersedes: null
superseded_by: null
pr: "44"
outcome: merged 2026-06-05 as #44
---

# Foreign-broker relay from `OutboxSubscriber` — design

## Goal

Officially support the FastStream-native decorator-relay pattern where an
`OutboxSubscriber` is the **source** and a foreign-broker publisher (Kafka,
Rabbit, NATS, Redis, Confluent — any FastStream broker) is the destination:

```python
publisher_kafka = broker_kafka.publisher("kafka_topic")

@publisher_kafka
@broker_outbox.subscriber("outbox_queue", max_workers=10, retry_strategy=...)
async def relay(body: dict) -> dict:
    return body
```

This is the canonical transactional-outbox primitive: the producer writes a
row to the outbox in the same DB transaction as its domain writes, and this
subscriber relays it to a real bus with at-least-once delivery.

`faststream-sqlbroker` ships the same pattern; we adopt (not invent) the
FastStream contract here.

## Non-goals

- **Foreign broker → outbox relay** (`@outbox_pub @kafka.sub`). The existing
  `OutboxPublisher.__call__` `NotImplementedError` stays. The transactional
  contract requires an `AsyncSession` the foreign dispatch loop cannot
  provide. This is stricter than `faststream-sqlbroker`, which allows the
  inverse direction and silently drops the transactional contract (see
  "Comparison with `faststream-sqlbroker`" below).
- **Wrapper API** (`broker.relay(queue=, to=publisher)`). The naked
  `@publisher @broker.subscriber(...)` chain stays the only API.
- **Multi-broker app launcher.** We document two lifecycle patterns and
  ship neither as a helper.

## How the wiring works today (no new mechanism)

`OutboxSubscriber` inherits `SubscriberUsecase.process_message`, whose
relevant body is:

```python
for p in chain(
    self.__get_response_publisher(message),  # → (OutboxFakePublisher,)
    h.handler._publishers,                   # → (KafkaPublisher, …)  from @pub
):
    await p._publish(
        result_msg.as_publish_command(),
        _extra_middlewares=(m.publish_scope for m in middlewares[::-1]),
    )
```

Three load-bearing properties make the chain safe with no extra plumbing:

1. **`OutboxFakePublisher` self-gates** on
   `isinstance(cmd, OutboxPublishCommand)` and no-ops for plain handler
   returns — so it does not accidentally fan a relayed message back into
   the outbox.
2. **Foreign publishers coerce via `<Broker>PublishCommand.from_cmd(cmd)`.**
   `KafkaPublisher._publish`, `RabbitPublisher._publish`, etc. already
   accept a generic `PublishCommand` produced by `Response(value).as_publish_command()`.
3. **`AcknowledgementMiddleware.__aexit__`** sees publisher-chain exceptions
   via the `AsyncExitStack` and runs the configured `AckPolicy` nack. A
   foreign-publish failure therefore triggers an outbox nack →
   retry-via-`retry_strategy` → eventual redelivery. **At-least-once relay
   is preserved by middleware ordering, not by anything we add to the
   dispatch path.**

**Implication.** The relay itself needs zero changes to the dispatch path.
All proposed code in this spec is for **guardrails and discoverability**,
not for the mechanism.

## Router publishers as relay decorators

`kafka_router.publisher("topic")` returns a publisher whose `_outer_config`
is the router's `ConfigComposition`. At construction time that composition
has no broker — `.producer` would raise. On
`broker_kafka.include_router(kafka_router)`, the broker's config is
prepended to the composition (`ConfigComposition.add_config` →
`configs = (broker_config, *old_configs)`), and `.broker_config` returns
`configs[0]` = the broker's config. From that moment,
`router_publisher._outer_config.producer` resolves to the broker's real
producer.

**Constraint.** `include_router` must run before the outbox subscriber
starts dispatching. The natural shape:

```python
broker_outbox = OutboxBroker(engine=engine)
broker_kafka = KafkaBroker("127.0.0.1:9092")
kafka_router = KafkaRouter()

publisher_kafka = kafka_router.publisher("kafka_topic")

@publisher_kafka
@broker_outbox.subscriber("outbox_queue")
async def relay(body: dict) -> dict:
    return body

broker_kafka.include_router(kafka_router)   # wires producer into router publisher
app = FastStream(broker_outbox, on_startup=[broker_kafka.connect])
```

The pattern is symmetric for the **outbox side**: an `OutboxRouter` can
host the subscriber, with a foreign publisher decorating it, as long as
`broker_outbox.include_router(outbox_router)` runs before
`broker_outbox.start()`. **Both halves of the chain can live on routers.**

No code changes — docs and tests only.

## Guardrails (code changes)

Three small, independent additions.

### G1. Block `OutboxResponse` + foreign publisher at dispatch time

**Problem.** If a handler returns `OutboxResponse(body=..., queue=..., session=...)`
**and** has a foreign publisher decorator, both fire: a row is inserted in
the outbox AND the same body is published to Kafka. Almost certainly not
intended.

**Behavior.** Override `OutboxSubscriber.process_message` (mirroring
FastStream's body) to insert a chain-composition check between
`ensure_response(...)` and the `for p in chain(...)` loop: if
`isinstance(result_msg, OutboxResponse)` AND `h.handler._publishers` is
non-empty AND any entry is not an `OutboxFakePublisher` → raise
`RuntimeError` with a message pointing at the two valid patterns
(`return body` for relay, or remove the foreign publisher decorator if
outbox fan-out is intended). The error propagates through the middleware
stack, `AckPolicy` nacks → outbox retries → operator sees a repeating
error in logs and fixes the handler. (Lease-loss invariants on the retry
path are unaffected: the existing `acquired_token` guard on
`mark_pending_with_lease` keeps the row safe.)

### G2. Detect foreign-broker-not-started at `OutboxBroker.start()`

**Problem.** A foreign publisher whose broker has not been started has a
`None` producer; the first relay attempt fails deep inside
`KafkaPublisher._publish` with an `AttributeError`. The outbox row
retries forever until the foreign broker comes up; the diagnostic is
muddy.

**Behavior.** In `OutboxBroker.start()` (after the existing startup body,
before returning), walk `self.subscribers`. For each subscriber walk
`h.handler._publishers` for every `h` in `subscriber.calls`. For each
publisher whose `_outer_config` is **not** `self.config` (foreign):
duck-type-check whether its broker is connected (look up
`publisher._outer_config.broker_config` and probe an attribute that
distinguishes started from unstarted state for the common broker types —
typically the producer attribute being non-`None`). If unstarted, log a
**single WARNING per unstarted foreign broker** with the broker's repr
and the affected queue names. Do not raise — startup ordering is a user
concern, the warning is the operator signal.

### G3. Inbound header propagation — opt-in subscriber kwarg, default off

**Problem.** FastStream's `Response(value)` carries empty headers. When a
handler returns a plain value, the relay loses the inbound outbox row's
headers (`content-type`, custom tracing keys, user headers).

**Behavior.** Add `propagate_inbound_headers: bool = False` to
`OutboxBroker.subscriber(...)`, `OutboxRouter.subscriber(...)`, and the
`fastapi.OutboxRouter.subscriber(...)` equivalents (plumbed through
`OutboxSubscriberConfig`). When `True`, the subscriber wraps the
post-`ensure_response` result: if the `Response` has empty `headers`,
fill them with `message.headers` before `as_publish_command()`. If the
user returned `Response(value, headers=...)` explicitly, do **not**
override their choice.

**Default `False`** — matches every native FastStream broker and
`faststream-sqlbroker`. Opt-in keeps us consistent with FastStream-wide
behavior; users who want propagation flip one kwarg.

## Comparison with `faststream-sqlbroker`

| Concern | sqlbroker | Outbox |
|---|---|---|
| Cross-broker relay mechanism | Same FastStream-native chain. `SqlBrokerSubscriber` does not override `process_message`; foreign publishers register via base `PublisherUsecase.__call__` and fire from `h.handler._publishers`. | Same. |
| `@sqlbroker_pub @kafka.sub` (relay-decorating the *internal* publisher) | `LogicPublisher.__call__` is not overridden. The relay path calls `_publish` → `producer.publish` **without the caller's `AsyncConnection`** (`connection` param defaults to `None`). **Silently drops the transactional contract for chaining ergonomics.** | Blocked at `OutboxPublisher.__call__` with `NotImplementedError`. Stricter. |
| `OutboxResponse` + foreign publisher dual-fire | Same FastStream-native chain; same dual-fire footgun applies. No guard shipped. | **G1 — guard shipped.** Ahead of sqlbroker. |
| Foreign broker not started detection | None. | **G2 — WARNING-level startup check.** Ahead of sqlbroker. |
| Inbound header propagation default | False (FastStream-wide convention). | False. Opt-in kwarg (G3). Same. |
| Lifecycle pattern in docs | `app = FastStream(broker_sqlbroker, on_startup=[broker_kafka.connect])`. Single FastStream app, foreign producer connected during startup. | **Adopt verbatim** (Section "Docs"). FastAPI two-router pattern documented as second option. |
| Relay tutorial visibility | Top of `Tutorial > Transactional outbox`. Featured prominently. | Match — top of `Usage` nav. README front page is rewritten around the relay. |
| Router-based relay coverage | Not shown in tutorial. | Covered in tutorial + test. Modest doc win. |

The net read: we are sqlbroker-shaped where the FastStream contract
dictates it (chain mechanism, header default, lifecycle idiom); more
conservative on the internal-publisher-as-relay-decorator question (block
rather than silently drop the contract); ahead on G1 and G2.

## Docs

Concrete placements, in priority order.

### D1. README front page rewrite

Replace the current quickstart's plain `broker.publish(...)` + handler
example with the **end-to-end relay**: domain code calls
`broker_outbox.publish(..., session=session)` inside a transaction, and a
relay subscriber forwards to Kafka. The relay block:

```python
publisher_kafka = broker_kafka.publisher("kafka_topic")

@publisher_kafka
@broker_outbox.subscriber("outbox_queue")
async def relay(body: dict) -> dict:
    return body
```

`OutboxResponse` stays off the front page (it is the advanced fan-out
path, deferred to its own doc).

### D2. Dedicated tutorial: `docs/usage/relay.md`

Promoted to the **top of the `Usage` nav** in `mkdocs.yml`, above
existing pages (publishing, DLQ, etc.). Sections:

1. **Why an outbox relay** — one paragraph framing the transactional-outbox
   pattern.
2. **Minimal relay** — the example above; note that both brokers must be
   running.
3. **Two-broker lifecycle** — two short subsections:
   - **FastAPI (recommended).** Mount `OutboxRouter` and the foreign
     broker's router on the same `FastAPI` app; both auto-start via
     lifespan.
   - **Standalone.** `app = FastStream(broker_outbox,
     on_startup=[broker_kafka.connect])` (sqlbroker's idiom).
4. **At-least-once contract.** Foreign-publish exception → `AckPolicy`
   nack → outbox retry. Downstream consumers must be idempotent
   (standard outbox property).
5. **Header propagation.** `propagate_inbound_headers=True` and the
   explicit `return Response(value, headers=msg.headers)` form.
6. **Using routers.** Both shapes: foreign router (`KafkaRouter`),
   outbox router (`OutboxRouter`), and both. Note the
   `include_router`-before-`start()` constraint.
7. **What not to do.** The `OutboxResponse` + foreign-publisher
   combination (links to G1's runtime error) and the
   `@outbox_pub @kafka.sub` direction (links to the existing
   `NotImplementedError`).

### D3. Cross-broker examples grid

Brief end-of-tutorial table: the same
`@publisher_X @broker_outbox.subscriber(...)` block adapted for Kafka,
Confluent, Rabbit, NATS, Redis. Just the broker setup + publisher line —
proves "any FastStream broker" without bloating the page.

### D4. Introduction page mention

One-line callout at the bottom of `docs/introduction/`: "Relay outbox
rows to Kafka / Rabbit / NATS / Redis with a single decorator → [tutorial
link]". Discoverable without restructuring the intro.

### D5. `CLAUDE.md` update

Add a "Relay to foreign broker" subsection under the existing producer /
subscriber sections: documents that `OutboxSubscriber` supports being a
relay source via the FastStream-native cross-broker chain, names the
three load-bearing properties (Section "How the wiring works today"),
and points at G1/G2/G3. Future-you reading `CLAUDE.md` immediately sees
the contract and how at-least-once is preserved.

### D6. AsyncAPI specification

No changes. The foreign publisher already advertises itself in the
foreign broker's AsyncAPI doc; the outbox subscriber advertises itself
in ours. The relay is implicit. Same as native cross-broker chains.

## Tests

All five live in a new `tests/test_relay.py` unless noted; all are
driven by `TestOutboxBroker` + `TestKafkaBroker` (no real Kafka)
except T5.

1. **`test_naked_decorator_chain_relays_to_foreign_broker`** — publish to
   the outbox queue, dispatch via `TestOutboxBroker(..., run_loops=False)`
   sync mode, assert the foreign `TestKafkaBroker`'s mock recorded a
   publish with the relayed body. Sub-assertions: default
   `propagate_inbound_headers=False` drops inbound headers; flipping to
   `True` preserves them.
2. **`test_relay_via_router_publisher`** — Kafka publisher from a
   `KafkaRouter` `include_router`'d into the Kafka broker after the chain
   is set up. Covers the outbox-side router too: subscriber on
   `OutboxRouter`, `broker_outbox.include_router(outbox_router)` after
   chain setup.
3. **`test_outbox_response_with_foreign_publisher_raises`** — stack a
   Kafka publisher on a subscriber whose handler returns
   `OutboxResponse(...)`. Dispatch one row, assert the G1 guard raises
   with the documented message and `AckPolicy` nacks (row stays / retries
   in sync mode).
4. **`test_foreign_broker_not_started_warns_on_start`** — build the
   chain, start `broker_outbox` without starting `broker_kafka`. Assert a
   single WARNING log per unstarted foreign broker with the broker's
   repr and affected queues. No exception raised.
5. **`test_integration.py::test_relay_at_least_once_under_publish_failure`**
   (Postgres-required, gated by `pg_engine`) — real outbox subscriber +
   foreign publisher whose `_publish` raises on first call, succeeds on
   retry. Assert exactly-one eventual delivery and `deliveries_count`
   reflects the retry. Uses `TestKafkaBroker` for the destination so no
   real Kafka is needed.

All test arguments — including pytest fixtures — are type-annotated per
the standing convention.

## File touch list

| File | Why |
|---|---|
| `faststream_outbox/subscriber/usecase.py` | Override `process_message` to add G1 guard; thread G3 header propagation. |
| `faststream_outbox/subscriber/config.py` | Add `propagate_inbound_headers: bool = False`. |
| `faststream_outbox/subscriber/factory.py` | Pipe new kwarg through. |
| `faststream_outbox/registrator.py` | Pipe new kwarg through `subscriber(...)`. |
| `faststream_outbox/router.py` | Pipe new kwarg through `OutboxRouter.subscriber(...)`. |
| `faststream_outbox/fastapi/router.py` | Pipe new kwarg through `fastapi.OutboxRouter.subscriber(...)`. |
| `faststream_outbox/broker.py` | G2: walk subscribers in `start()`, emit WARNINGs for unstarted foreign-publisher brokers. |
| `README.md` | D1 — front-page rewrite. |
| `docs/usage/relay.md` | D2 — new tutorial page. |
| `mkdocs.yml` | D2 — nav promotion of the relay tutorial. |
| `docs/introduction/index.md` | D4 — one-line callout. |
| `CLAUDE.md` | D5 — new subsection. |
| `tests/test_relay.py` | T1–T4 — new file. |
| `tests/test_integration.py` | T5 — append. |

## Open questions

None blocking. The shape is set; remaining details (exact warning
message wording, exact `RuntimeError` message text, README rewrite
contents) are filled in during planning.
