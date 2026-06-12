# Relay to a foreign broker

> Want a worked end-to-end example? See
> [Tutorial: Add a Kafka relay](../tutorials/add-kafka-relay.md).

The outbox pattern's payoff line: domain code writes a row to the outbox in
the same DB transaction as its other writes, and a separate worker relays
those rows to a real bus (Kafka, RabbitMQ, NATS, Redis…). `faststream-outbox`
supports this directly via FastStream's cross-broker chain — stack a
foreign-broker publisher decorator on an outbox subscriber and you're done.

*If you don't have a database write to atomically commit alongside, use
the foreign broker directly — see
[Comparison](../concepts/comparison.md).*

## Why an outbox relay

When a request must (a) update your database and (b) emit an event onto a
message bus, the naive shape — DB commit, then bus publish — leaks events on
crashes between the two steps. The outbox pattern fixes this by writing the
event as a row in the same transaction as the domain update; a separate
worker reads the row and publishes to the bus. The row is the durability
boundary, and the relay carries an at-least-once guarantee end to end.

## Minimal relay

```python
from faststream.kafka import KafkaBroker
from faststream_outbox import OutboxBroker

broker_outbox = OutboxBroker(engine=engine)
broker_kafka = KafkaBroker("127.0.0.1:9092")
publisher_kafka = broker_kafka.publisher("kafka_topic")


@publisher_kafka
@broker_outbox.subscriber("outbox_queue")
async def relay(body: dict) -> dict:
    return body
```

That's the whole thing. `broker_outbox.publish(body, queue="outbox_queue", session=session)`
in your domain transaction writes a row; the subscriber dispatches it; the
handler returns it; the Kafka publisher decorator picks it up and publishes
to `kafka_topic`. Failure handling, retries, and DLQ are unchanged from
the rest of the outbox subscriber's behavior.

## Two-broker lifecycle

Both brokers must be started for the relay to work. Two idiomatic shapes:

### FastAPI (recommended)

Mount `OutboxRouter` and the foreign broker's router on the same `FastAPI`
app. Both auto-start via FastAPI's lifespan.

```python
from fastapi import FastAPI
from faststream.kafka.fastapi import KafkaRouter
from faststream_outbox.fastapi import OutboxRouter

outbox_router = OutboxRouter(engine=engine)
kafka_router = KafkaRouter("127.0.0.1:9092")

publisher_kafka = kafka_router.publisher("kafka_topic")


@publisher_kafka
@outbox_router.subscriber("outbox_queue")
async def relay(body: dict) -> dict:
    return body


app = FastAPI()
app.include_router(outbox_router)
app.include_router(kafka_router)
```

### Standalone

A single `FastStream` app with the foreign broker's `connect` hooked into
`on_startup`:

```python
from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream_outbox import OutboxBroker

broker_outbox = OutboxBroker(engine=engine)
broker_kafka = KafkaBroker("127.0.0.1:9092")
publisher_kafka = broker_kafka.publisher("kafka_topic")


@publisher_kafka
@broker_outbox.subscriber("outbox_queue")
async def relay(body: dict) -> dict:
    return body


app = FastStream(broker_outbox, on_startup=[broker_kafka.connect])
```

## At-least-once contract

If the foreign publish raises (Kafka down, partition unavailable, etc.),
the exception propagates through FastStream's `AcknowledgementMiddleware`,
the outbox row is nacked, and the configured `retry_strategy` reschedules
it. The next dispatch re-runs the handler and re-attempts the foreign
publish. **Net effect: at-least-once delivery to the foreign broker.**

Downstream consumers should handle duplicates idempotently, the same way
they would behind any at-least-once bus.

## Header propagation

By default, FastStream's `Response(value)` ships with empty headers, so
the inbound outbox row's headers (`content-type`, custom trace keys, etc.)
are **not** forwarded to the foreign publish. Two ways to override:

**Explicit (per handler):**

```python
from faststream.response import Response

@publisher_kafka
@broker_outbox.subscriber("outbox_queue")
async def relay(body: dict, msg: OutboxMessage) -> Response:
    return Response(body, headers=msg.headers)
```

**Opt-in (per subscriber):**

```python
@publisher_kafka
@broker_outbox.subscriber("outbox_queue", propagate_inbound_headers=True)
async def relay(body: dict) -> dict:
    return body
```

With `propagate_inbound_headers=True`, the subscriber fills `Response.headers`
from the inbound `OutboxMessage.headers` *unless* the handler returned a
`Response(..., headers=...)` explicitly.

## Using routers

Both halves of the chain can live on routers — the FastAPI shape above
already does this with `KafkaRouter` and `OutboxRouter`. The constraint is
that `broker.include_router(router)` must happen *before* the brokers
start. Inside `FastAPI(..., lifespan=...)` the include happens during app
construction (before lifespan), so it's automatic.

For the standalone (non-FastAPI) lifecycle, the order is:

```python
broker_kafka.include_router(kafka_router)
broker_outbox.include_router(outbox_router)
# then start
```

## What not to do

**Do not** combine `OutboxResponse(...)` and a foreign-publisher decorator.

```python
@publisher_kafka
@broker_outbox.subscriber("outbox_queue")
async def relay(body: dict) -> OutboxResponse:
    return OutboxResponse(body=body, queue="next_queue", session=...)  # rejected at dispatch
```

This would both insert a row into the outbox AND publish to Kafka. The
subscriber raises `RuntimeError` at dispatch time when it detects the
combination — pick one path.

**Do not** stack an outbox publisher on a foreign subscriber.

```python
@broker_outbox.publisher("outbox_queue")
@broker_kafka.subscriber("kafka_topic")  # NotImplementedError at decoration
async def relay(body: dict) -> dict:
    return body
```

This direction would need the Kafka subscriber's dispatch loop to provide
an `AsyncSession` for the outbox insert — there isn't one without breaking
the transactional contract. `OutboxPublisher.__call__` raises
`NotImplementedError` at decoration time. Call `await broker_outbox.publish(...)`
inside the handler instead, on a session you opened yourself.

## Other foreign brokers

The same pattern works for Confluent, RabbitMQ, NATS, and Redis — the only
change is the `publisher` line:

| Foreign broker | Publisher line |
|---|---|
| Kafka | `broker_kafka.publisher("topic")` |
| Confluent | `broker_confluent.publisher("topic")` |
| RabbitMQ | `broker_rabbit.publisher("queue")` |
| NATS | `broker_nats.publisher("subject")` |
| Redis | `broker_redis.publisher("channel")` |

Any FastStream broker whose publisher's `_publish` accepts a generic
`PublishCommand` works as a relay destination — that is the FastStream
cross-broker contract, not an outbox-specific feature.
