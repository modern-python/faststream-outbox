# Tutorial: Add a Kafka relay

## What you'll add

In [Tutorial: Your first outbox app](./first-outbox-app.md) the handler
printed the row and that was the end of it. Real outbox systems usually
*relay* the row to a real message bus — Kafka, RabbitMQ, NATS — so
downstream services can consume it. In this tutorial you'll add a Kafka
broker, stack a single decorator above the existing subscriber, and watch
a row written inside a Postgres transaction land on a Kafka topic.

By the end you will have run a single message end-to-end through the
relay and seen the row arrive at a `kafka-console-consumer`.

## Before you start

- You finished [Tutorial: Your first outbox app](./first-outbox-app.md).
  This tutorial extends that same `app.py`, the same `outbox-postgres`
  container, and the same project directory.
- Docker Compose (the `docker compose` CLI) for the Kafka container.
- Another ten minutes.

## Step 1: Add Kafka via docker-compose

Postgres is already running from Tutorial 1. Add Kafka via a small
`docker-compose.yml`. Single-broker [KRaft
mode](https://kafka.apache.org/documentation/#kraft) — no separate
ZooKeeper service, and Confluent's `cp-kafka:7.6.0` image is known to
run well on Apple Silicon. Two listeners: one for clients on the host
(your `faststream run` process) and one for clients inside the Docker
network (the `kafka-console-consumer` we'll use in Step 5).

```yaml title="docker-compose.yml"
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: outbox-kafka
    ports:
      - "9092:9092"
    environment:
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: HOST://0.0.0.0:9092,DOCKER://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: HOST://localhost:9092,DOCKER://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,HOST:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: DOCKER
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

Bring it up:

```bash
docker compose up -d kafka
```

You should see (image pull progress trimmed):

```text
 Network outbox-tutorial_default  Creating
 Network outbox-tutorial_default  Created
 Container outbox-kafka  Creating
 Container outbox-kafka  Created
 Container outbox-kafka  Starting
 Container outbox-kafka  Started
```

Give it ten seconds and confirm the broker came up cleanly:

```bash
docker compose logs kafka 2>&1 | grep -m1 'Kafka Server started'
```

You should see:

```text
outbox-kafka  | [2026-06-12 05:22:33,782] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)
```

## Step 2: Install `faststream[kafka]`

```bash
uv add 'faststream[kafka]'
```

You should see:

```text
Resolved 29 packages in 785ms
Installed 3 packages in 6ms
 + aiokafka==0.14.0
 + async-timeout==5.0.1
 + packaging==26.2
```

Your pinned versions will differ.

## Step 3: Add the Kafka broker

Open `app.py` from Tutorial 1 and add a `KafkaBroker` plus a publisher
for the `orders.kafka` topic. Rename the existing `broker` to
`broker_outbox` so the two brokers have distinct names. Hook
`broker_kafka.connect` into `FastStream`'s `on_startup` so the Kafka
client opens before the first row is dispatched.

```python title="app.py (edits)"
from faststream.kafka import KafkaBroker

broker_outbox = OutboxBroker(engine, outbox_table=outbox_table)
broker_kafka = KafkaBroker("localhost:9092")
kafka_publisher = broker_kafka.publisher("orders.kafka")

app = FastStream(broker_outbox, on_startup=[broker_kafka.connect])
```

## Step 4: Stack the publisher decorator

Stack `@kafka_publisher` above the existing
`@broker_outbox.subscriber("orders")` and change the handler to `return
order_id`. The stacked decorator picks up the return value and publishes
it to `orders.kafka`. The outbox subscriber is still the one driving
delivery — Kafka becomes the *destination*, not a second subscriber.

```python title="app.py (edits)"
@kafka_publisher
@broker_outbox.subscriber("orders")
async def handle(order_id: int) -> int:
    print(f"got order {order_id}")
    return order_id
```

The full `app.py` now reads:

```python title="app.py"
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream_outbox import OutboxBroker, make_outbox_table

metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")

engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")
broker_outbox = OutboxBroker(engine, outbox_table=outbox_table)
broker_kafka = KafkaBroker("localhost:9092")
kafka_publisher = broker_kafka.publisher("orders.kafka")

app = FastStream(broker_outbox, on_startup=[broker_kafka.connect])

session_factory = async_sessionmaker(engine, expire_on_commit=False)


@kafka_publisher
@broker_outbox.subscriber("orders")
async def handle(order_id: int) -> int:
    print(f"got order {order_id}")
    return order_id


@app.after_startup
async def publish_one() -> None:
    async with session_factory() as session, session.begin():
        await broker_outbox.publish(1, queue="orders", session=session)
```

## Step 5: Run it and watch a row reach Kafka

Start the app in one terminal:

```bash
uv run faststream run app:app
```

You should see:

```text
2026-06-12 08:23:28,284 INFO     - FastStream app starting...
2026-06-12 08:23:28,328 INFO     - orders  |  - `Handle` waiting for messages
2026-06-12 08:23:28,389 INFO     - FastStream app started successfully! To exit, press CTRL+C
2026-06-12 08:23:28,394 INFO     - orders  |  - Received
Topic orders.kafka not found in cluster metadata
got order 1
2026-06-12 08:23:28,527 INFO     - orders  |  - Processed
```

The `Topic orders.kafka not found in cluster metadata` line is
`aiokafka` noticing a brand-new topic and asking the broker to
auto-create it — first-run only.

In a second terminal, attach a console consumer to the topic:

```bash
docker compose exec kafka kafka-console-consumer \
    --bootstrap-server kafka:9092 --topic orders.kafka --from-beginning
```

You should see:

```text
1
```

The single row `broker_outbox.publish(1, ...)` wrote inside the
Postgres transaction has now landed on the Kafka topic. The path was:
session commit → outbox row → outbox subscriber → handler → Kafka
publisher decorator → Kafka topic. Press `Ctrl-C` to stop the consumer.

## What about Kafka downtime?

<!--
Maintainer note: the spec for this tutorial originally proposed a live
"kill Kafka, watch the retry" step. We attempted it during authoring
(Confluent cp-kafka 7.6.0, ~10s and ~20s outage windows) and could not
get the outbox subscriber's retry log lines to surface — aiokafka's
client-side reconnect absorbs short outages internally, so no outbox-
level retry fires. The plan authorized falling back to a contract-
focused callout in lieu of a fragile live demo. If you re-attempt this
in the future and find a Kafka setup that reliably surfaces the retry,
this callout can give way to a real Step 6 again.
-->

If Kafka were unavailable when the outbox subscriber dispatched a row,
the foreign publish would raise, the outbox row would be nacked, and
the configured `retry_strategy` would reschedule it. The next dispatch
re-runs the handler and re-attempts the foreign publish. The net effect
is **at-least-once delivery to the foreign broker** — the outbox row is
the durability boundary, and it stays in the table for the duration of the
retry budget (the default `ExponentialRetry` allows 10 attempts). Once the
budget is exhausted the row is deleted — the default configures no DLQ — so
configure a longer `retry_strategy` or a `dlq_table` to survive outages
beyond that (with the default schedule, ~13–14 minutes).

In practice, `aiokafka`'s producer has its own client-side reconnect
and retry logic, so a short Kafka outage usually completes from the
outbox subscriber's perspective as a single (slow) publish rather than
as a visible retry on the outbox side. Either way the at-least-once
property is preserved. See [Subscriber § Retry
strategies](../usage/subscriber.md#retry-strategies) for the outbox's
own retry policy and [Relay § At-least-once
contract](../usage/relay.md#at-least-once-contract) for the relay
contract in full.

## What you just built

- A two-broker app: an `OutboxBroker` over Postgres and a `KafkaBroker`
  over a local Kafka container.
- A single subscriber whose return value is forwarded to a Kafka topic
  via a stacked publisher decorator — no second handler, no manual
  client code.
- An at-least-once relay: the row is durable in Postgres until the
  Kafka publish succeeds.

The interesting property is the *transactional* part of the publish.
The `broker_outbox.publish(1, ...)` call in `publish_one` ran inside a
session that committed atomically — the row reached the outbox table
as part of the same `COMMIT` that any sibling domain writes would have
committed. There is no window in which the row exists but a sibling
domain write doesn't, or vice versa. The Kafka delivery happens *after*
that boundary, asynchronously, with its own retry safety net. The
outbox is what makes those two halves — transactional domain write and
non-transactional bus publish — survive a process crash together.

## Clean up

```bash
docker compose down -v
docker stop outbox-postgres
```

The first stops Kafka and removes the compose network; the second
stops the Postgres container from Tutorial 1.

## What's next

- [Relay reference](../usage/relay.md) — the full contract: header
  propagation, two-broker lifecycle, other foreign brokers
  (RabbitMQ / NATS / Redis), what *not* to do.
- [Subscriber retry strategies](../usage/subscriber.md#retry-strategies)
  — `ExponentialRetry`, `LinearRetry`, `ConstantRetry`, `NoRetry`, and
  "retry only on transient errors."
- [Comparison](../concepts/comparison.md) — see the section *"vs.
  FastStream + `KafkaBroker` / `RabbitBroker` directly"* for the
  pattern's trade-offs vs. just publishing to Kafka straight from
  your request handler.
