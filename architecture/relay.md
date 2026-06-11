# Relay to foreign broker — implementation detail

User-facing: `docs/usage/relay.md`. Invariant summary: `CLAUDE.md` § Relay to foreign broker.

## The bare mechanism

`OutboxSubscriber` can source a FastStream-native cross-broker chain — `@kafka_pub @broker_outbox.subscriber("q")` (and the same for Rabbit/NATS/Redis/Confluent) is the canonical transactional-outbox primitive. The mechanism is upstream FastStream's `SubscriberUsecase.process_message` walking `chain(self.__get_response_publisher(message), h.handler._publishers)`; no override of the dispatch path is needed for the chain itself.

Three load-bearing properties keep it safe:

a. `OutboxFakePublisher` self-gates on `isinstance(cmd, OutboxPublishCommand)` so it no-ops on plain handler returns.
b. Foreign publishers coerce via `<Broker>PublishCommand.from_cmd(cmd)`.
c. `AcknowledgementMiddleware.__aexit__` turns publisher-chain exceptions into outbox nacks, preserving at-least-once via the configured `retry_strategy`.

## Three guardrails

### `OutboxResponse` + foreign publisher refused

`OutboxSubscriber` overrides `process_message` to check chain composition. If a handler returns `OutboxResponse(...)` while having a non-`OutboxFakePublisher` entry in `handler._publishers`, the override raises `_OutboxConfigError` (a private `RuntimeError` subclass). The subscriber also overrides `consume()` and extends `dispatch_one`'s exception handler to re-raise `_OutboxConfigError` rather than swallowing it via upstream's `except Exception: pass`. The exception propagates through `AcknowledgementMiddleware` and triggers the outbox's normal nack path so the row is retried (and the operator sees the error log) until the handler is fixed.

### WARNING for unstarted foreign brokers at `start()`

`OutboxBroker.start()` walks `self.subscribers`, for each subscriber walks `handler._publishers`, and for any publisher whose `_outer_config` is not `self.config` and whose foreign broker's `producer` is falsy (Kafka's `EmptyProducerState` is falsy; an active producer is truthy), logs one WARNING per unstarted foreign broker. The dedup state lives on the broker as `_warned_foreign_config_ids: set[int]` so multiple `start()` calls (broker `__aenter__` plus test-broker `_fake_start`) don't double-fire. Logger is `logging.getLogger(__name__)` at the top of `broker.py`, matching the existing pattern in `metrics/__init__.py` and `publisher/producer.py`. Operators see the cause before the first relayed row fails.

### `propagate_inbound_headers: bool = False` on subscriber

When `True`, the `process_message` override fills `Response.headers` from the inbound `OutboxMessage.headers` only when the handler returned a `Response` with empty headers (explicit user-set headers win). Default False matches the FastStream-wide convention; users who want propagation flip one kwarg.
