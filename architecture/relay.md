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

`OutboxSubscriber` overrides `process_message` to check chain composition. If a handler returns `OutboxResponse(...)` while having a non-`OutboxFakePublisher` entry in `handler._publishers`, the override raises `_OutboxConfigError` (a private `RuntimeError` subclass). The subscriber also overrides `consume()` and extends `dispatch_one`'s exception handler to re-raise `_OutboxConfigError` rather than swallowing it via upstream's `except Exception: pass`. The re-raised error unwinds out of `dispatch_one` **before** any terminal flush, so the worker loop catches it, logs it at ERROR, and moves on — it does **not** route the row through the reconnect/backoff path (which would throttle unrelated rows) and no nack is ever flushed. The row's lease simply expires and a later fetch reclaims it (retry via lease expiry, **not** the `retry_strategy`) until the handler is fixed (P18).

### WARNING for unstarted foreign brokers at `start()`

`OutboxBroker.start()` walks `self.subscribers`, for each subscriber walks `handler._publishers`, and for any publisher whose `_outer_config` is not `self.config` and whose foreign broker's `producer` is falsy (Kafka's `EmptyProducerState` is falsy; an active producer is truthy), logs one WARNING per unstarted foreign broker. The dedup state lives on the broker as `_warned_foreign_config_ids: set[int]` so multiple `start()` calls (broker `__aenter__` plus test-broker `_fake_start`) don't double-fire. Logger is `logging.getLogger(__name__)` at the top of `broker.py`, matching the existing pattern in `metrics/__init__.py` and `publisher/producer.py`. Operators see the cause before the first relayed row fails.

### `propagate_inbound_headers: bool = False` on subscriber

When `True`, the `process_message` override fills `Response.headers` from the inbound `OutboxMessage.headers` only when the handler returned a `Response` with empty headers (explicit user-set headers win). Default False matches the FastStream-wide convention; users who want propagation flip one kwarg.

**Envelope-managed keys are stripped for a chained `OutboxResponse`.** `_maybe_propagate_inbound_headers` drops `content-type` and `correlation_id` from the propagated dict when the result is an `OutboxResponse`. That response re-encodes through `_encode_payload`, which re-derives `content-type` from the *new* body and reads `correlation_id` from the dedicated field; propagating the inbound row's values would make `_encode_payload` raise on any cross-content-type or custom-`correlation_id` relay, nacking the **successful** inbound row to retry-exhaustion (audit F5-01 / F5-02). Foreign-publisher relays (Kafka/etc.) don't re-encode through the outbox envelope, so they keep forwarding these headers verbatim — including `content-type`.

## Who retries during a foreign-broker outage

Retries come from **two tiers**, and short outages never reach the outbox one:

- **Transient blip (~10–30s):** the client library (e.g. `aiokafka`) absorbs it with its own reconnect+retry loop. The `publish` inside the handler **blocks until the broker returns**, then succeeds. From the outbox subscriber's view that is one slow *successful* publish — **no raise, no nack, no `nacked_retried` tick, no `next_attempt_at` reschedule**. At-least-once still holds (the outbox row is held until the client acks), but the retrying tier is the client, not the outbox.
- **Sustained outage / hard failure:** the client eventually raises into the handler; `AcknowledgementMiddleware` turns that into a nack and the outbox's `retry_strategy` takes over (property (c) above).

Implications: a quick `docker compose stop kafka` will **not** produce a visible outbox-level retry log line — to exercise the outbox retry tier in a demo or test, raise inside the handler instead. Operators sizing alerts on `lease_lost` / `nacked_retried` should not expect spikes during short blips. One real failure mode: a long-blocked publish (client spinning) can outrun `lease_ttl_seconds` and surface as a `lease_lost` on the terminal write — `lease_lost` correlating with broker instability is that.
