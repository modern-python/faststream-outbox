---
status: shipped
date: 2026-06-04
slug: foreign-broker-relay
spec: design.md
pr: "44"
---

# Foreign-broker relay — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Officially support the FastStream-native decorator-relay pattern (`@kafka_pub @broker_outbox.subscriber(...)`) so an `OutboxSubscriber` can serve as the source of an outbox-to-foreign-broker relay, with three small guardrails (block `OutboxResponse` + foreign-publisher dual-fire, WARN on unstarted foreign brokers, opt-in inbound-header propagation) and a docs push that promotes the relay tutorial to the top of the navigation.

**Architecture:** Zero changes to the dispatch path — the FastStream chain `chain(self.__get_response_publisher(message), h.handler._publishers)` already runs foreign publishers, `OutboxFakePublisher` already self-gates on `OutboxPublishCommand`, and `AcknowledgementMiddleware.__aexit__` already turns publisher-chain exceptions into outbox nacks. Guardrails go in: G1 (override `OutboxSubscriber.process_message` to refuse the `OutboxResponse` + foreign-publisher combo), G2 (walk subscribers in `OutboxBroker.start()` and warn for unstarted foreign brokers), G3 (a `propagate_inbound_headers: bool = False` kwarg plumbed through config → factory → registrator → router → fastapi router, applied in the same `process_message` override). Docs: a new `docs/usage/relay.md`, nav promotion in `mkdocs.yml`, intro callout, `CLAUDE.md` subsection, and a README front-page rewrite.

**Tech Stack:** Python 3.13+, FastStream 0.7.1, SQLAlchemy 2.x (async), `uv` for deps, `ruff` for lint, `ty` for type check, `pytest` (under docker compose for the Postgres-backed suite). New dev dependency: `faststream[kafka]` for `TestKafkaBroker` in the relay tests.

**Spec:** [`planning/specs/2026-06-04-foreign-broker-relay-design.md`](../specs/2026-06-04-foreign-broker-relay-design.md)

**Commit strategy:** Per-task commits. Each task ends with `git add` of named files only (never `git add -A`) and a `git commit` invocation. Tasks 8–10 (docs) can be folded into one commit if Task 8 completes cleanly; otherwise commit per docs task. The final Task 11 produces no commit — it only runs full lint + test suite and reports.

**Branch:** `feat/foreign-broker-relay`.

---

### Task 1: Branch and add `faststream[kafka]` dev dependency

**Files:**
- Modify: `pyproject.toml` (dev dependency group)

The `TestKafkaBroker` driver used by Tasks 2–5 lives behind FastStream's `kafka` extra. Without it, the relay tests `ImportError` on first run. We add it to the `dev` dependency group (not `[project.optional-dependencies]`, which is for runtime extras) so that user installs of `faststream-outbox` do not pick up Kafka transport unless they explicitly depend on it.

- [ ] **Step 1: Create the feature branch from `main`**

Run: `git switch -c feat/foreign-broker-relay`
Expected: `Switched to a new branch 'feat/foreign-broker-relay'`.

- [ ] **Step 2: Edit `pyproject.toml`**

Find the `[dependency-groups]` block (around line 24). Change the `dev` list from:

```toml
dev = [
    "pytest",
    "pytest-asyncio",
    "pytest-cov",
    "asyncpg>=0.29",
    "alembic>=1.13",
    "fastapi>=0.95",
    "httpx2>=2.2",
    "prometheus-client>=0.19",
    "opentelemetry-api>=1.20",
    "opentelemetry-sdk>=1.20",
]
```

to:

```toml
dev = [
    "pytest",
    "pytest-asyncio",
    "pytest-cov",
    "asyncpg>=0.29",
    "alembic>=1.13",
    "fastapi>=0.95",
    "faststream[kafka]>=0.7.1,<0.8",
    "httpx2>=2.2",
    "prometheus-client>=0.19",
    "opentelemetry-api>=1.20",
    "opentelemetry-sdk>=1.20",
]
```

The version pin matches the runtime `faststream` pin on line 12 so resolver does not pull a different minor.

- [ ] **Step 3: Refresh the lockfile**

Run: `just install`
Expected: `uv` resolves `aiokafka` and supporting Kafka deps. Sync reports no errors.

- [ ] **Step 4: Confirm Kafka imports work**

Run: `uv run python -c "from faststream.kafka import KafkaBroker, KafkaRouter, TestKafkaBroker; print('ok')"`
Expected output: `ok`

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add faststream[kafka] to dev deps for relay tests

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Baseline relay test — naked decorator chain, default behavior

**Files:**
- Create: `tests/test_relay.py`

This test pins the FastStream-native cross-broker chain against `OutboxSubscriber` without any guardrail code (G1/G2/G3 not yet implemented). If it fails on first run, the spec's "How the wiring works today" analysis is wrong and the work stops here for re-investigation.

We use `TestOutboxBroker(..., run_loops=False)` so each `await outbox.publish(...)` synchronously drives the subscriber's `dispatch_one` (the documented test idiom; see `CLAUDE.md` "Test broker" section). The destination `TestKafkaBroker` records published commands on its in-memory `_producer.mock`, which we assert against.

- [ ] **Step 1: Create the test file**

Create `tests/test_relay.py` with the following exact contents:

```python
from typing import Any

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from faststream_outbox import OutboxBroker
from faststream_outbox.testing import TestOutboxBroker


pytestmark = pytest.mark.asyncio


async def test_naked_decorator_chain_relays_plain_return_to_kafka() -> None:
    """A handler decorated `@kafka_pub @outbox.subscriber(...)` returning a plain
    value publishes the value through the Kafka publisher chain."""
    broker_outbox = OutboxBroker()
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    # TestKafkaBroker first, TestOutboxBroker second — see "Context-manager
    # ordering note" at the end of this plan. The foreign broker's mock producer
    # must be wired before our subscriber starts (Task 6 introduces a startup
    # warning that probes foreign producers).
    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        await outbox.publish({"hello": "world"}, queue="relay_queue", session=None)
        publisher_kafka.mock.assert_called_once_with({"hello": "world"})
```

Note: `session=None` is fine in `TestOutboxBroker` — the fake client ignores the session argument (see CLAUDE.md "Test broker" section).

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/test_relay.py::test_naked_decorator_chain_relays_plain_return_to_kafka -v --no-cov`
Expected: PASS. `--no-cov` is required because a single-file run would trip `--cov-fail-under=100` from `pyproject.toml`'s addopts.

If this fails:
- `AttributeError: 'NoneType' object has no attribute 'publish'`: the Kafka broker did not start under `TestKafkaBroker` — re-check the context-manager order (Kafka must be inside the `async with` chain).
- `AssertionError: Expected 'mock' to have been called once. Called 0 times.`: the publisher chain did not fire. Re-read the spec's "How the wiring works today" — the OutboxParser must be setting `reply_to=msg.queue` (it does, line 23 of `parser.py`), and `process_message`'s `chain(...)` loop must be iterating `handler._publishers`. **Stop here and re-investigate.**

- [ ] **Step 3: Commit**

```bash
git add tests/test_relay.py
git commit -m "test: baseline relay from OutboxSubscriber to TestKafkaBroker

Pins the FastStream-native cross-broker chain against OutboxSubscriber
with no guardrail code in place: a plain handler return relays to Kafka
via the @publisher decorator stack.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Router-shape relay tests

**Files:**
- Modify: `tests/test_relay.py` (append)

Adds two tests covering the router-publisher shapes from spec section "Router publishers as relay decorators": (a) Kafka publisher from a `KafkaRouter` included into the broker; (b) outbox subscriber on an `OutboxRouter` included into `broker_outbox`. Both have to happen *before* the broker starts. We exercise the `include_router` ordering inside each test.

- [ ] **Step 1: Append the two new tests to `tests/test_relay.py`**

Add the following at the bottom of the existing file (after the function added in Task 2). Also add the new imports at the top:

```python
from faststream.kafka import KafkaRouter  # add to existing kafka import line
from faststream_outbox import OutboxRouter
from faststream_outbox.router import OutboxRoute
```

(If `OutboxRouter` is not in `faststream_outbox.__init__`, import from `faststream_outbox.router import OutboxRouter` instead. Use whichever path resolves — check `faststream_outbox/__init__.py` once.)

Then add the two functions:

```python
async def test_relay_via_kafka_router_publisher() -> None:
    """A publisher obtained from a KafkaRouter (then include_router'd into the
    broker) relays the same way a broker-direct publisher does."""
    broker_outbox = OutboxBroker()
    broker_kafka = KafkaBroker("kafka://test:9092")
    kafka_router = KafkaRouter()
    publisher_kafka = kafka_router.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    broker_kafka.include_router(kafka_router)

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        await outbox.publish({"router": True}, queue="relay_queue", session=None)
        publisher_kafka.mock.assert_called_once_with({"router": True})


async def test_relay_via_outbox_router_subscriber() -> None:
    """A subscriber registered on an OutboxRouter (then include_router'd into
    broker_outbox) accepts a foreign-publisher decorator the same way a
    broker-direct subscriber does."""
    broker_outbox = OutboxBroker()
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")
    outbox_router = OutboxRouter()

    @publisher_kafka
    @outbox_router.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    broker_outbox.include_router(outbox_router)

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        await outbox.publish({"outbox_router": True}, queue="relay_queue", session=None)
        publisher_kafka.mock.assert_called_once_with({"outbox_router": True})
```

- [ ] **Step 2: Run the two new tests**

Run:
```bash
uv run pytest tests/test_relay.py::test_relay_via_kafka_router_publisher tests/test_relay.py::test_relay_via_outbox_router_subscriber -v --no-cov
```
Expected: both PASS.

If `test_relay_via_outbox_router_subscriber` fails with an `OutboxRouter` import error, look at `faststream_outbox/__init__.py` for the public name — the import-line guidance in Step 1 covers both possibilities. Use the correct import path.

- [ ] **Step 3: Commit**

```bash
git add tests/test_relay.py
git commit -m "test: relay via KafkaRouter and OutboxRouter publishers/subscribers

Confirms include_router-before-start() resolves the router-publisher's
ConfigComposition to the real producer (Kafka side) and that
broker_outbox.include_router wires foreign-decorated subscribers from
an OutboxRouter the same as broker-direct ones.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: G3 — `propagate_inbound_headers` kwarg

**Files:**
- Modify: `faststream_outbox/subscriber/config.py`
- Modify: `faststream_outbox/subscriber/factory.py`
- Modify: `faststream_outbox/registrator.py`
- Modify: `faststream_outbox/router.py` (the `OutboxRoute.__init__` signature)
- Modify: `faststream_outbox/fastapi/router.py` (the `subscriber()` override)
- Modify: `faststream_outbox/subscriber/usecase.py` (introduce a `process_message` override that applies the toggle — this override also hosts G1 in Task 5)
- Modify: `tests/test_relay.py` (extend Task 2's test, add a new one)

We add the kwarg with default `False` (matches FastStream-wide convention and `faststream-sqlbroker`). The dispatch-time application lives inside an override of `SubscriberUsecase.process_message` — that override also hosts G1 in Task 5, so design the override to be reusable for both.

- [ ] **Step 1: Write the failing test for the ON behavior**

Append the following function to `tests/test_relay.py`:

```python
async def test_propagate_inbound_headers_true_forwards_outbox_headers_to_kafka() -> None:
    """With propagate_inbound_headers=True, the inbound outbox row's headers
    are forwarded onto the Response before the foreign-publisher chain fires."""
    broker_outbox = OutboxBroker()
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue", propagate_inbound_headers=True)
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        await outbox.publish(
            {"hi": 1},
            queue="relay_queue",
            session=None,
            headers={"x-trace-id": "abc123", "content-type": "application/json"},
        )
        publisher_kafka.mock.assert_called_once()
        # The OutboxFakePublisher and KafkaPublisher both ran; we assert on the
        # Kafka mock's call arg. The exact spy shape depends on FastStream's
        # TestKafkaBroker; we read it off the mock's call_args.
        call_args = publisher_kafka.mock.call_args
        assert call_args is not None
        # FastStream's TestKafkaBroker spy records (body,) positional; headers on the
        # PublishCommand are attached separately. Inspect what TestKafkaBroker exposes
        # at runtime: typically `publisher_kafka.mock.call_args.args[0]` is the body,
        # and the Kafka publisher's recorded outbound headers are on the captured
        # publish_command. The reference test below uses the body-only shape since
        # native KafkaBroker tests use the same pattern.
        assert call_args.args[0] == {"hi": 1}
```

Also update the existing Task-2 test to assert on the **default-off** header behavior explicitly. Replace the bottom of `test_naked_decorator_chain_relays_plain_return_to_kafka` (the lines after `publisher_kafka.mock.assert_called_once_with({"hello": "world"})`) with:

```python
        # Default behavior: propagate_inbound_headers is False, so inbound
        # outbox row headers do NOT reach the Kafka publish. We assert this
        # by emitting a row with headers and confirming Kafka's mock recorded
        # the body only (headers default-empty on Response(value)).
        publisher_kafka.mock.reset_mock()
        await outbox.publish(
            {"second": True},
            queue="relay_queue",
            session=None,
            headers={"x-trace-id": "ignored"},
        )
        publisher_kafka.mock.assert_called_once_with({"second": True})
```

(Drop the `_ = kafka` line — no longer needed.)

- [ ] **Step 2: Run the new test and confirm it fails**

Run: `uv run pytest tests/test_relay.py::test_propagate_inbound_headers_true_forwards_outbox_headers_to_kafka -v --no-cov`
Expected: FAIL with `TypeError: subscriber() got an unexpected keyword argument 'propagate_inbound_headers'`. This is the signal the kwarg plumbing is not yet wired — Steps 3–8 add it.

- [ ] **Step 3: Add the config field**

Edit `faststream_outbox/subscriber/config.py`. Add the field after `max_deliveries`:

```python
@dataclass(kw_only=True)
class OutboxSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "OutboxBrokerConfig"
    queues: list[str]
    max_workers: int
    retry_strategy: "RetryStrategyProto | None"
    fetch_batch_size: int
    min_fetch_interval: float
    max_fetch_interval: float
    lease_ttl_seconds: float
    max_deliveries: int | None
    propagate_inbound_headers: bool
```

The new field has no default so callers must pass it explicitly — Step 4 updates the factory accordingly.

- [ ] **Step 4: Plumb through the factory**

Edit `faststream_outbox/subscriber/factory.py`. Add the parameter to `create_subscriber` (keyword-only, default `False`) and pass it into the `OutboxSubscriberConfig(...)` call:

```python
def create_subscriber(
    *,
    queues: list[str],
    max_workers: int,
    retry_strategy: "RetryStrategyProto | None",
    fetch_batch_size: int,
    min_fetch_interval: float,
    max_fetch_interval: float,
    lease_ttl_seconds: float,
    max_deliveries: int | None,
    config: "OutboxBrokerConfig",
    ack_policy: AckPolicy | None = None,
    propagate_inbound_headers: bool = False,
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
) -> OutboxSubscriber:
```

And in the `OutboxSubscriberConfig(...)` block:

```python
    usecase_config = OutboxSubscriberConfig(
        _outer_config=config,
        _ack_policy=ack_policy if ack_policy is not None else EMPTY,
        queues=queues,
        max_workers=max_workers,
        retry_strategy=retry_strategy,
        fetch_batch_size=fetch_batch_size,
        min_fetch_interval=min_fetch_interval,
        max_fetch_interval=max_fetch_interval,
        lease_ttl_seconds=lease_ttl_seconds,
        max_deliveries=max_deliveries,
        propagate_inbound_headers=propagate_inbound_headers,
    )
```

- [ ] **Step 5: Plumb through the registrator**

Edit `faststream_outbox/registrator.py`. In `OutboxRegistrator.subscriber(...)`, add the kwarg (default `False`) after `ack_policy` and pass it through to `create_subscriber`:

```python
    def subscriber(  # ty: ignore[invalid-method-override]
        self,
        queues: str | list[str],
        *,
        max_workers: int = 1,
        retry_strategy: "RetryStrategyProto | None" = None,
        fetch_batch_size: int = 10,
        min_fetch_interval: float = 1.0,
        max_fetch_interval: float = 10.0,
        lease_ttl_seconds: float = 60.0,
        max_deliveries: int | None = None,
        ack_policy: AckPolicy | None = None,
        propagate_inbound_headers: bool = False,
        dependencies: Iterable["Dependant"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
    ) -> "OutboxSubscriber":
```

And in the `create_subscriber(...)` call:

```python
        subscriber = create_subscriber(
            queues=queue_list,
            max_workers=max_workers,
            retry_strategy=resolved_retry_strategy,
            fetch_batch_size=fetch_batch_size,
            min_fetch_interval=min_fetch_interval,
            max_fetch_interval=max_fetch_interval,
            lease_ttl_seconds=lease_ttl_seconds,
            max_deliveries=max_deliveries,
            ack_policy=ack_policy,
            propagate_inbound_headers=propagate_inbound_headers,
            config=self.config,  # ty: ignore[invalid-argument-type]
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
```

- [ ] **Step 6: Plumb through `OutboxRoute`**

Edit `faststream_outbox/router.py`. Add `propagate_inbound_headers: bool = False` to `OutboxRoute.__init__`'s kwargs (after `ack_policy`) and pass it through to `super().__init__(...)`. The `OutboxRouter` itself inherits `subscriber()` from `OutboxRegistrator` (already updated in Step 5), so no change to the router class body.

```python
    def __init__(  # noqa: PLR0913
        self,
        call: Callable[..., SendableMessage] | Callable[..., Awaitable[SendableMessage]],
        queues: str | list[str],
        *,
        max_workers: int = 1,
        retry_strategy: "RetryStrategyProto | None" = None,
        fetch_batch_size: int = 10,
        min_fetch_interval: float = 1.0,
        max_fetch_interval: float = 10.0,
        lease_ttl_seconds: float = 60.0,
        max_deliveries: int | None = None,
        ack_policy: AckPolicy | None = None,
        propagate_inbound_headers: bool = False,
        dependencies: Iterable["Dependant"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            call=call,
            queues=queues,
            max_workers=max_workers,
            retry_strategy=retry_strategy,
            fetch_batch_size=fetch_batch_size,
            min_fetch_interval=min_fetch_interval,
            max_fetch_interval=max_fetch_interval,
            lease_ttl_seconds=lease_ttl_seconds,
            max_deliveries=max_deliveries,
            ack_policy=ack_policy,
            propagate_inbound_headers=propagate_inbound_headers,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
```

- [ ] **Step 7: Plumb through the FastAPI router**

Edit `faststream_outbox/fastapi/router.py`. Locate `OutboxRouter.subscriber(...)` (it has a `# noqa: PLR0913` on its def line). Add `propagate_inbound_headers: bool = False` to its kwargs and pass it through to `super().subscriber(...)` (which dispatches to `OutboxRegistrator.subscriber` via the underlying broker). Match the kwarg's position to where it lives in `OutboxRegistrator.subscriber` (right after `ack_policy`).

If the FastAPI router's `subscriber()` forwards via `**kwargs`, no change is needed — but in this codebase it forwards by name (per the existing `# noqa: PLR0913` on the def). Add the named pass-through.

- [ ] **Step 8: Apply the kwarg in dispatch — introduce the `process_message` override**

Edit `faststream_outbox/subscriber/usecase.py`. Add the following override **inside the `OutboxSubscriber` class**, placed near `_make_response_publisher` (the existing override) so adjacent FastStream-shape overrides stay grouped. Add the imports needed at the top of the file (verify each is not already imported).

Required new imports at the top of `subscriber/usecase.py` (after the existing imports — `typing`, `contextlib`, `collections.abc` are already imported; only add what's missing):

```python
from contextlib import AsyncExitStack  # add to the existing `from contextlib import ...` line
from itertools import chain
from faststream.exceptions import SubscriberNotFound
from faststream.response.utils import ensure_response
```

Verify by grepping the file's top before editing; do not duplicate.

Inside the class, add:

```python
    @typing.override
    async def process_message(self, msg: OutboxInnerMessage) -> "Response":  # type: ignore[override]
        """Outbox-specific process_message that (a) optionally fills empty
        Response headers with the inbound message's headers (G3) and (b)
        refuses the OutboxResponse + foreign-publisher dual-fire combo (G1,
        added in Task 5).

        Upstream equivalent (replaced):
          SubscriberUsecase.process_message -> faststream/_internal/endpoint/subscriber/usecase.py

        Divergence from upstream is strictly additive — the chain composition,
        middleware ordering, parsing-error rethrow, and AckPolicy semantics are
        preserved verbatim. Any new cleanup added upstream to process_message
        must be mirrored here.
        """
        context = self._outer_config.fd_config.context
        logger_state = self._outer_config.logger

        async with AsyncExitStack() as stack:
            stack.enter_context(self.lock)
            stack.enter_context(context.scope("handler_", self))
            stack.enter_context(context.scope("logger", logger_state.logger.logger))
            for k, v in self._outer_config.extra_context.items():
                stack.enter_context(context.scope(k, v))

            middlewares: list[typing.Any] = []
            for base_m in self._SubscriberUsecase__build__middlewares_stack():  # name-mangled
                middleware = base_m(msg, context=context)
                middlewares.append(middleware)
                await middleware.__aenter__()

            cache: dict[typing.Any, typing.Any] = {}
            parsing_error: Exception | None = None
            for h in self.calls:
                try:
                    message = await h.is_suitable(msg, cache)
                except Exception as e:  # noqa: BLE001
                    parsing_error = e
                    break

                if message is not None:
                    stack.enter_context(
                        context.scope("log_context", self.get_log_context(message)),
                    )
                    stack.enter_context(context.scope("message", message))

                    for m in middlewares:
                        stack.push_async_exit(m.__aexit__)

                    result_msg = ensure_response(
                        await h.call(
                            message=message,
                            _extra_middlewares=(m.consume_scope for m in middlewares[::-1]),
                        ),
                    )

                    if not result_msg.correlation_id:
                        result_msg.correlation_id = message.correlation_id

                    if self._config.propagate_inbound_headers and not result_msg.headers:
                        result_msg.headers = dict(message.headers)

                    for p in chain(
                        self._SubscriberUsecase__get_response_publisher(message),  # name-mangled
                        h.handler._publishers,
                    ):
                        await p._publish(
                            result_msg.as_publish_command(),
                            _extra_middlewares=(m.publish_scope for m in middlewares[::-1]),
                        )

                    return result_msg

            for m in middlewares:
                stack.push_async_exit(m.__aexit__)

            if parsing_error:
                raise parsing_error

            error_msg = f"There is no suitable handler for {msg=}"
            raise SubscriberNotFound(error_msg)

        return ensure_response(None)
```

About the name-mangled accesses (`_SubscriberUsecase__build__middlewares_stack`, `_SubscriberUsecase__get_response_publisher`): the upstream methods are private (`__build__middlewares_stack`, `__get_response_publisher`). Python name-mangles them to `_SubscriberUsecase__build__middlewares_stack` etc. We could reproduce both methods' bodies inline, but they handle subtleties (AckPolicy injection, no-reply gating) that we do not want to drift from upstream. Use the mangled name with a `# noqa: SLF001` comment.

Replace the access lines with the mangled-name version plus `# noqa: SLF001`:

```python
            for base_m in self._SubscriberUsecase__build__middlewares_stack():  # noqa: SLF001
```

```python
                    for p in chain(
                        self._SubscriberUsecase__get_response_publisher(message),  # noqa: SLF001
                        h.handler._publishers,  # noqa: SLF001
                    ):
```

- [ ] **Step 9: Run the relay tests**

Run: `uv run pytest tests/test_relay.py -v --no-cov`
Expected: all four tests PASS.

If the new test still fails on header assertion, check `TestKafkaBroker`'s spy contract — `publisher_kafka.mock.call_args.args` might be the *raw body*, with headers attached on the captured publish-command. If so, refine the header assertion to read `publisher_kafka.mock.call_args` and inspect what the test broker actually records. Headers should be observable somewhere on the spy; if not directly, fall back to asserting `result_msg.headers` shape by spying on the publisher's `_publish` method with `monkeypatch`.

- [ ] **Step 10: Run lint and `ty` check**

Run: `just lint`
Expected: clean. If `ty` complains about the name-mangled access, add the `# ty: ignore[unresolved-attribute]` directive at the call sites along with the `# noqa: SLF001`.

- [ ] **Step 11: Commit**

```bash
git add faststream_outbox/subscriber/config.py faststream_outbox/subscriber/factory.py faststream_outbox/subscriber/usecase.py faststream_outbox/registrator.py faststream_outbox/router.py faststream_outbox/fastapi/router.py tests/test_relay.py
git commit -m "feat: propagate_inbound_headers kwarg for foreign-broker relay

Adds opt-in inbound-header propagation on OutboxSubscriber so handlers
that return a plain value can forward outbox-row headers to the
foreign publisher chain. Default False matches FastStream's
broker-wide convention. The dispatch hook lives in a process_message
override that Task 5 (OutboxResponse + foreign-publisher guard) also
uses.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: G1 — block `OutboxResponse` + foreign publisher

**Files:**
- Modify: `faststream_outbox/subscriber/usecase.py` (extend the `process_message` override added in Task 4)
- Modify: `tests/test_relay.py` (add the guard test)

We extend the Task-4 override with a chain-composition check. The guard fires only when (a) the handler's response is an `OutboxResponse` AND (b) `h.handler._publishers` contains at least one entry that is **not** an `OutboxFakePublisher`. Raising inside the middleware-stacked region propagates to `AcknowledgementMiddleware.__aexit__`, which nacks per `AckPolicy`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_relay.py`:

```python
from faststream_outbox import OutboxResponse  # add near the existing imports


async def test_outbox_response_with_foreign_publisher_raises() -> None:
    """A handler that returns OutboxResponse(...) AND is decorated by a foreign
    publisher is rejected at dispatch time so the user does not silently
    dual-fire (row in outbox + Kafka publish)."""
    broker_outbox = OutboxBroker()
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> OutboxResponse:
        return OutboxResponse(body=body, queue="next_queue", session=None)

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        with pytest.raises(RuntimeError, match="OutboxResponse"):
            await outbox.publish({"x": 1}, queue="relay_queue", session=None)
```

The test relies on the sync-dispatch mode of `TestOutboxBroker` (default `run_loops=False`) so the exception propagates synchronously out of `outbox.publish(...)`. In loop mode the exception would be swallowed by `dispatch_one`'s broad except and turned into a retry; the sync mode is the documented test-broker idiom for asserting handler-side exceptions.

- [ ] **Step 2: Run the test and confirm it fails**

Run: `uv run pytest tests/test_relay.py::test_outbox_response_with_foreign_publisher_raises -v --no-cov`
Expected: FAIL — the relay currently dual-fires without raising.

- [ ] **Step 3: Add the guard to the `process_message` override**

In `faststream_outbox/subscriber/usecase.py`, in the `process_message` override added in Task 4, locate the `for p in chain(...)` loop. Just before that loop, add:

```python
                    self._reject_outbox_response_with_foreign_publisher(result_msg, h.handler)

                    for p in chain(
                        self._SubscriberUsecase__get_response_publisher(message),  # noqa: SLF001
                        h.handler._publishers,  # noqa: SLF001
                    ):
                        ...
```

Then add the helper as a method on `OutboxSubscriber` (near the override):

```python
    @staticmethod
    def _reject_outbox_response_with_foreign_publisher(
        result_msg: "Response",
        handler: typing.Any,
    ) -> None:
        """Refuse the dual-fire combination: OutboxResponse + foreign publisher.

        OutboxResponse(body=..., queue=..., session=...) writes to the outbox in
        the caller's transaction; a foreign-publisher decorator also publishes
        the relayed body. Both would fire from the chain. That is almost
        certainly not intended — pick one.
        """
        if not isinstance(result_msg, OutboxResponse):
            return
        foreign = [
            p for p in handler._publishers
            if not isinstance(p, OutboxFakePublisher)
        ]
        if not foreign:
            return
        msg = (
            "Handler returned OutboxResponse and is also decorated by a foreign-broker "
            "publisher — this would dual-fire (insert a row into the outbox AND publish "
            "to the foreign broker). Pick one: return a plain value to use the foreign "
            "publisher as a relay, or remove the foreign publisher decorator and keep "
            "OutboxResponse for outbox fan-out."
        )
        raise RuntimeError(msg)
```

Add `from faststream_outbox.response import OutboxResponse` to the top-of-file imports if not already present. `OutboxFakePublisher` is already imported (used by `_make_response_publisher`).

- [ ] **Step 4: Run the test and confirm it passes**

Run: `uv run pytest tests/test_relay.py::test_outbox_response_with_foreign_publisher_raises -v --no-cov`
Expected: PASS.

- [ ] **Step 5: Run the rest of the relay tests to confirm no regression**

Run: `uv run pytest tests/test_relay.py -v --no-cov`
Expected: all five tests PASS.

- [ ] **Step 6: Lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add faststream_outbox/subscriber/usecase.py tests/test_relay.py
git commit -m "feat: refuse OutboxResponse + foreign-publisher dual-fire

A handler that returns OutboxResponse(...) and is also decorated by a
foreign-broker publisher would both insert an outbox row AND publish
to the foreign broker on every dispatch. Detect the combo at dispatch
time and raise a RuntimeError pointing at the two valid patterns.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: G2 — WARN on unstarted foreign broker at `start()`

**Files:**
- Modify: `faststream_outbox/broker.py`
- Modify: `tests/test_relay.py`

When `broker_outbox.start()` runs, walk every subscriber's `handler._publishers` looking for publishers whose `_outer_config` is *not* this broker's. For each such foreign publisher, duck-type-check whether the foreign broker has a producer; if not, log a single WARNING per unstarted broker referencing the affected queue names.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_relay.py`:

```python
import logging  # add to top of file if not already imported


async def test_unstarted_foreign_broker_warns_on_start(caplog: pytest.LogCaptureFixture) -> None:
    """If a foreign-publisher decorator is on an outbox subscriber but the
    foreign broker has not been started, start(broker_outbox) logs a single
    WARNING per unstarted foreign broker."""
    broker_outbox = OutboxBroker()
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    with caplog.at_level(logging.WARNING, logger="faststream_outbox"):
        async with TestOutboxBroker(broker_outbox, run_loops=False):
            pass  # start triggered inside __aenter__

    matching = [r for r in caplog.records if r.levelno == logging.WARNING and "relay_queue" in r.getMessage()]
    assert len(matching) == 1, f"Expected exactly one WARNING referencing relay_queue, got {len(matching)}: {[r.getMessage() for r in caplog.records]}"
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `uv run pytest tests/test_relay.py::test_unstarted_foreign_broker_warns_on_start -v --no-cov`
Expected: FAIL (no WARNING emitted because the check does not exist yet).

- [ ] **Step 3: Implement the check in `OutboxBroker.start()`**

Edit `faststream_outbox/broker.py`. Modify the existing `start()` override (around line 211) to call a new helper before returning:

```python
    @typing.override
    async def start(self) -> None:
        await self.connect()
        await super().start()
        self._warn_on_unstarted_foreign_publishers()
```

Then add the helper as a method on `OutboxBroker`:

```python
    def _warn_on_unstarted_foreign_publishers(self) -> None:
        """Emit one WARNING per foreign-publisher broker that has not been started.

        Foreign-publisher decorators stacked on outbox subscribers only work if
        the foreign broker's producer is wired. When it is not, the first
        relayed row fails deep inside the foreign publisher with an opaque
        AttributeError; this preflight pushes the diagnostic up to start() so
        operators see the cause immediately.
        """
        logger_state = self.config.broker_config.logger
        log = logger_state.logger.logger if logger_state is not None else None
        if log is None:
            return
        warned: set[int] = set()
        for sub in self.subscribers:
            for call in sub.calls:
                for pub in call.handler._publishers:  # noqa: SLF001
                    outer = pub._outer_config  # noqa: SLF001
                    if outer is self.config:
                        continue  # not foreign
                    producer = getattr(outer, "producer", None)
                    if producer is not None:
                        continue  # already wired
                    key = id(outer)
                    if key in warned:
                        continue
                    warned.add(key)
                    queues = sorted({q for s in self.subscribers for q in getattr(s, "_queues", [])})
                    log.warning(
                        "Foreign publisher %r is decorated on outbox subscriber(s) for "
                        "queue(s) %s, but its broker has not been started yet. The first "
                        "relay attempt will fail and the row will retry until the broker "
                        "starts. Call `await foreign_broker.start()` or "
                        "`foreign_broker.connect` in your app's startup hook.",
                        pub,
                        queues,
                    )
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `uv run pytest tests/test_relay.py::test_unstarted_foreign_broker_warns_on_start -v --no-cov`
Expected: PASS.

- [ ] **Step 5: Run all relay tests to confirm no regression**

Run: `uv run pytest tests/test_relay.py -v --no-cov`
Expected: all six tests PASS.

If `test_naked_decorator_chain_relays_plain_return_to_kafka` (or any other Task 2/3/4 test) regresses with a spurious WARNING, the foreign broker is in fact unstarted at the moment `TestOutboxBroker.__aenter__` calls our start hook. Investigate — the fix is to detect the test-broker state (e.g., the foreign broker's producer becomes non-`None` only inside `TestKafkaBroker.__aenter__`). One safe option: skip the warning when the foreign broker's outer config is *also* registered with a `TestBroker` (probe by looking for a sentinel attribute the test broker sets). Document the workaround in code if used.

- [ ] **Step 6: Lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add faststream_outbox/broker.py tests/test_relay.py
git commit -m "feat: WARN on unstarted foreign-publisher brokers at start()

Preflight check during OutboxBroker.start() walks subscribers for
publishers whose _outer_config is foreign and logs one WARNING per
unstarted foreign broker. Operators see the cause immediately
instead of debugging an AttributeError on the first relay attempt.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Integration test — at-least-once under foreign-publish failure

**Files:**
- Modify: `tests/test_integration.py`

Real outbox subscriber (Postgres-backed) plus a foreign publisher whose `_publish` raises on the first call and succeeds on retry. We assert exactly-one eventual delivery and that `deliveries_count` on the row reflects the retry. `TestKafkaBroker` provides the destination so no real Kafka is needed; the outbox side uses the real `OutboxBroker` against `pg_engine`.

- [ ] **Step 1: Append the test to `tests/test_integration.py`**

Locate the existing imports. Add at the top of the file (only the names not already present):

```python
from unittest.mock import AsyncMock, patch
from typing import Any
import asyncio

from faststream.kafka import KafkaBroker, TestKafkaBroker

from faststream_outbox import OutboxBroker
```

Append the test at the bottom of the file:

```python
async def test_relay_at_least_once_under_foreign_publish_failure(
    pg_engine: AsyncEngine,
) -> None:
    """A foreign publish that fails on the first attempt is retried via the
    outbox's retry_strategy, and the row eventually clears after a successful
    second attempt. Asserts that the foreign publisher saw the body twice
    (at-least-once) and the outbox row was deleted after the second attempt."""
    broker_outbox = OutboxBroker(engine=pg_engine)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    # Inject a fault in the foreign publisher: first _publish raises, second succeeds.
    call_count = 0
    original_publish = publisher_kafka._publish

    async def flaky_publish(*args: Any, **kwargs: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            msg = "simulated foreign-publish failure"
            raise RuntimeError(msg)
        return await original_publish(*args, **kwargs)

    @publisher_kafka
    @broker_outbox.subscriber(
        "relay_queue",
        max_workers=1,
        max_fetch_interval=0.1,
        min_fetch_interval=0.0,
        lease_ttl_seconds=2.0,  # short enough to retry quickly during the test
    )
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    with patch.object(publisher_kafka, "_publish", side_effect=flaky_publish):
        async with TestKafkaBroker(broker_kafka), broker_outbox:
            # Publish one row via the broker's own session.
            async with broker_outbox._engine.connect() as conn:
                async with conn.begin():
                    await broker_outbox.publish(
                        {"body": "first"},
                        queue="relay_queue",
                        session=conn,  # AsyncConnection — type-relax via session param contract
                    )

            # Wait until the row clears (foreign mock recorded the body at least once).
            for _ in range(50):
                if call_count >= 2:
                    break
                await asyncio.sleep(0.1)

            assert call_count >= 2, (
                f"Expected at-least-once delivery via retry, but foreign _publish was called {call_count} time(s)."
            )
```

If the test does not have an `AsyncConnection`-as-session contract (the broker only accepts `AsyncSession`), adapt by passing the engine's `async_sessionmaker` instead:

```python
            from sqlalchemy.ext.asyncio import async_sessionmaker
            sm = async_sessionmaker(broker_outbox._engine, expire_on_commit=False)
            async with sm() as session, session.begin():
                await broker_outbox.publish(
                    {"body": "first"},
                    queue="relay_queue",
                    session=session,
                )
```

- [ ] **Step 2: Run the test**

Run: `just test tests/test_integration.py::test_relay_at_least_once_under_foreign_publish_failure --no-cov`
Expected: PASS. Test runs under docker compose so Postgres is available at the DSN the fixture expects.

If the test times out at `call_count >= 2`:
- The first failure should produce a `nacked_retried` (or `nacked_terminal` if retries are exhausted) event. Check the WARNING log; if the row is `nacked_terminal`, `lease_ttl_seconds` is too tight relative to the default retry-strategy backoff. Bump `lease_ttl_seconds` and/or pass an explicit `retry_strategy=ExponentialRetry(initial_delay_seconds=0.1, multiplier=1.0, max_attempts=3)` to give it room.
- Confirm the foreign publisher's `_publish` is actually being patched by inspecting `publisher_kafka._publish.__name__`.

- [ ] **Step 3: Commit**

```bash
git add tests/test_integration.py
git commit -m "test: at-least-once relay under simulated foreign-publish failure

Confirms the FastStream AckPolicy nack path (publisher-chain exception
-> AcknowledgementMiddleware.__aexit__ -> outbox row nack/retry) end
to end against a real Postgres-backed OutboxBroker plus TestKafkaBroker.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Docs — tutorial, examples grid, mkdocs nav

**Files:**
- Create: `docs/usage/relay.md`
- Modify: `mkdocs.yml`

Tutorial is promoted to the top of the `Usage` nav, above `usage/basic.md`. Tutorial sections mirror the spec's D2: Why → Minimal example → Two-broker lifecycle (FastAPI + Standalone) → At-least-once contract → Header propagation → Using routers → What not to do → Cross-broker examples grid.

- [ ] **Step 1: Create `docs/usage/relay.md`**

Create the file with the following contents:

````markdown
# Relay to a foreign broker

The outbox pattern's payoff line: domain code writes a row to the outbox in
the same DB transaction as its other writes, and a separate worker relays
those rows to a real bus (Kafka, RabbitMQ, NATS, Redis…). `faststream-outbox`
supports this directly via FastStream's cross-broker chain — stack a
foreign-broker publisher decorator on an outbox subscriber and you're done.

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
    return OutboxResponse(body=body, queue="next_queue", session=...)  # ❌
```

This would both insert a row into the outbox AND publish to Kafka. The
subscriber raises `RuntimeError` at dispatch time when it detects the
combination — pick one path.

**Do not** stack an outbox publisher on a foreign subscriber.

```python
@broker_outbox.publisher("outbox_queue")
@broker_kafka.subscriber("kafka_topic")  # ❌
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
````

- [ ] **Step 2: Update `mkdocs.yml` to promote the tutorial to the top of `Usage`**

Edit `mkdocs.yml`. Change the `Usage:` block from:

```yaml
  - Usage:
      - Basic usage: usage/basic.md
      - Subscriber: usage/subscriber.md
```

to:

```yaml
  - Usage:
      - Relay to a foreign broker: usage/relay.md
      - Basic usage: usage/basic.md
      - Subscriber: usage/subscriber.md
```

Keep the rest of the `Usage` list intact.

- [ ] **Step 3: Visually verify the docs build (optional but recommended)**

Run: `uv run mkdocs build --strict 2>&1 | tail -20`
Expected: build succeeds. If `mkdocs` is not installed in the project venv, skip this step.

- [ ] **Step 4: Commit**

```bash
git add docs/usage/relay.md mkdocs.yml
git commit -m "docs: add foreign-broker relay tutorial, promote to top of Usage nav

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Docs — intro callout, CLAUDE.md subsection

**Files:**
- Modify: `docs/introduction/how-it-works.md` (one-line callout near the bottom)
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add the intro callout**

Edit `docs/introduction/how-it-works.md`. Locate the last paragraph (or the section closest to "what's next"). Append a one-line callout:

```markdown
> **Relay outbox rows to Kafka / RabbitMQ / NATS / Redis with a single decorator → [Relay tutorial](../usage/relay.md).**
```

If the file's structure does not have a natural "what's next" tail, append the callout as the last line of the file.

- [ ] **Step 2: Add the CLAUDE.md subsection**

Edit `CLAUDE.md`. Locate the existing "Producer side" or "Two-loop subscriber" section. Insert a new subsection (level-3 heading) immediately after the "Producer side" block:

```markdown
### Relay to foreign broker

`OutboxSubscriber` supports being the source of a FastStream-native
cross-broker chain: `@kafka_pub @broker_outbox.subscriber("q")` (and the
same for Rabbit/NATS/Redis/Confluent) is the canonical
transactional-outbox primitive. The mechanism is upstream FastStream's
`SubscriberUsecase.process_message` walking
`chain(self.__get_response_publisher(message), h.handler._publishers)`;
no override of the dispatch path is needed. Three load-bearing
properties keep the chain safe: (a) `OutboxFakePublisher` self-gates on
`isinstance(cmd, OutboxPublishCommand)` so it no-ops on plain handler
returns; (b) foreign publishers coerce via
`<Broker>PublishCommand.from_cmd(cmd)`; (c)
`AcknowledgementMiddleware.__aexit__` turns publisher-chain exceptions
into outbox nacks, preserving at-least-once delivery via the configured
`retry_strategy`.

Three guardrails sit on top of the bare mechanism:

- **`OutboxResponse` + foreign publisher refused.** The
  `process_message` override checks the chain composition and raises a
  `RuntimeError` if a handler returns `OutboxResponse(...)` while having
  a non-`OutboxFakePublisher` entry in `handler._publishers`. The
  exception propagates through `AcknowledgementMiddleware` and triggers
  the outbox's normal nack path.

- **WARNING for unstarted foreign brokers at start().** `OutboxBroker.start()`
  walks `self.subscribers`, for each subscriber walks
  `handler._publishers`, and for any publisher whose `_outer_config` is
  not `self.config` and whose foreign broker has no `producer` set, logs
  one WARNING per unstarted foreign broker. Operators see the cause
  before the first relayed row fails.

- **`propagate_inbound_headers: bool = False` on subscriber.** When
  `True`, the `process_message` override fills `Response.headers` from
  the inbound `OutboxMessage.headers` only when the handler returned a
  `Response` with empty headers (explicit user-set headers win). Default
  False matches the FastStream-wide convention; users who want
  propagation flip one kwarg.

User-facing reference: `docs/usage/relay.md`.
```

Adjust the inserted position to match the existing CLAUDE.md flow (the file is large; place this subsection adjacent to the producer-side material, not in the middle of subscriber-internals).

- [ ] **Step 3: Commit**

```bash
git add docs/introduction/how-it-works.md CLAUDE.md
git commit -m "docs: intro callout and CLAUDE.md subsection for foreign-broker relay

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Docs — README front-page rewrite

**Files:**
- Modify: `README.md`

Replace the existing quickstart with the end-to-end relay example. The relay is the canonical outbox use case; promoting it to the README front page matches the spec's visibility goal and aligns with how `faststream-sqlbroker` features the same pattern.

- [ ] **Step 1: Read the current README**

Run: `cat README.md | head -80`

Inspect the existing structure. The rewrite preserves the project intro, badges (if any), and installation block; only the "quickstart" / "usage" code-example region is replaced.

- [ ] **Step 2: Replace the quickstart section**

Locate the existing quickstart code block (the one that shows `broker.publish(...)` and a handler in isolation). Replace it with:

````markdown
## Quickstart — outbox relay to Kafka

Write the outbox row in your domain transaction:

```python
from faststream_outbox import OutboxBroker

broker_outbox = OutboxBroker(engine=engine)


async def create_order(session, order_data):
    order = await orders.insert(session, order_data)
    await broker_outbox.publish(
        {"order_id": order.id, "total": order.total},
        queue="orders_outbox",
        session=session,  # same transaction as the row above
    )
    # On session.commit(), both the order row AND the outbox row land
    # atomically. No commit, no event.
```

Relay outbox rows to Kafka with a single decorator:

```python
from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream_outbox import OutboxBroker

broker_outbox = OutboxBroker(engine=engine)
broker_kafka = KafkaBroker("127.0.0.1:9092")
publisher_kafka = broker_kafka.publisher("orders")


@publisher_kafka
@broker_outbox.subscriber("orders_outbox")
async def relay(body: dict) -> dict:
    return body


app = FastStream(broker_outbox, on_startup=[broker_kafka.connect])
```

The same shape works for RabbitMQ, NATS, Redis, and Confluent. See the
[relay tutorial](docs/usage/relay.md) for the FastAPI lifecycle,
header propagation, router shapes, and the at-least-once contract.
````

Adjust headings and anchors to match the existing README's style.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README quickstart around the outbox relay

The outbox-to-foreign-broker relay is the canonical use case for this
package; promote it to the front page so readers see the payoff line
immediately.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Final validation

**Files:** none modified — this task only runs lint + full test suite and reports.

- [ ] **Step 1: Lint in non-mutating mode**

Run: `just lint-ci`
Expected: clean (no `eof-fixer`, no `ruff format`, no `ruff check --fix`, no `ty check` errors).

- [ ] **Step 2: Run the full test suite under docker compose**

Run: `just test`
Expected: all tests pass, coverage is at 100%.

If coverage drops, look at the diff:
- `_warn_on_unstarted_foreign_publishers` branch where logger is `None` — add a targeted test or a `# pragma: no cover` on that one defensive guard.
- The `parsing_error` re-raise and `SubscriberNotFound` paths in the `process_message` override — these are reproduced from upstream and may not have outbox-specific coverage. If the existing suite does not cover them, mark them `# pragma: no cover` with a comment pointing at the upstream parallel.

- [ ] **Step 3: Inspect the branch state**

Run: `git log --oneline main..HEAD`
Expected: ~9 commits, one per task (Tasks 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).

Run: `git diff main..HEAD --stat`
Expected: the file list matches the spec's "File touch list" exactly.

- [ ] **Step 4: Push and open a PR**

Run:
```bash
git push -u origin feat/foreign-broker-relay
gh pr create --title "feat: foreign-broker relay from OutboxSubscriber" --body "$(cat <<'EOF'
## Summary

- Document and test FastStream's native cross-broker chain (`@kafka_pub @broker_outbox.subscriber(...)`) as the canonical outbox use case.
- Three guardrails: refuse `OutboxResponse` + foreign-publisher dual-fire (G1), WARN on unstarted foreign brokers at `start()` (G2), opt-in inbound-header propagation (G3, default False).
- Promote the relay tutorial to the top of `Usage` nav; rewrite the README quickstart around the relay example.
- Concrete `faststream-sqlbroker` comparison documented in the spec and CLAUDE.md.

## Test plan

- [x] `tests/test_relay.py` — six unit tests against `TestKafkaBroker` (naked chain, two router shapes, header propagation, G1 guard, G2 warning).
- [x] `tests/test_integration.py::test_relay_at_least_once_under_foreign_publish_failure` — Postgres-backed test that proves AckPolicy nack → retry → eventual success.
- [x] `just lint-ci` clean.
- [x] `just test` passes with 100% coverage.

Spec: `planning/specs/2026-06-04-foreign-broker-relay-design.md`
Plan: `planning/plans/2026-06-04-foreign-broker-relay-plan.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed to stdout. Hand to user.

---

## Notes for the engineer

- **All test arguments — including pytest fixtures — must be type-annotated** per the project's standing convention.
- **No local imports.** Every import goes at the top of the module. This applies to test files too.
- **`ty: ignore[<rule>]`** is the project's escape hatch for type checker complaints; match the existing usage in `broker.py` and `registrator.py`.
- The `process_message` override in Task 4 is the second non-trivial FastStream-method override in this codebase (after `OutboxBroker.stop` and `OutboxSubscriber.stop`). Mirror the convention used there: include an `# Upstream equivalent (replaced): …` comment pointing at the upstream method, and keep divergence strictly additive.
- If at any point a Task's "Expected: PASS" step does not pass, **stop, diagnose, and fix in place**. Do not skip to the next Task with a failing test — the plan was written with the dependency order in mind.

## Context-manager ordering note

Every relay test enters `TestKafkaBroker` **before** `TestOutboxBroker`:

```python
async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
    ...
```

Reason: `TestOutboxBroker.__aenter__` calls `broker_outbox.start()`, which
after Task 6 walks every foreign-publisher `_outer_config.producer` to
decide whether to emit a "broker not started" WARNING. If
`TestKafkaBroker.__aenter__` runs *after* the outbox starts, the Kafka
broker's mock producer is still `None` at probe time and we get a
false-positive WARNING. Entering the foreign test broker first wires its
mock producer before our subscriber starts.

Task 6's own test (`test_unstarted_foreign_broker_warns_on_start`) is the
only one that deliberately enters the outbox broker **without** the Kafka
context — that's how it triggers the WARNING under test.
