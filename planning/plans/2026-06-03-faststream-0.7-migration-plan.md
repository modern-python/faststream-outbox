# FastStream 0.7 Migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `faststream-outbox` to depend on `faststream>=0.7,<0.8`, fix the five mechanically-forced break points (codec attribute on producers, `add_call` lost `middlewares_=`, `create_publisher_fake_subscriber` is now an instance method), and drop the public per-call `middlewares=` kwarg upstream removed.

**Architecture:** Single-version code path (no 0.6 compat shim). All changes land on `chore/faststream-0.7-migration` and are squashed to one bundled commit before opening the PR — the spec calls for a single coherent migration commit. During development, normal commits per task are fine; the final task squashes them.

**Tech Stack:** `uv` for lock/sync; `just` for lint/test recipes; Python 3.13+; FastStream 0.7.x; SQLAlchemy 2.0+ (asyncpg); pytest with `--cov-fail-under=100`.

**Spec:** `planning/specs/2026-06-03-faststream-0.7-migration-design.md` (committed in the previous turn — see `git log --oneline | head -1`).

---

## File map

| Path | Action | Responsibility |
|---|---|---|
| `pyproject.toml` | Modify | Bump pin from `faststream>=0.6,<0.7` to `faststream>=0.7,<0.8` |
| `faststream_outbox/publisher/producer.py` | Modify | Add `codec` attribute to `OutboxProducer` to satisfy 0.7 `ProducerProto` |
| `faststream_outbox/testing.py` | Modify | Add `codec` attribute to `FakeOutboxProducer`; change `create_publisher_fake_subscriber` from `@staticmethod` to instance method |
| `faststream_outbox/registrator.py` | Modify | Drop `middlewares=` kwarg from `subscriber()` + `publisher()`; drop `middlewares_=` from the `add_call(...)` call |
| `faststream_outbox/publisher/factory.py` | Modify | Drop `middlewares=` parameter from `create_publisher` |
| `faststream_outbox/publisher/config.py` | Modify | Drop `middlewares` field from `OutboxPublisherConfig` |
| `faststream_outbox/router.py` | Modify | Drop `middlewares=` kwarg from `OutboxRoute.__init__` (the broker-level `OutboxRouter.middlewares=` STAYS — it routes to `broker_middlewares=`) |
| `faststream_outbox/fastapi/router.py` | Modify | Drop `middlewares=` kwarg from `subscriber()` and `publisher()` overrides; the broker-level `middlewares=` on `OutboxRouter.__init__` STAYS |
| `faststream_outbox/subscriber/factory.py` | Conditional | Verify `AckPolicy.ACK_FIRST` survived; delete the rejection branch + its test only if the enum member is gone |
| `tests/test_fake.py` | Modify | Delete `test_publisher_accepts_middlewares_kwarg` (the only test covering the removed kwarg) |
| `tests/test_unit.py` | Conditional | Update if `_basic_publish`/`_publish` `_extra_middlewares=` kwarg renamed in 0.7 |
| `docs/usage/publisher.md` | Modify | Remove the "Per-publisher `middlewares=` wrap every `publisher.publish(...)` call" sentence at line 103–105 |
| `CLAUDE.md` | Modify | Trim two passages mentioning per-call middlewares (line 37 `broker.publisher(...)` signature + the "*middlewares*" sentence in the same paragraph) |
| `uv.lock` | Auto-updated | Updated by `uv lock --upgrade` in Task 1 |

**Files explicitly NOT modified:**
- `tests/test_middleware_opentelemetry.py` and `tests/test_middleware_prometheus.py` use `OutboxBroker(middlewares=[...])` — that's broker-level middleware, the 0.7-supported shape; leave them.
- `docs/usage/observability.md` uses broker-level `middlewares=` in `OutboxBroker(...)` — same; leave it.
- `README.md` has no per-call middleware references.

---

## Task 1: Branch + pin bump + first-pass install

**Files:**
- Modify: `pyproject.toml` (line 13)
- Auto-modified: `uv.lock`

- [ ] **Step 1: Create the migration branch**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git switch -c chore/faststream-0.7-migration
```

Expected: `Switched to a new branch 'chore/faststream-0.7-migration'`. If the branch already exists from a prior aborted attempt, run `git switch chore/faststream-0.7-migration` and confirm `git status` is clean before proceeding.

- [ ] **Step 2: Bump the pin**

Use Edit on `pyproject.toml`:

`old_string`:
```toml
    "faststream>=0.6,<0.7",
```

`new_string`:
```toml
    "faststream>=0.7,<0.8",
```

- [ ] **Step 3: Resolve and install**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv lock --upgrade-package faststream && uv sync --all-extras --all-groups
```

Expected: `Resolved` line that names a `faststream` version starting with `0.7.`. If resolution fails with a `no version found` error, check PyPI manually (`uv pip index versions faststream`) — the 0.7 release may have been yanked or the version range may need widening.

- [ ] **Step 4: Capture the installed FastStream version + audit the upstream shapes you'll need**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run python -c "
import faststream, inspect
print('faststream version:', faststream.__version__)

# R2: where does CodecProto live, and is it Optional in ProducerProto?
from faststream._internal.producer import ProducerProto
print('ProducerProto annotations:', getattr(ProducerProto, '__annotations__', {}))
try:
    from faststream._internal.codec import CodecProto
    print('CodecProto: faststream._internal.codec.CodecProto')
except ImportError:
    import importlib, pkgutil, faststream._internal
    print('CodecProto NOT in faststream._internal.codec; searching:')
    for m in pkgutil.walk_packages(faststream._internal.__path__, prefix='faststream._internal.'):
        try:
            mod = importlib.import_module(m.name)
            if hasattr(mod, 'CodecProto'):
                print('  found in:', m.name)
        except Exception:
            pass

# R3: did AckPolicy.ACK_FIRST survive?
from faststream.middlewares import AckPolicy
print('AckPolicy members:', [a.name for a in AckPolicy])

# Confirm add_call signature
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
print('add_call sig:', inspect.signature(SubscriberUsecase.add_call))

# Confirm create_publisher_fake_subscriber shape
from faststream._internal.testing.broker import TestBroker
print('create_publisher_fake_subscriber sig:',
      inspect.signature(TestBroker.create_publisher_fake_subscriber))
"
```

Record the output. The remaining tasks make decisions based on:
- **`CODEC_IMPORT_PATH`** — the dotted import for `CodecProto` (e.g. `faststream._internal.codec`).
- **`CODEC_IS_OPTIONAL`** — whether `ProducerProto.__annotations__['codec']` resolves to a non-Optional type (if yes, we'll default to a no-op instance; if Optional, default to `None`).
- **`ACK_FIRST_GONE`** — `True` if `AckPolicy.ACK_FIRST` is not in the members list; controls Task 9.
- **`ADD_CALL_KWARGS`** — confirm `parser_`, `decoder_`, `dependencies_` are present and `middlewares_` is absent (matches spec).
- **`CREATE_PUB_FAKE_SIG`** — confirm `self` is the first parameter (instance method, not static).

If any decision differs from spec assumptions (e.g. `middlewares_` is still in `add_call`), STOP — re-open the spec to revise before continuing.

- [ ] **Step 5: First-pass lint (expect failures)**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && just lint-ci 2>&1 | tee /tmp/lint-after-bump.txt | tail -40
```

Expected: failures in `ty check` against the files listed in the file map above (missing `codec` attr on producers, wrong static/instance method, etc.). Keep `/tmp/lint-after-bump.txt` for reference while you do Tasks 2–8 — it's your punch list.

If `ruff format --check` complains about unrelated files, fix those formatting issues here (one tiny formatting commit doesn't fight the single-commit goal — the squash at Task 11 collapses everything).

- [ ] **Step 6: Stage + interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add pyproject.toml uv.lock && git commit -m "wip: bump faststream pin to >=0.7,<0.8"
```

(This is an interim commit; Task 11 squashes the branch to a single commit.)

---

## Task 2: Satisfy `ProducerProto.codec` on `OutboxProducer`

**Files:**
- Modify: `faststream_outbox/publisher/producer.py` (around lines 36–56)

This task uses the `CODEC_IMPORT_PATH` and `CODEC_IS_OPTIONAL` values captured in Task 1, Step 4.

- [ ] **Step 1: Write a failing test asserting `OutboxProducer` satisfies `ProducerProto`**

Add to `tests/test_unit.py` (locate the section that tests `OutboxProducer` — there's an existing block; add this near the existing producer tests, or at end of file if none cluster nicely):

```python
def test_outbox_producer_satisfies_producer_proto() -> None:
    """Phase-1 0.7 compat: ProducerProto in 0.7 requires a `codec` attribute."""
    from faststream._internal.producer import ProducerProto
    from sqlalchemy import MetaData

    from faststream_outbox import make_outbox_table
    from faststream_outbox.publisher.producer import OutboxProducer

    table = make_outbox_table(MetaData())
    producer = OutboxProducer(table=table, parser=None, decoder=None)
    # Attribute access must not raise; instance must structurally match.
    assert hasattr(producer, "codec")
    assert hasattr(producer, "_parser")
    assert hasattr(producer, "_decoder")
    assert isinstance(producer, ProducerProto)
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run pytest tests/test_unit.py::test_outbox_producer_satisfies_producer_proto -x --no-cov -v
```

Expected: `FAILED` with `AssertionError` on `hasattr(producer, "codec")` or `isinstance` check.

- [ ] **Step 3: Add the `codec` attribute**

If `CODEC_IS_OPTIONAL` is **True** (the Optional path — simpler):

Use Edit on `faststream_outbox/publisher/producer.py`:

`old_string`:
```python
    def __init__(
        self,
        *,
        table: Table,
        parser: typing.Optional["CustomCallable"],
        decoder: typing.Optional["CustomCallable"],
        metrics_recorder: MetricsRecorder = _noop_recorder,
    ) -> None:
        self._table = table
        self._channel = f"outbox_{table.name}"
        self.serializer: SerializerProto | None = None
        default = OutboxParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)
        self._metrics_recorder = metrics_recorder
```

`new_string`:
```python
    def __init__(
        self,
        *,
        table: Table,
        parser: typing.Optional["CustomCallable"],
        decoder: typing.Optional["CustomCallable"],
        metrics_recorder: MetricsRecorder = _noop_recorder,
    ) -> None:
        self._table = table
        self._channel = f"outbox_{table.name}"
        self.serializer: SerializerProto | None = None
        # ProducerProto[0.7] requires a `codec` attribute. The outbox owns its
        # own encoding pipeline (_encode_payload) and never reads this attribute
        # at runtime — it exists solely to satisfy the protocol.
        self.codec: "CodecProto | None" = None
        default = OutboxParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)
        self._metrics_recorder = metrics_recorder
```

And add the import (inside the TYPE_CHECKING block at lines ~29–33):

`old_string`:
```python
if typing.TYPE_CHECKING:
    from collections.abc import Mapping

    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.types import AsyncCallable, CustomCallable
```

`new_string`:
```python
if typing.TYPE_CHECKING:
    from collections.abc import Mapping

    from fast_depends.library.serializer import SerializerProto
    from <CODEC_IMPORT_PATH> import CodecProto  # noqa: TC003 — protocol-only annotation
    from faststream._internal.types import AsyncCallable, CustomCallable
```

Replace `<CODEC_IMPORT_PATH>` with the dotted import recorded in Task 1, Step 4 (likely `faststream._internal.codec`). The import lives under `TYPE_CHECKING` because we never reference `CodecProto` at runtime.

If `CODEC_IS_OPTIONAL` is **False** (the non-Optional path):

Same edits as above, except:
- The runtime import (NOT under TYPE_CHECKING) must bring in the upstream-provided default codec instance. Recipe:
  ```python
  from <CODEC_IMPORT_PATH> import <DefaultCodecName>
  ```
  Inspect what upstream ships next to `CodecProto` — likely a `JSONCodec()` or similar. Use `dir()` on the codec module to find it:
  ```bash
  uv run python -c "import importlib; m = importlib.import_module('<CODEC_IMPORT_PATH>'); print([n for n in dir(m) if not n.startswith('_')])"
  ```
- Initialize the attribute as `self.codec: CodecProto = <DefaultCodecName>()`.

- [ ] **Step 4: Run the test, expect pass**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run pytest tests/test_unit.py::test_outbox_producer_satisfies_producer_proto -x --no-cov -v
```

Expected: `PASSED`.

- [ ] **Step 5: Run `ty` on the producer file**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run ty check faststream_outbox/publisher/producer.py
```

Expected: no errors. If `ty` complains that the `codec` attribute conflicts with the abstract class, double-check the annotation form (Optional vs non-Optional).

- [ ] **Step 6: Interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add faststream_outbox/publisher/producer.py tests/test_unit.py && git commit -m "wip: add codec attribute to OutboxProducer for 0.7 ProducerProto"
```

---

## Task 3: Satisfy `ProducerProto.codec` on `FakeOutboxProducer`

**Files:**
- Modify: `faststream_outbox/testing.py` (around line 281)

- [ ] **Step 1: Write a failing test**

Add to `tests/test_fake.py` (cluster near other producer/broker construction tests):

```python
def test_fake_outbox_producer_satisfies_producer_proto() -> None:
    """Phase-1 0.7 compat: FakeOutboxProducer needs the same `codec` attribute."""
    from faststream._internal.producer import ProducerProto

    from faststream_outbox.testing import FakeOutboxClient, FakeOutboxProducer

    broker = _make_broker()  # uses the existing helper at top of test_fake.py
    fc = FakeOutboxClient()
    fp = FakeOutboxProducer(fc, broker, serializer=None, run_loops=False)
    assert hasattr(fp, "codec")
    assert isinstance(fp, ProducerProto)
```

- [ ] **Step 2: Run and confirm it fails**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run pytest tests/test_fake.py::test_fake_outbox_producer_satisfies_producer_proto -x --no-cov -v
```

Expected: `FAILED`.

- [ ] **Step 3: Add `codec` to `FakeOutboxProducer`**

Use Edit on `faststream_outbox/testing.py`:

`old_string`:
```python
class FakeOutboxProducer:
    """
    In-memory ``OutboxProducer`` substitute routing inserts through ``FakeOutboxClient``.

    Used by ``TestOutboxBroker`` so ``broker.publisher(queue).publish(body, session=...)``
    drives the same in-memory fake store as ``broker.publish(body, session=...)``. The
    *session* on the command is ignored — the fake client has no transaction.

    In sync mode (``run_loops=False``), each successful insert short-circuits into
    ``_sync_dispatch`` so handlers run before ``publish`` returns — matches the
    ``broker.publish`` patch in ``_build_fake_publish``.
    """

    _parser: typing.Any = None
    _decoder: typing.Any = None
```

`new_string`:
```python
class FakeOutboxProducer:
    """
    In-memory ``OutboxProducer`` substitute routing inserts through ``FakeOutboxClient``.

    Used by ``TestOutboxBroker`` so ``broker.publisher(queue).publish(body, session=...)``
    drives the same in-memory fake store as ``broker.publish(body, session=...)``. The
    *session* on the command is ignored — the fake client has no transaction.

    In sync mode (``run_loops=False``), each successful insert short-circuits into
    ``_sync_dispatch`` so handlers run before ``publish`` returns — matches the
    ``broker.publish`` patch in ``_build_fake_publish``.
    """

    _parser: typing.Any = None
    _decoder: typing.Any = None
    # ProducerProto[0.7] requires `codec`. The fake producer ignores it at
    # runtime, same as OutboxProducer.
    codec: typing.Any = None
```

Note: class-level `typing.Any = None` matches the existing `_parser` / `_decoder` shape on this class — no need to mirror the more careful annotation from Task 2. The point is structural satisfaction for `isinstance(fp, ProducerProto)`.

If Task 2 went down the non-Optional `CodecProto` path, set `codec: typing.Any = <DefaultCodecName>()` here too (instance assignment, not a class default — move into `__init__`).

- [ ] **Step 4: Run the test, expect pass**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run pytest tests/test_fake.py::test_fake_outbox_producer_satisfies_producer_proto -x --no-cov -v
```

Expected: `PASSED`.

- [ ] **Step 5: Interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add faststream_outbox/testing.py tests/test_fake.py && git commit -m "wip: add codec attribute to FakeOutboxProducer"
```

---

## Task 4: Convert `create_publisher_fake_subscriber` to instance method

**Files:**
- Modify: `faststream_outbox/testing.py` (lines 604–616)

- [ ] **Step 1: Confirm upstream signature**

The output from Task 1, Step 4 should have shown `create_publisher_fake_subscriber` as an instance method (first parameter `self`). If it didn't — STOP, re-investigate.

- [ ] **Step 2: Drop `@staticmethod`, add `self`**

Use Edit on `faststream_outbox/testing.py`:

`old_string`:
```python
    @staticmethod
    def create_publisher_fake_subscriber(  # pragma: no cover
        broker: OutboxBroker,
        publisher: typing.Any,
    ) -> tuple["OutboxSubscriber", bool]:
        # Required by FastStream's TestBroker abstract base, but never called —
        # we skip the publisher fake-subscriber loop in ``_fake_start`` because
        # ``FakeOutboxProducer`` already lands rows in the fake client AND drives
        # the real subscriber via ``_sync_dispatch``. The FastStream
        # publisher-spy infrastructure would mock the real handler and break that.
        del broker, publisher
        msg = "TestOutboxBroker handles publisher dispatch via FakeOutboxProducer; this is unreachable."
        raise NotImplementedError(msg)
```

`new_string`:
```python
    def create_publisher_fake_subscriber(  # pragma: no cover
        self,
        broker: OutboxBroker,
        publisher: typing.Any,
    ) -> tuple["OutboxSubscriber", bool]:
        # Required by FastStream's TestBroker abstract base, but never called —
        # we skip the publisher fake-subscriber loop in ``_fake_start`` because
        # ``FakeOutboxProducer`` already lands rows in the fake client AND drives
        # the real subscriber via ``_sync_dispatch``. The FastStream
        # publisher-spy infrastructure would mock the real handler and break that.
        del self, broker, publisher
        msg = "TestOutboxBroker handles publisher dispatch via FakeOutboxProducer; this is unreachable."
        raise NotImplementedError(msg)
```

- [ ] **Step 3: Verify ty no longer complains about override mismatch**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run ty check faststream_outbox/testing.py
```

Expected: no `invalid-method-override` error on `create_publisher_fake_subscriber`. (Other errors may remain — they're addressed in later tasks.)

- [ ] **Step 4: Interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add faststream_outbox/testing.py && git commit -m "wip: create_publisher_fake_subscriber as instance method"
```

---

## Task 5: Drop `middlewares_=` from `add_call` call site

**Files:**
- Modify: `faststream_outbox/registrator.py` (around lines 60 and 95–100)

- [ ] **Step 1: Run the broken existing tests to confirm the failure mode**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run pytest tests/test_unit.py -k subscriber -x --no-cov 2>&1 | tail -20
```

Expected: some test that exercises `broker.subscriber(...)` fails with `TypeError: add_call() got an unexpected keyword argument 'middlewares_'` — confirms 0.7 dropped the kwarg.

- [ ] **Step 2: Drop `middlewares=` from `OutboxRegistrator.subscriber` signature**

Use Edit on `faststream_outbox/registrator.py`:

`old_string`:
```python
    @override
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
        dependencies: Iterable["Dependant"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        middlewares: Sequence[SubscriberMiddleware[OutboxInnerMessage]] = (),
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
    ) -> "OutboxSubscriber":
```

`new_string`:
```python
    @override
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
        dependencies: Iterable["Dependant"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
    ) -> "OutboxSubscriber":
```

- [ ] **Step 3: Drop `middlewares_=middlewares` from the `add_call(...)` call**

`old_string`:
```python
        return subscriber.add_call(
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )
```

`new_string`:
```python
        return subscriber.add_call(
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            dependencies_=dependencies,
        )
```

- [ ] **Step 4: Clean up the `SubscriberMiddleware` import (now unused on this line)**

Check whether `SubscriberMiddleware` is still referenced elsewhere in the file:

```bash
grep -n "SubscriberMiddleware" /Users/kevinsmith/src/pypi/faststream-outbox/faststream_outbox/registrator.py
```

If the only remaining hit is the `from faststream._internal.types import ...` line at the top, remove `SubscriberMiddleware` from that import:

`old_string`:
```python
from faststream._internal.types import CustomCallable, SubscriberMiddleware
```

`new_string`:
```python
from faststream._internal.types import CustomCallable
```

If `SubscriberMiddleware` is still referenced elsewhere (it shouldn't be after this edit), leave the import alone.

- [ ] **Step 5: Drop `middlewares=` from `OutboxRegistrator.publisher` signature**

`old_string`:
```python
    @override
    def publisher(  # ty: ignore[invalid-method-override]
        self,
        queue: str,
        *,
        headers: dict[str, str] | None = None,
        middlewares: Sequence["PublisherMiddleware[OutboxPublishCommand]"] = (),
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> OutboxPublisher:
        """
        Construct a queue-scoped publisher.

        The publisher is standalone-only — call ``await pub.publish(body, session=session)``
        from inside your own transaction. Attempting to use it as a relay decorator on a
        subscriber raises ``NotImplementedError`` at decoration time, since the dispatch
        loop has no reachable ``AsyncSession`` without breaking the outbox transactional
        contract.

        *middlewares* run around every ``publisher.publish(...)`` call (and around
        ``broker.publish(...)`` when this publisher is used as the response publisher).
        """
        publisher = create_publisher(
            queue=queue,
            headers=headers,
            middlewares=middlewares,
            broker_config=self.config,  # ty: ignore[invalid-argument-type]
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )
        super().publisher(publisher)
        return publisher
```

`new_string`:
```python
    @override
    def publisher(  # ty: ignore[invalid-method-override]
        self,
        queue: str,
        *,
        headers: dict[str, str] | None = None,
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> OutboxPublisher:
        """
        Construct a queue-scoped publisher.

        The publisher is standalone-only — call ``await pub.publish(body, session=session)``
        from inside your own transaction. Attempting to use it as a relay decorator on a
        subscriber raises ``NotImplementedError`` at decoration time, since the dispatch
        loop has no reachable ``AsyncSession`` without breaking the outbox transactional
        contract.
        """
        publisher = create_publisher(
            queue=queue,
            headers=headers,
            broker_config=self.config,  # ty: ignore[invalid-argument-type]
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )
        super().publisher(publisher)
        return publisher
```

- [ ] **Step 6: Clean up the now-unused `PublisherMiddleware` import**

Check:

```bash
grep -n "PublisherMiddleware" /Users/kevinsmith/src/pypi/faststream-outbox/faststream_outbox/registrator.py
```

If the only remaining hit is the TYPE_CHECKING import, remove that line:

`old_string`:
```python
if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream._internal.types import PublisherMiddleware

    from faststream_outbox.response import OutboxPublishCommand
    from faststream_outbox.retry import RetryStrategyProto
    from faststream_outbox.subscriber.usecase import OutboxSubscriber
```

`new_string`:
```python
if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream_outbox.retry import RetryStrategyProto
    from faststream_outbox.subscriber.usecase import OutboxSubscriber
```

(Note: also drop the `OutboxPublishCommand` TYPE_CHECKING import — it was only used in the dropped `Sequence["PublisherMiddleware[OutboxPublishCommand]"]` annotation. Confirm with the same grep.)

- [ ] **Step 7: Also remove `Sequence` from the runtime import if no longer used**

```bash
grep -n "^from collections.abc\|Sequence" /Users/kevinsmith/src/pypi/faststream-outbox/faststream_outbox/registrator.py
```

If `Sequence` is now unused, drop it from `from collections.abc import Iterable, Sequence`.

- [ ] **Step 8: Re-run lint and the subscriber tests**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run ruff check faststream_outbox/registrator.py && uv run ty check faststream_outbox/registrator.py
```

Expected: clean. If ruff flags an unused import, drop it.

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run pytest tests/test_unit.py -k subscriber -x --no-cov 2>&1 | tail -10
```

Expected: the previous `TypeError` is gone. There may be other failures still (covered in later tasks).

- [ ] **Step 9: Interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add faststream_outbox/registrator.py && git commit -m "wip: drop middlewares= kwarg from OutboxRegistrator subscriber/publisher"
```

---

## Task 6: Drop `middlewares` from publisher factory + config

**Files:**
- Modify: `faststream_outbox/publisher/factory.py`
- Modify: `faststream_outbox/publisher/config.py`

These changes are paired — `create_publisher(middlewares=...)` was already deleted from its only caller in Task 5; now drop it from the factory's signature and the config's field.

- [ ] **Step 1: Drop `middlewares` param from `create_publisher`**

Use Edit on `faststream_outbox/publisher/factory.py`:

`old_string`:
```python
def create_publisher(
    *,
    queue: str,
    headers: dict[str, str] | None,
    middlewares: "Sequence[PublisherMiddleware[OutboxPublishCommand]]",
    broker_config: "OutboxBrokerConfig",
    title_: str | None,
    description_: str | None,
    schema_: typing.Any | None,
    include_in_schema: bool,
) -> OutboxPublisher:
    publisher_config = OutboxPublisherConfig(
        _outer_config=broker_config,
        queue=queue,
        headers=headers,
        middlewares=middlewares,
    )
```

`new_string`:
```python
def create_publisher(
    *,
    queue: str,
    headers: dict[str, str] | None,
    broker_config: "OutboxBrokerConfig",
    title_: str | None,
    description_: str | None,
    schema_: typing.Any | None,
    include_in_schema: bool,
) -> OutboxPublisher:
    publisher_config = OutboxPublisherConfig(
        _outer_config=broker_config,
        queue=queue,
        headers=headers,
    )
```

- [ ] **Step 2: Clean up the unused imports in factory.py**

```bash
grep -n "Sequence\|PublisherMiddleware\|OutboxPublishCommand" /Users/kevinsmith/src/pypi/faststream-outbox/faststream_outbox/publisher/factory.py
```

If the TYPE_CHECKING block's `Sequence`, `PublisherMiddleware`, and `OutboxPublishCommand` imports are no longer referenced, edit:

`old_string`:
```python
if typing.TYPE_CHECKING:
    from collections.abc import Sequence

    from faststream._internal.types import PublisherMiddleware

    from faststream_outbox.configs import OutboxBrokerConfig
    from faststream_outbox.response import OutboxPublishCommand
```

`new_string`:
```python
if typing.TYPE_CHECKING:
    from faststream_outbox.configs import OutboxBrokerConfig
```

- [ ] **Step 3: Drop `middlewares` from `OutboxPublisherConfig`**

Use Edit on `faststream_outbox/publisher/config.py`:

`old_string`:
```python
"""Config dataclasses for the outbox publisher (usecase + AsyncAPI spec)."""

import typing
from collections.abc import Sequence
from dataclasses import dataclass, field

from faststream._internal.configs import PublisherSpecificationConfig, PublisherUsecaseConfig


if typing.TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware

    from faststream_outbox.configs import OutboxBrokerConfig


@dataclass(kw_only=True)
class OutboxPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "OutboxBrokerConfig"
    queue: str
    headers: dict[str, str] | None = None
    middlewares: Sequence["PublisherMiddleware[typing.Any]"] = field(default_factory=tuple)
```

`new_string`:
```python
"""Config dataclasses for the outbox publisher (usecase + AsyncAPI spec)."""

import typing
from dataclasses import dataclass

from faststream._internal.configs import PublisherSpecificationConfig, PublisherUsecaseConfig


if typing.TYPE_CHECKING:
    from faststream_outbox.configs import OutboxBrokerConfig


@dataclass(kw_only=True)
class OutboxPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "OutboxBrokerConfig"
    queue: str
    headers: dict[str, str] | None = None
```

- [ ] **Step 4: Verify lint and ty are clean on the two files**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && \
  uv run ruff check faststream_outbox/publisher/factory.py faststream_outbox/publisher/config.py && \
  uv run ty check faststream_outbox/publisher/factory.py faststream_outbox/publisher/config.py
```

Expected: clean.

- [ ] **Step 5: Verify `PublisherUsecase` (upstream) doesn't read the removed `middlewares` field from the config**

The dataclass removal could break the base class if 0.7's `PublisherUsecaseConfig` expects subclasses to carry a `middlewares` field. Sanity-check:

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run python -c "
from faststream._internal.configs import PublisherUsecaseConfig
import dataclasses
print('PublisherUsecaseConfig fields:', [f.name for f in dataclasses.fields(PublisherUsecaseConfig)])
"
```

If `middlewares` is in the parent's field list, the parent's default propagates to our subclass — fine. If the parent class **requires** subclasses to override `middlewares`, then leave the field but make it a `Sequence[Any] = field(default_factory=tuple)` shim. Adjust based on what the inspection reveals.

- [ ] **Step 6: Interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add faststream_outbox/publisher/factory.py faststream_outbox/publisher/config.py && git commit -m "wip: drop middlewares from publisher factory + config"
```

---

## Task 7: Drop `middlewares=` from `OutboxRoute`

**Files:**
- Modify: `faststream_outbox/router.py` (lines 23–62)

`OutboxRouter.__init__` (lines 75–100) accepts a top-level `middlewares=` kwarg that maps to `broker_middlewares=` — that is broker-level and STAYS. Only `OutboxRoute` (which is a subscriber-route) loses the kwarg.

- [ ] **Step 1: Drop the param + pass-through**

Use Edit on `faststream_outbox/router.py`:

`old_string`:
```python
class OutboxRoute(SubscriberRoute):
    """Delayed-registration subscriber for use with ``OutboxRouter``."""

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
        dependencies: Iterable["Dependant"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        middlewares: Sequence[SubscriberMiddleware[OutboxInnerMessage]] = (),
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
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
```

`new_string`:
```python
class OutboxRoute(SubscriberRoute):
    """Delayed-registration subscriber for use with ``OutboxRouter``."""

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
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
```

- [ ] **Step 2: Clean up the now-unused `SubscriberMiddleware` import**

Check:
```bash
grep -n "SubscriberMiddleware" /Users/kevinsmith/src/pypi/faststream-outbox/faststream_outbox/router.py
```

If only the import remains, drop `SubscriberMiddleware` from the line:

`old_string`:
```python
from faststream._internal.types import BrokerMiddleware, CustomCallable, SubscriberMiddleware
```

`new_string`:
```python
from faststream._internal.types import BrokerMiddleware, CustomCallable
```

Then re-grep for `Sequence`; if only `Sequence[BrokerMiddleware[...]]` (in `OutboxRouter.__init__`) remains, leave it.

- [ ] **Step 3: Verify lint + ty**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run ruff check faststream_outbox/router.py && uv run ty check faststream_outbox/router.py
```

Expected: clean.

- [ ] **Step 4: Interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add faststream_outbox/router.py && git commit -m "wip: drop middlewares= from OutboxRoute"
```

---

## Task 8: Drop `middlewares=` from FastAPI router subscriber/publisher overrides

**Files:**
- Modify: `faststream_outbox/fastapi/router.py` (lines 153–236)

The top-level `OutboxRouter.__init__(middlewares=...)` at line 80 is broker-level and STAYS (it maps to `broker_middlewares=` via the parent `StreamRouter.__init__`). Only the per-call `subscriber()` and `publisher()` overrides drop the kwarg.

- [ ] **Step 1: Drop `middlewares=` from `OutboxRouter.subscriber` (signature + pass-through)**

Use Edit on `faststream_outbox/fastapi/router.py`:

`old_string`:
```python
    def subscriber(  # ty: ignore[invalid-method-override]  # noqa: PLR0913
        self,
        queues: str | list[str],
        *,
        # Outbox-subscriber knobs (mirror ``OutboxRegistrator.subscriber``)
        max_workers: int = 1,
        retry_strategy: "RetryStrategyProto | None" = None,
        fetch_batch_size: int = 10,
        min_fetch_interval: float = 1.0,
        max_fetch_interval: float = 10.0,
        lease_ttl_seconds: float = 60.0,
        max_deliveries: int | None = None,
        ack_policy: AckPolicy | None = None,
        # FastStream subscriber-level knobs
        dependencies: Iterable["params.Depends"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        middlewares: Sequence[SubscriberMiddleware[OutboxInnerMessage]] = (),
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
        # FastAPI response-model knobs (defaults match ``StreamRouter`` expectations)
        response_model: typing.Any = _DEFAULT_RESPONSE_MODEL,
        response_model_include: typing.Optional["IncEx"] = None,
        response_model_exclude: typing.Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> "OutboxSubscriber":
        # ``StreamRouter.subscriber`` uses ``*extra: NameRequired | str`` — our
        # ``queues: str | list[str]`` is wider; the actual broker-side
        # ``OutboxRegistrator.subscriber`` accepts both.
        return typing.cast(
            "OutboxSubscriber",
            super().subscriber(
                queues,  # ty: ignore[invalid-argument-type]
                max_workers=max_workers,
                retry_strategy=retry_strategy,
                fetch_batch_size=fetch_batch_size,
                min_fetch_interval=min_fetch_interval,
                max_fetch_interval=max_fetch_interval,
                lease_ttl_seconds=lease_ttl_seconds,
                max_deliveries=max_deliveries,
                ack_policy=ack_policy,
                dependencies=dependencies,
                parser=parser,
                decoder=decoder,
                middlewares=middlewares,
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
                response_model=response_model,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
            ),
        )
```

`new_string`:
```python
    def subscriber(  # ty: ignore[invalid-method-override]  # noqa: PLR0913
        self,
        queues: str | list[str],
        *,
        # Outbox-subscriber knobs (mirror ``OutboxRegistrator.subscriber``)
        max_workers: int = 1,
        retry_strategy: "RetryStrategyProto | None" = None,
        fetch_batch_size: int = 10,
        min_fetch_interval: float = 1.0,
        max_fetch_interval: float = 10.0,
        lease_ttl_seconds: float = 60.0,
        max_deliveries: int | None = None,
        ack_policy: AckPolicy | None = None,
        # FastStream subscriber-level knobs
        dependencies: Iterable["params.Depends"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
        # FastAPI response-model knobs (defaults match ``StreamRouter`` expectations)
        response_model: typing.Any = _DEFAULT_RESPONSE_MODEL,
        response_model_include: typing.Optional["IncEx"] = None,
        response_model_exclude: typing.Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> "OutboxSubscriber":
        # ``StreamRouter.subscriber`` uses ``*extra: NameRequired | str`` — our
        # ``queues: str | list[str]`` is wider; the actual broker-side
        # ``OutboxRegistrator.subscriber`` accepts both.
        return typing.cast(
            "OutboxSubscriber",
            super().subscriber(
                queues,  # ty: ignore[invalid-argument-type]
                max_workers=max_workers,
                retry_strategy=retry_strategy,
                fetch_batch_size=fetch_batch_size,
                min_fetch_interval=min_fetch_interval,
                max_fetch_interval=max_fetch_interval,
                lease_ttl_seconds=lease_ttl_seconds,
                max_deliveries=max_deliveries,
                ack_policy=ack_policy,
                dependencies=dependencies,
                parser=parser,
                decoder=decoder,
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
                response_model=response_model,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
            ),
        )
```

- [ ] **Step 2: Drop `middlewares=` from `OutboxRouter.publisher`**

`old_string`:
```python
    def publisher(  # ty: ignore[invalid-method-override]
        self,
        queue: str,
        *,
        headers: dict[str, str] | None = None,
        middlewares: Sequence["PublisherMiddleware[OutboxPublishCommand]"] = (),
        title: str | None = None,
        description: str | None = None,
        schema: typing.Any | None = None,
        include_in_schema: bool = True,
    ) -> "OutboxPublisher":
        # ``StreamRouter.publisher`` forwards directly to ``self.broker.publisher``;
        # mirror its delegation so outbox users get the right return type.
        return self.broker.publisher(
            queue,
            headers=headers,
            middlewares=middlewares,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )
```

`new_string`:
```python
    def publisher(  # ty: ignore[invalid-method-override]
        self,
        queue: str,
        *,
        headers: dict[str, str] | None = None,
        title: str | None = None,
        description: str | None = None,
        schema: typing.Any | None = None,
        include_in_schema: bool = True,
    ) -> "OutboxPublisher":
        # ``StreamRouter.publisher`` forwards directly to ``self.broker.publisher``;
        # mirror its delegation so outbox users get the right return type.
        return self.broker.publisher(
            queue,
            headers=headers,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )
```

- [ ] **Step 3: Clean up now-unused imports**

```bash
grep -n "SubscriberMiddleware\|PublisherMiddleware\|OutboxPublishCommand" /Users/kevinsmith/src/pypi/faststream-outbox/faststream_outbox/fastapi/router.py
```

Remove from the runtime import line:
- `SubscriberMiddleware` from `from faststream._internal.types import BrokerMiddleware, CustomCallable, SubscriberMiddleware` if no longer referenced.

Remove from the TYPE_CHECKING block:
- `from faststream._internal.types import PublisherMiddleware` (if unreferenced).
- `from faststream_outbox.response import OutboxPublishCommand` (if unreferenced).

The `BrokerMiddleware` import STAYS — used by `OutboxRouter.__init__(middlewares=...)` at line 80.

- [ ] **Step 4: Re-check whether `# noqa: PLR0913` should remain**

After the drop, count the args on `subscriber()`. If it's still > 15, the `noqa` stays. If ≤ 15, drop the `# noqa: PLR0913` from the method line.

```bash
uv run python -c "
import inspect
from faststream_outbox.fastapi.router import OutboxRouter
print('subscriber args:', len(inspect.signature(OutboxRouter.subscriber).parameters))
print('publisher args:', len(inspect.signature(OutboxRouter.publisher).parameters))
"
```

Adjust the `noqa` accordingly.

- [ ] **Step 5: Verify lint + ty**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run ruff check faststream_outbox/fastapi/router.py && uv run ty check faststream_outbox/fastapi/router.py
```

Expected: clean.

- [ ] **Step 6: Interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add faststream_outbox/fastapi/router.py && git commit -m "wip: drop middlewares= from fastapi router subscriber/publisher"
```

---

## Task 9: Conditionally handle `AckPolicy.ACK_FIRST`

**Files:**
- Modify (conditional): `faststream_outbox/subscriber/factory.py` (lines 108–116)
- Modify (conditional): the matching test in `tests/test_unit.py`

This task branches on the `ACK_FIRST_GONE` value from Task 1, Step 4.

**Branch A — `ACK_FIRST_GONE == False` (the enum member survived):** No change needed. The footgun rejection still applies; the existing test still passes. Skip to Task 10.

**Branch B — `ACK_FIRST_GONE == True` (the enum member is gone):**

- [ ] **Step 1: Locate and delete the rejection branch**

Use Edit on `faststream_outbox/subscriber/factory.py`:

`old_string`:
```python
    is_no_retry = isinstance(retry_strategy, NoRetry)
    if ack_policy is AckPolicy.ACK_FIRST:
        msg = (
            "ack_policy=AckPolicy.ACK_FIRST is not supported by the outbox broker: it "
            "deletes the row before the handler runs, so a handler crash silently drops "
            "the message — defeating the outbox reliability guarantee. Use NACK_ON_ERROR "
            "(default, retries via retry_strategy), REJECT_ON_ERROR (delete on first "
            "failure, no retry), or MANUAL (handler calls msg.ack()/nack()/reject() itself)."
        )
        raise ValueError(msg)
    if ack_policy is AckPolicy.REJECT_ON_ERROR and retry_strategy is not None and not is_no_retry:
```

`new_string`:
```python
    is_no_retry = isinstance(retry_strategy, NoRetry)
    if ack_policy is AckPolicy.REJECT_ON_ERROR and retry_strategy is not None and not is_no_retry:
```

- [ ] **Step 2: Locate and delete the matching test**

```bash
grep -n "ACK_FIRST" /Users/kevinsmith/src/pypi/faststream-outbox/tests/*.py
```

Delete the test that asserts the ACK_FIRST rejection. (Exact name/file depends on the test layout — likely `tests/test_unit.py::test_subscriber_rejects_ack_first` or similar.)

- [ ] **Step 3: Verify lint + ty**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run ruff check faststream_outbox/subscriber/factory.py && uv run ty check faststream_outbox/subscriber/factory.py
```

Expected: clean.

- [ ] **Step 4: Update `CLAUDE.md`** — the architecture section documents this rejection. Remove the "AckPolicy.ACK_FIRST is rejected at registration with ValueError" passage:

```bash
grep -n "ACK_FIRST" /Users/kevinsmith/src/pypi/faststream-outbox/CLAUDE.md
```

If hits exist, trim them (whole sentence; leave surrounding paragraph coherent).

- [ ] **Step 5: Interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add faststream_outbox/subscriber/factory.py tests/ CLAUDE.md && git commit -m "wip: drop ACK_FIRST rejection (enum member gone in 0.7)"
```

---

## Task 10: Test cleanup + docs + CLAUDE.md

**Files:**
- Modify: `tests/test_fake.py` (delete `test_publisher_accepts_middlewares_kwarg` at lines 406–423)
- Modify (conditional): `tests/test_unit.py` (if `_basic_publish` / `_publish` `_extra_middlewares=` kwarg changed in 0.7)
- Modify: `docs/usage/publisher.md` (lines 102–105)
- Modify: `CLAUDE.md` (line 37)

- [ ] **Step 1: Delete `test_publisher_accepts_middlewares_kwarg`**

Use Edit on `tests/test_fake.py`:

`old_string`:
```python
async def test_publisher_accepts_middlewares_kwarg() -> None:
    """B3: ``broker.publisher(..., middlewares=...)`` threads the middleware through to publish."""
    broker = _make_broker()
    seen: list[str] = []

    async def record_middleware(call_next: typing.Callable, cmd: typing.Any) -> typing.Any:
        seen.append(f"before:{cmd.destination}")
        result = await call_next(cmd)
        seen.append(f"after:{cmd.destination}")
        return result

    publisher = broker.publisher("orders", middlewares=(record_middleware,))

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await publisher.publish({"x": 1}, session=_fake_session())

    assert seen == ["before:orders", "after:orders"]


async def test_outbox_route_accepts_ack_policy() -> None:
```

`new_string`:
```python
async def test_outbox_route_accepts_ack_policy() -> None:
```

- [ ] **Step 2: Audit `_extra_middlewares=` usage**

```bash
grep -n "_extra_middlewares" /Users/kevinsmith/src/pypi/faststream-outbox/faststream_outbox/ /Users/kevinsmith/src/pypi/faststream-outbox/tests/ -r
```

Expected hits:
- `faststream_outbox/publisher/usecase.py` line ~99 (`await self._basic_publish(cmd, producer=..., _extra_middlewares=())`)
- `faststream_outbox/publisher/usecase.py` line ~111 (the `_publish` override signature)
- `tests/test_unit.py` line ~925 (the test that calls `_publish(cmd, _extra_middlewares=())`)

Verify that upstream's `_basic_publish` / `_publish` still accept `_extra_middlewares=`:

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run python -c "
import inspect
from faststream._internal.broker.pub_base import BrokerPublishingMixin
from faststream._internal.endpoint.publisher import PublisherUsecase
print('_basic_publish sig:', inspect.signature(BrokerPublishingMixin._basic_publish))
print('_publish sig:', inspect.signature(PublisherUsecase._publish))
"
```

- **If `_extra_middlewares` is still accepted:** no change needed.
- **If `_extra_middlewares` was removed in 0.7:** drop those three usages (signature, call site, test line). Replace `_extra_middlewares=()` with whatever 0.7 uses (likely just remove it entirely if middlewares are now exclusively broker-scope).
- **If `_extra_middlewares` was renamed:** rename at the three sites.

Make whatever edits are needed; if no change is needed, log "no change" and move on.

- [ ] **Step 3: Trim `docs/usage/publisher.md`**

`old_string`:
```markdown
Per-call `headers` are merged with the decorator's static headers
(per-call wins). Per-publisher `middlewares=` wrap every
`publisher.publish(...)` call — useful for tracing spans, metrics
counters, or audit-log writes scoped to a single queue.
```

`new_string`:
```markdown
Per-call `headers` are merged with the decorator's static headers
(per-call wins).
```

- [ ] **Step 4: Trim `CLAUDE.md`**

Find the paragraph at line ~37 that documents `broker.publisher(..., middlewares=...)`:

`old_string`:
```
`broker.publisher(queue, *, headers=None, middlewares=(), title=None, description=None, schema=None, include_in_schema=True)` returns an `OutboxPublisher` — a typed, queue-scoped wrapper around `broker.publish` with the same transactional contract: `await pub.publish(body, *, session, headers=None, correlation_id=None, activate_in=None, activate_at=None, timer_id=None)`. Static headers passed to the decorator are merged with per-call headers (per-call wins). `middlewares=` wrap every `publisher.publish(...)` call. The publisher exists for AsyncAPI spec coverage and per-queue config — **not** for decorator-relay chaining. `OutboxPublisher.__call__` raises `NotImplementedError` at decoration time so `@pub @broker.subscriber(...)` fails fast with a message pointing at the manual `broker.publish(...)` pattern. Rationale: the dispatch loop has no reachable `AsyncSession` without breaking the outbox transactional contract (row commits with caller's domain writes), so a relay decorator would either silently open its own session (defeating the point) or require contextvar plumbing (over-engineered for the use case).
```

`new_string`:
```
`broker.publisher(queue, *, headers=None, title=None, description=None, schema=None, include_in_schema=True)` returns an `OutboxPublisher` — a typed, queue-scoped wrapper around `broker.publish` with the same transactional contract: `await pub.publish(body, *, session, headers=None, correlation_id=None, activate_in=None, activate_at=None, timer_id=None)`. Static headers passed to the decorator are merged with per-call headers (per-call wins). The publisher exists for AsyncAPI spec coverage and per-queue config — **not** for decorator-relay chaining. `OutboxPublisher.__call__` raises `NotImplementedError` at decoration time so `@pub @broker.subscriber(...)` fails fast with a message pointing at the manual `broker.publish(...)` pattern. Rationale: the dispatch loop has no reachable `AsyncSession` without breaking the outbox transactional contract (row commits with caller's domain writes), so a relay decorator would either silently open its own session (defeating the point) or require contextvar plumbing (over-engineered for the use case).
```

- [ ] **Step 5: Final sweep — confirm no per-call middlewares= references remain**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && \
  grep -rn "middlewares=" faststream_outbox/ tests/ docs/ README.md CLAUDE.md 2>/dev/null | \
  grep -v "broker_middlewares" | \
  grep -v "BrokerMiddleware" | \
  grep -v "# noqa\|# ty:"
```

Expected hits to remain (each is broker-scope and stays):
- `tests/test_middleware_opentelemetry.py:45` (`OutboxBroker(middlewares=[...])`)
- `tests/test_middleware_prometheus.py:37,206` (`OutboxBroker(middlewares=[...])`)
- `docs/usage/observability.md:266` (`OutboxBroker(middlewares=[...])`)
- `faststream_outbox/fastapi/router.py:127` (`super().__init__(...middlewares=middlewares...)`)
- `faststream_outbox/router.py:92` (`broker_middlewares=middlewares`)

If anything else turns up, audit it — it's likely a missed cleanup.

- [ ] **Step 6: Interim commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git add tests/test_fake.py docs/usage/publisher.md CLAUDE.md && git commit -m "wip: drop test + docs for removed per-call middlewares="
```

If Step 2 made code edits, include those files in the `git add`.

---

## Task 11: Full test pass + squash + push

**Files:** none new — verification + final commit shape.

- [ ] **Step 1: Full lint**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && just lint-ci
```

Expected: all checks pass. If anything fails, fix in-place and re-run before proceeding.

- [ ] **Step 2: No-Postgres test tier**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && uv run pytest tests/test_unit.py tests/test_fake.py --no-cov -v 2>&1 | tail -40
```

Expected: all tests pass. Failures here are easier to debug locally than the full docker run.

- [ ] **Step 3: Full suite with coverage gate**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && just test
```

Expected: full suite green at `--cov-fail-under=100`. If coverage drops below 100, look for orphaned branches (most likely candidates: the removed `middlewares=` branches in registrator/factory/config). The fix is usually one of:
  1. Branch is fully dead → delete it.
  2. Branch survives but the only test exercising it was deleted → re-add a focused test or restructure.

Iterate until green.

- [ ] **Step 4: Final repo-wide sanity checks**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && \
  echo '--- pin ---' && \
  grep -n 'faststream' pyproject.toml | grep -v 'faststream-outbox' && \
  echo '--- stale pin refs ---' && \
  (grep -rn 'faststream<0.7\|faststream>=0.6' . --include='*.py' --include='*.toml' --include='*.md' || echo 'none') && \
  echo '--- import sanity ---' && \
  uv run python -c "from faststream_outbox import OutboxBroker; from faststream_outbox.fastapi import OutboxRouter; print('OK')"
```

Expected:
- pin shows `"faststream>=0.7,<0.8"`
- stale pin refs prints `none`
- import sanity prints `OK`

- [ ] **Step 5: Inspect the WIP commit list**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git log --oneline main..HEAD
```

Expected: a handful of `wip: ...` commits from Tasks 1–10.

- [ ] **Step 6: Squash to a single bundled commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git reset --soft $(git merge-base main HEAD) && git status
```

Expected: all the touched files staged, nothing committed yet on the branch.

- [ ] **Step 7: Create the single bundled commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git commit -m "$(cat <<'EOF'
chore: migrate to faststream 0.7

Pure compat migration; no new 0.7 features adopted.

Internal break points fixed:
- pyproject.toml: pin bumped to faststream>=0.7,<0.8.
- publisher/producer.py: OutboxProducer gained a `codec` attribute to
  satisfy ProducerProto[0.7]. The outbox owns its own encoding pipeline
  via _encode_payload and ignores the attribute at runtime.
- testing.py: FakeOutboxProducer gained the same `codec` attribute;
  create_publisher_fake_subscriber converted from @staticmethod to
  instance method to match upstream's abstract base.
- registrator.py: dropped `middlewares_=` from the SubscriberUsecase
  .add_call call (kwarg removed upstream in 0.7).

Public surface (BREAKING):
- Dropped the per-call `middlewares=` kwarg from
  OutboxRegistrator.subscriber, OutboxRegistrator.publisher, OutboxRoute,
  and the FastAPI router's subscriber/publisher overrides. Upstream
  removed publisher- and subscriber-level middlewares as a public
  surface in 0.7; install middleware at the broker scope instead via
  `OutboxBroker(middlewares=[...])`. Broker-scope middlewares on
  OutboxBroker, OutboxRouter, and the FastAPI router are unchanged.

Spec: planning/specs/2026-06-03-faststream-0.7-migration-design.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 8: Confirm the commit**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git log -1 --stat
```

Expected: one new commit on the branch, with all the modified files listed.

- [ ] **Step 9: Final test rerun on the squashed branch**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && just lint-ci && just test
```

Expected: green on both. (Sanity check that the squash didn't drop anything — `git reset --soft` preserves the working tree, so this should be identical to Step 3, but worth re-running on a paranoid basis.)

- [ ] **Step 10: Push the branch + open the PR**

```bash
cd /Users/kevinsmith/src/pypi/faststream-outbox && git push -u origin chore/faststream-0.7-migration
```

Then open the PR using `gh pr create` — refer to the standard PR-creation flow in `CLAUDE.md` (the operator will run this; do not auto-open without explicit user instruction).

---

## Acceptance criteria (spec §"Verification")

After Task 11, all of these must be true:

- [ ] `uv lock --upgrade` resolved `faststream` to a `0.7.x` release (Task 1, Step 3 output).
- [ ] `uv sync --all-extras --all-groups --frozen` succeeds (Task 11, equivalent via `just install` — run manually if needed).
- [ ] `just lint-ci` clean (Task 11, Step 1).
- [ ] `just test` green at `--cov-fail-under=100` (Task 11, Step 3).
- [ ] `uv run pytest tests/test_unit.py tests/test_fake.py` green standalone (Task 11, Step 2).
- [ ] `git grep -n "middlewares_=\|middlewares=" faststream_outbox/ tests/ docs/` returns only `broker_middlewares=` / `BrokerMiddleware` references (Task 10, Step 5).
- [ ] `git grep -n "faststream<0.7\|faststream>=0.6" .` returns nothing (Task 11, Step 4).
- [ ] Manual import sanity passes (Task 11, Step 4).
- [ ] The branch has exactly one commit beyond `main` (Task 11, Step 8) — `git log --oneline main..HEAD | wc -l` returns `1`.
- [ ] The commit message documents each break point + the dropped kwarg (Task 11, Step 7).
