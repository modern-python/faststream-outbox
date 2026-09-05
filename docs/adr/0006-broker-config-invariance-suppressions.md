# The `BrokerUsecase` invariance suppressions stay

**Decision:** the `# ty: ignore` comments arising from `BrokerUsecase`'s invariance on its
config-type parameter stay where they are. They are not removed, not narrowed to a runtime cast, and
not worked around by widening `OutboxBrokerConfig`.

They look like a cluster of unrelated escapes and are one root cause. `OutboxBroker` extends
`BrokerUsecase[..., ..., OutboxBrokerConfig]`, and `BrokerUsecase` is invariant on that parameter,
so anything that unifies an outbox type against the plain `BrokerConfig` form is rejected:
`TestOutboxBroker(TestBroker[OutboxBroker, OutboxBroker])` against the `Broker` TypeVar's bound,
`patch_broker_calls(broker)` in `testing.py`, and the `type[OutboxBroker]` key in
`get_broker_registry`'s dict against its annotated `type[BrokerUsecase[Any, Any]]`. All three are
runtime-safe: the call sites only iterate `broker.subscribers` or build a mapping. The related
`invalid-method-override` suppressions on `publish` / `publish_batch` are a *different* deliberate
divergence — the outbox contract adds `session`, `activate_in`, `activate_at`, `timer_id` to
upstream's signature, which is the whole point of the broker — but they get re-argued in the same
breath, so they are recorded here too.

- **Widening the config type** to satisfy the variance would give up the typed access to
  `client`, `engine`, `dlq_table`, and the recorder that `OutboxBrokerConfig` exists to provide, and
  push those back to `Any` at every read.
- **Casting at each site** replaces a suppression that documents a known upstream variance rule with
  an assertion that is not true of the object being passed. The suppression is the honest form; see
  [ADR-0003](0003-conn-union-is-deliberate.md) for the same reasoning applied to a type union.
- **Deleting them and living with the diagnostics** is not available: `just lint-ci` gates on `ty`.

**Revisit trigger:** upstream FastStream makes `BrokerUsecase` covariant in its config-type
parameter, or restructures the `TestBroker` / `patch_broker_calls` surface so the outbox config no
longer has to unify with `BrokerConfig`. Re-run `just lint` with the suppressions removed to confirm
before dropping them.
