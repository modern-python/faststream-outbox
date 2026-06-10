# Planning

Specs and plans for `faststream-outbox` changes. See
[CLAUDE.md](../CLAUDE.md#workflow) for the per-feature workflow.

Each change is a paired `*-design.md` + `*-plan.md`. Both halves live
together in `active/` while in flight; both move to `archived/` when the
implementing PR merges. Frontmatter records `status`, `pr`, and
`outcome`. See [`_templates/`](_templates/) for copy-and-fill starting
points.

## Active

- **[docs-landing-and-comparison](active/2026-06-10-docs-landing-and-comparison-design.md)**
  — Rewrite docs landing, reshape nav into Concepts/Guides/Reference,
  add a Comparison page.
- **[planning-conventions](active/2026-06-10-planning-conventions-design.md)**
  — This restructure. Codify the spec/plan boundary, add
  `active/`/`archived/`/`_templates/`, migrate the existing pairs.

## Archived (shipped)

- **[drain-test-flaky-fetch-observation](archived/2026-06-09-drain-test-flaky-fetch-observation-design.md)**
  (#48, 2026-06-10) — Drain test waits via the `fetched` recorder
  instead of an SQL poll, killing a 3.14 coverage flake.
- **[mkdocs-github-pages](archived/2026-06-09-mkdocs-github-pages-design.md)**
  (#45, 2026-06-09) — Docs hosting moves from Read the Docs to GitHub
  Pages on `faststream-outbox.modern-python.org`.
- **[foreign-broker-relay](archived/2026-06-04-foreign-broker-relay-design.md)**
  (#44, 2026-06-05) — `OutboxSubscriber` officially supports the
  FastStream-native decorator relay to Kafka/Rabbit/NATS/Redis with
  three guardrails.
- **[faststream-0.7.1-testbroker-typing](archived/2026-06-04-faststream-0.7.1-testbroker-typing-design.md)**
  (#43, 2026-06-04) — Adopt FastStream 0.7.1's `TestBroker[Broker,
  EnterType]` typing fix; drop two `# ty: ignore` directives.
- **[faststream-0.7-migration](archived/2026-06-03-faststream-0.7-migration-design.md)**
  (#42, 2026-06-03) — Migrate to `faststream>=0.7,<0.8`; fix mechanical
  break points; drop per-call `middlewares=` kwarg.
- **[all-extra-and-planning-dir](archived/2026-06-03-all-extra-and-planning-dir-design.md)**
  (#41, 2026-06-03) — Add `faststream-outbox[all]` aggregate extra;
  bootstrap the `planning/` directory itself.

## Other

- **[architecture/](architecture/)** — deep-dive reference for shipped
  invariants (relay, timers, DLQ, drain, metrics, test broker).
- **[lint-suppressions.md](lint-suppressions.md)** — audit of `noqa` /
  `ty: ignore` directives and why each one stays.
