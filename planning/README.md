# Planning

Specs, plans, and change history for `faststream-outbox`. The living truth
about *what the system does now* lives in [`architecture/`](../architecture/)
at the repo root; this directory records *how it got there*.

## Conventions

> This section is the portable convention — identical across the
> modern-python repos. The Index below is repo-specific. To adopt elsewhere,
> copy this section plus [`_templates/`](_templates/) and point that repo's
> `CLAUDE.md` Workflow + truth home at it.

### Two axes, never mixed

- **`architecture/` (repo root) — the present.** One file per capability,
  living prose, updated whenever a change ships. The truth home.
- **`planning/changes/` — the past-and-pending.** One folder per change,
  frozen once shipped.

Shipping a change **promotes** its conclusions into the affected
`architecture/<capability>.md` by hand, then archives the bundle. That
hand-edit is what keeps `architecture/` true; the archived bundle carries the
*why*.

### Change bundles

A change is a folder `changes/active/YYYY-MM-DD.NN-<slug>/`:

- `YYYY-MM-DD` — proposal date; `.NN` — zero-padded intra-day counter
  (`.01`, `.02`, …) that breaks same-date ties so the timeline sorts stably.
- `<slug>` — kebab-case description, not a story ID.

On merge the folder moves to `changes/archive/` with `status: shipped`, `pr:`,
and `outcome:` filled, and its line moves from **Active** to **Archived** in
the Index below.

### Three lanes

| Lane | Artifacts | Use when |
|------|-----------|----------|
| **Full** | `design.md` + `plan.md` | design judgment; new file/module; public-API change; cross-cutting/multi-file; non-trivial test design |
| **Lightweight** | `change.md` | small-but-real: ≲30 LOC net, ≤2 files, no new file, no public-API change, single straightforward test |
| **Tiny** | none — conventional commit | typo, dep bump, linter/formatter/CI tweak, mechanical rename, single-line config |

Heavier lane wins on ambiguity. A `change.md` that outgrows its lane splits
into `design.md` + `plan.md`.

### Artifacts at a glance

- **`design.md`** — the spec: the *thinking* (why, design, trade-offs, scope).
- **`plan.md`** — the plan: the *sequencing* (the executor's task checklist).
- **`change.md`** — both, condensed, for the lightweight lane.
- **`releases/<semver>.md`** — per-release user-facing notes.
- **`audits/<date>-<slug>.md`** — findings from a code/docs/bug-hunt sweep;
  spawns fix changes.
- **`retros/<date>-<slug>.md`** — what we learned after a body of work.
- **`deferred.md`** — real-but-unscheduled items, each with a revisit trigger.

Templates live in [`_templates/`](_templates/).

### Frontmatter

`design.md` / `change.md`: `status` (draft|approved|shipped|superseded),
`date`, `slug`, `supersedes`, `superseded_by`, `pr`, `outcome`.
`plan.md`: `status`, `date`, `slug`, `spec`, `pr`. Files in `architecture/`
carry **no** frontmatter — living prose, dated by git.

## Index

### Active

_None._

### Archived (shipped)

- **[docs-diataxis-nav](changes/archive/2026-06-19.02-docs-diataxis-nav/design.md)**
  (#104, 2026-06-19) — Dissolve the standalone Patterns nav section: fold the
  messaging-service case study into Guides and move the file to `docs/usage/`
  (no section renames). Seven top-level sections → six.
- **[messaging-service-patterns-doc](changes/archive/2026-06-19.01-messaging-service-patterns-doc/design.md)**
  (#103, 2026-06-19) — New `docs/patterns/` section with one page composing
  the outbox in an anonymized chat/notifications service: transactional event
  relay, fire-unless-cancelled timer, and nested test brokers.
- **[actionable-schema-drift-error](changes/archive/2026-06-16.01-actionable-schema-drift-error/design.md)**
  (#99, 2026-06-16) — `validate_schema()` appends a hand-written-migration
  pointer to its `RuntimeError` for Alembic-blind drift (the `outbox_lease_ck`
  CHECK and partial-index predicates that `--autogenerate` can't remediate);
  recipe lives in `docs/operations/alembic.md`.
- **[portable-planning-convention](changes/archive/2026-06-13.01-portable-planning-convention/design.md)**
  (#77, 2026-06-13) — Two-axis OpenSpec-shaped convention: `architecture/`
  truth + `changes/` folder bundles, `.NN` intra-day tiebreak, three lanes,
  dedicated `audits/`+`retros/`, portable README. Supersedes
  planning-conventions.
- **[docs-tutorials](changes/archive/2026-06-12.01-docs-tutorials/design.md)**
  (#58, 2026-06-12) — The two tutorials deferred from #56: *Your first outbox
  app* and *Add a Kafka relay*. Kill-Kafka step folded into an at-least-once
  callout after `aiokafka` absorbed the outage on both attempts.
- **[docs-tutorials-and-observability-split](changes/archive/2026-06-11.02-docs-tutorials-and-observability-split/design.md)**
  (#56, 2026-06-12) — Three-way split of `usage/observability.md` into
  Reference + How-to + Explanation; tutorials deferred to #58.
- **[operator-pages](changes/archive/2026-06-11.01-operator-pages/design.md)**
  (#53, 2026-06-11) — `docs/operations/`: Production checklist, Troubleshooting
  playbook, Alembic migrations. The B follow-on from #50.
- **[docs-landing-and-comparison](changes/archive/2026-06-10.02-docs-landing-and-comparison/design.md)**
  (#50, 2026-06-10) — Docs landing rewrite, four-section nav reshape, new
  Comparison page.
- **[planning-conventions](changes/archive/2026-06-10.01-planning-conventions/design.md)**
  (#49, 2026-06-10) — Spec/plan boundary, `active/`/`archived/`/`_templates/`
  layout, frontmatter, migration of the existing pairs. *Superseded by
  [portable-planning-convention](changes/archive/2026-06-13.01-portable-planning-convention/design.md).*
- **[drain-test-flaky-fetch-observation](changes/archive/2026-06-09.02-drain-test-flaky-fetch-observation/design.md)**
  (#48, 2026-06-10) — Drain test waits via the `fetched` recorder instead of an
  SQL poll, killing a 3.14 coverage flake.
- **[mkdocs-github-pages](changes/archive/2026-06-09.01-mkdocs-github-pages/design.md)**
  (#45, 2026-06-09) — Docs hosting moves from Read the Docs to GitHub Pages on
  `faststream-outbox.modern-python.org`.
- **[foreign-broker-relay](changes/archive/2026-06-04.02-foreign-broker-relay/design.md)**
  (#44, 2026-06-05) — `OutboxSubscriber` officially supports the
  FastStream-native decorator relay to Kafka/Rabbit/NATS/Redis with three
  guardrails.
- **[faststream-0.7.1-testbroker-typing](changes/archive/2026-06-04.01-faststream-0.7.1-testbroker-typing/design.md)**
  (#43, 2026-06-04) — Adopt FastStream 0.7.1's `TestBroker[Broker, EnterType]`
  typing fix; drop two `# ty: ignore` directives.
- **[faststream-0.7-migration](changes/archive/2026-06-03.02-faststream-0.7-migration/design.md)**
  (#42, 2026-06-03) — Migrate to `faststream>=0.7,<0.8`; fix mechanical break
  points; drop per-call `middlewares=` kwarg.
- **[all-extra-and-planning-dir](changes/archive/2026-06-03.01-all-extra-and-planning-dir/design.md)**
  (#41, 2026-06-03) — Add `faststream-outbox[all]` aggregate extra; bootstrap
  the `planning/` directory itself.

## Other

- **[`architecture/`](../architecture/)** at the repo root — the living
  capability truth (relay, timers, dlq, drain, metrics, test broker). This is
  the promotion target on every ship.
- **[audits/](audits/)** — findings reports (2026-06-12 code + docs audits).
- **[lint-suppressions.md](lint-suppressions.md)** — repo-specific extra (not
  part of the portable core): audit of `noqa` / `ty: ignore` directives and
  why each one stays.
- **[deferred.md](deferred.md)** — the long-tail register of real-but-
  unscheduled items with revisit triggers.
