# AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`faststream-outbox` is a FastStream broker whose transport is a Postgres table (transactional
outbox pattern), Postgres-only at v0. [`CONTEXT.md`](CONTEXT.md) opens with what it does and owns
the vocabulary — read it before naming a concept in code, a test name, or an issue title.

## Commands

`just` (task runner) and `uv` (package manager). The [`justfile`](justfile) is the source of truth —
`just --list`, or read it; every recipe carries its intent as a comment. The things it does not say:

- `just test [args]` forwards args **unquoted**, so a spaced `-k` expression word-splits and fails.
  Run one keyword per invocation, or a substring matching all targets.
- `tests/test_unit.py` + `tests/test_fake.py` need no Postgres and run directly under `uv run
  pytest`; `tests/test_integration.py` skips if `POSTGRES_DSN` is unreachable. The coverage gate is
  on by default, so a partial run trips it — pass `--no-cov` while iterating.
- Nothing validates Markdown links outside `docs/`. `just docs-build` runs `mkdocs --strict` over
  the site only; root Markdown, `.github/`, and `docs/agents/` are unchecked.

## Architecture

Behavior detail has no prose home — it lives in the code and its `INVARIANT:`-marked tests. Before
writing prose about a capability, run the admission check in **Where a fact goes** below.

Every module under `faststream_outbox/` is named for what it does; read it. What a single-file read
will **not** tell you:

- `subscriber/usecase.py` is load-bearing: **every terminal write filters on `acquired_token`**, so
  a stale writer finds `rowcount == 0` and is dropped. Any new fetch or terminal path must preserve
  that — `tests/test_client_contract.py::test_delete_noop_on_token_mismatch` is the claim.
- `client.py` and `testing.py` implement the same rules twice, in SQL and in Python, because one
  runs in the database and one in the process. They cannot share an implementation, so
  `tests/test_client_contract.py` couples them by behaviour — a change to either adapter's fetch,
  terminal, or DLQ semantics belongs in that suite.
- `schema.py` — the three partial indexes and the `<table>_lease_ck` CHECK are load-bearing, not
  decoration.
- `message.py` — the `DLQFailureReason` `Literal` is a **public contract**; operator queries key off
  those strings, so changing one is API-breaking.
- `metrics/` (the recorder seam) and `prometheus/` + `opentelemetry/` (native middleware) are two
  seams, deliberately. Each fires for events the other physically cannot observe — `fetched` has no
  `StreamMessage`, `lease_lost` fires after `consume_scope` exits. **Don't collapse them.**

`TestOutboxBroker` swaps in `FakeOutboxClient`. Sync mode is the default; `run_loops=True` runs the
real fetch and worker loops against the fake, which retry, lease-expiry, and scheduling tests need.

## Workflow

Two things outlive the PR, and there are exactly two places to put them: an alternative **rejected**
with reasoning becomes an ADR in [`docs/adr/`](docs/adr/) (`NNNN-slug.md`, sequential), and real work
**not scheduled** becomes a GitHub issue. There is no third state and no truth-home directory — a
behaviour change is reviewed with the diff, not promoted to a page.

### Where a fact goes

Four homes, one owner each:

| Home | Holds |
|---|---|
| `faststream_outbox/` | anything readable from the module — the default |
| a named test | an **invariant**: must stay true, and a change could silently break it |
| `docs/adr/` | a rejected alternative, with the reasoning that would otherwise be re-litigated |
| `docs/` | anything a user needs |

Before writing a line anywhere:

> Can an agent get this by reading `faststream_outbox/`? → **don't write it.**
> Would a wrong change here fail a test? → it belongs **in the test**, not in prose.
> Does a user need it? → **`docs/`**.
> Otherwise it does not get written.

**Prose about mechanism has no home. There is no file to add a paragraph to.** This file included:
it is always loaded, so a line that restates a docstring, a justfile comment, or `pyproject.toml`
costs every turn and rots in two places at once.

An invariant is a test whose name is the claim, with a docstring opening `INVARIANT:` and a second
paragraph naming **what breaks it** — design rationale, not a report of what this one test catches;
a sibling test may be the one that trips. `tests/test_invariant_census.py` enforces that shape. Both
ADRs and `INVARIANT:` docstrings ratchet: nothing prunes a record once its call is settled. Keeping
them lean is a standing habit.

## Code Style

- **Never use local/inline imports** — tests included, no `if TYPE_CHECKING` exception. `ruff`
  catches them; if `# noqa: PLC0415` looks like the fix, hoist the import instead.
- Docstrings: public API documents the contract; internal helpers get a one-line contract, plus at
  most 1–2 lines for a genuinely non-obvious constraint. Never narrate implementation or justify
  code to a reviewer — cross-file rationale lives in an `INVARIANT:` test docstring or an ADR.
- Lint suppressions are intentional and carry their reason at the site. The recurring cluster is
  everything downstream of `BrokerUsecase`'s invariance on its config type.

## Agent skills

- **Issues and specs** — GitHub Issues on `modern-python/faststream-outbox`, via `gh`:
  [`docs/agents/issue-tracker.md`](docs/agents/issue-tracker.md)
- **Triage labels** — the five canonical roles: [`docs/agents/triage-labels.md`](docs/agents/triage-labels.md)
- **Domain docs** — single-context, `CONTEXT.md` + `docs/adr/`: [`docs/agents/domain.md`](docs/agents/domain.md)
