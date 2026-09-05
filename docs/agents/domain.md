# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the
codebase. This repo is **single-context**: one package, one domain.

## Before exploring, read these

In order, stopping when you have what you need:

- **`CLAUDE.md`** at the repo root — the invariants list is the map. Each bullet links to the
  capability file that owns the detail.
- **`architecture/<capability>.md`** for the capability you are about to touch — the **truth home**
  for its implementation detail. `architecture/README.md` indexes them.
- **`planning/decisions/`** — settled design calls, including rejected options with the reasoning
  that would otherwise be re-litigated. `just index` lists them with their one-line summaries.
- **`CONTEXT.md`** at the repo root — the domain glossary, if it exists.

If any of these don't exist, **proceed silently**. Don't flag their absence; don't suggest creating
them upfront. There is no `CONTEXT.md` today: `/domain-modeling` creates one lazily when terms
actually get resolved.

## File structure

```
/
├── CLAUDE.md                  ← invariants + pointers into architecture/
├── CONTEXT.md                 ← glossary (not yet created)
├── architecture/              ← truth home, one file per capability
│   ├── producer.md
│   ├── subscriber.md
│   └── …
├── planning/
│   ├── changes/               ← per-change files (Full / Lightweight lanes)
│   ├── decisions/             ← ADR equivalent; see docs/agents/issue-tracker.md
│   └── _templates/
├── docs/                      ← user-facing site (MkDocs)
├── faststream_outbox/
└── tests/
```

There is no `CONTEXT-MAP.md` and no per-package `CONTEXT.md`, and no `docs/adr/`: the ADR role
belongs to `planning/decisions/`.

## Three homes, no fourth

A fact goes to exactly one of them, and picking wrong is how the corpus rots:

| Home                  | Holds                                                              |
| --------------------- | ------------------------------------------------------------------ |
| `architecture/<cap>.md` | how a capability actually works, and the invariants it rests on   |
| `planning/decisions/` | a call taken **without** a code change, especially a rejected option |
| `docs/`               | anything a user needs                                              |

**When a change alters a capability's behavior, update the matching `architecture/<capability>.md`
in the same PR.** That promotion is what keeps `architecture/` true; a behavior change that lands
without it silently turns the truth home into a lie.

Do not invent a fourth home. A paragraph of mechanism prose with nowhere to go is a signal the fact
belongs in the code or a test, not in a new file.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a
test name), use the term as the codebase uses it. The load-bearing ones are named in `CLAUDE.md` and
defined in `architecture/`: **lease** and **lease token**, **fetch loop** / **worker loop**,
**relay**, **timer** and **`timer_id`**, **DLQ**, **outcome** (`Ack` / `Retry` / `Terminal`),
**retry strategy**. Don't drift to synonyms — a row is *leased*, not locked; a handler returns an
*outcome*, not a status.

If the concept you need has no name yet, that's a signal: either you're inventing language the
project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Link style

`docs/agents/` is excluded from the MkDocs build (`exclude_docs` in `mkdocs.yml`), so these files
may link to repo paths outside `docs/` freely — nothing validates or publishes them.

Files inside the **built** `docs/` tree are different. `just docs-build` runs `mkdocs --strict`, and
the same files are read on GitHub. Two rules keep a link working in both renderings:

- **Between files inside `docs/`, use a plain relative `.md` link.** MkDocs rewrites it to a site
  URL and GitHub follows it as a file.
- **Never link from a built page inside `docs/` to a path outside it.** It cannot resolve in both
  renderings: MkDocs emits `links.not_found` and ships the link verbatim, so it 404s on the site.
  Cite `faststream_outbox/…`, `tests/…`, `architecture/…`, and root files as inline code, never as
  links.

## Flag conflicts

If your output contradicts an `architecture/` invariant or a `planning/decisions/` record, surface it
explicitly rather than silently overriding:

> _Contradicts `planning/decisions/2026-07-17-conn-union-is-deliberate.md`, but worth reopening
> because…_

A decision record carries a **Revisit trigger** for exactly this. Check whether the trigger has
actually fired before arguing the case again.
