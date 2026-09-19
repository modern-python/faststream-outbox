# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root.
- **`docs/adr/`**: read ADRs that touch the area you're about to work in.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure

Single-context repo:

```
/
├── CONTEXT.md
├── docs/adr/
│   ├── 0001-metrics-recorders-not-unified.md
│   └── 0002-free-threading-is-compat-only.md
└── faststream_outbox/
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0001 (metrics recorders not unified), but worth reopening because…_

## Link style inside `docs/`

`docs/agents/` is excluded from the MkDocs build (`exclude_docs` in `mkdocs.yml`), so these files may
link to repo paths outside `docs/` freely — nothing validates or publishes them.

Files inside the **built** `docs/` tree are different. `just docs-build` runs `mkdocs --strict`, and
the same files are read on GitHub. Two rules keep a link working in both renderings:

- **Between files inside `docs/`, use a plain relative `.md` link.** MkDocs rewrites it to a site
  URL and GitHub follows it as a file. From one ADR to another, that is `[ADR-NNNN](NNNN-slug.md)`.
- **Never link from a built page inside `docs/` to a path outside it.** It cannot resolve in both
  renderings: MkDocs emits `links.not_found` and ships the link verbatim, so it 404s on the site.
  Cite `faststream_outbox/…`, `tests/…`, and root files as inline code, never as links.
