# Releases

How `faststream-outbox` versions and ships to PyPI, plus the per-version notes
in this directory.

## Versioning is tag-driven

The package is **published by creating a GitHub release + git tag named with
the bare semver** (e.g. `0.10.3`, no `v` prefix). CI publishes to PyPI from the
tag. `pyproject.toml` pins `version = "0"`; the real version is **derived from
the tag at build time**, so cutting a release needs **no `pyproject` edit**.

Semver (pre-1.0, `0.x`): the org leans on **patch** bumps even for behavior
changes, but a release that adds schema migrations or input validation
rejecting previously-accepted calls is honestly a **minor** bump — size it that
way.

## Release notes

One file per version, `planning/releases/<version>.md` (e.g. `0.10.3.md`).
Format mirrors the sister project `httpware`'s `planning/releases/`:

- `# <pkg> <ver> — <headline>`
- a bold one-line summary
- gap / fix sections
- **Migration** — what users must do to upgrade
- **Touched surface** — files/areas changed
- **See also** — PR links

Minor releases add a **Breaking changes** section.

## Procedure

1. Write the notes file first; get it reviewed.
2. **Only on an explicit go** (this step publishes): create the tag + GitHub
   release, using the notes file as the release body.
