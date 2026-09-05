# No automated validation of docs code fences

**Decision:** the docs CI guard does not parse, type-check, or execute the Python inside `docs/`
code fences. Code-sample correctness stays a human-review concern; CI guards links, anchors, and
page structure via `mkdocs build --strict` and stops there.

The 2026-07-05 docs audit fixed three broken code samples, including one calling `pub.publish({...})`
without the required `session=` — valid syntax, a `TypeError` at runtime. Three levels of fence
validation were weighed against that failure class and all three were rejected. The value/cost ratio
is poor here: the failure is rare (that audit was the first sweep to find broken samples, and its
backlog is closed), and the cheap in-tool guards already cover the structural drift class at zero
maintained code.

- **Syntax-parsing all fences** (AST) is cheap but would not have caught the `session=` bug — valid
  syntax — or any stale number.
- **Type-checking curated fences** against the real API would catch that class, but needs a
  fragment-vs-complete tagging convention the docs do not have. Most fences are intentional
  fragments (undefined `broker`, `...` bodies, bare signatures) that a type-checker floods with
  false positives, and retrofitting the convention across ~23 pages then maintaining it is overhead
  on every future doc edit. False positives on intentional fragments would also train reviewers to
  ignore the check, which is worse than no check.
- **Executing curated fences** (doctest-style) has the highest fidelity and needs a harness,
  fixtures, mocks, and a Postgres service for the integration samples.

**Revisit trigger:** doc code samples break ≥3 times within a release cycle, at which point curated
type-checking earns its keep; or a clean fragment-vs-complete convention emerges naturally (an
`examples/` directory of runnable snippets the docs `--8<--` include), which removes the
false-positive obstacle and makes type-checking or execution cheap to add.
