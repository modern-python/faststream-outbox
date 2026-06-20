---
status: draft
date: 2026-06-20
slug: flat-changes-generated-index
spec: flat-changes-generated-index
pr: null
---

# flat-changes-generated-index — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Flatten `planning/changes/`, make `status:` frontmatter the sole
lifecycle state, add a `summary:` field, and replace the hand-maintained README
Index with a stdlib generator (`just index`).

**Spec:** [`design.md`](./design.md)

**Branch:** `chore/flat-changes-generated-index`

**Commit strategy:** Per-task commits.

## Global constraints

- Python 3.13+. **All imports at module top** — no inline/local imports, tests
  included (CLAUDE.md).
- `ruff` runs `select = ["ALL"]`; the new `.py` files must pass `just lint`
  (`eof-fixer`, `ruff format`, `ruff check --fix`, `ty check`).
- Coverage is `--cov=.` with `--cov-fail-under=100` — the whole repo root is
  measured, so `planning/index.py` must reach 100% line coverage once a test
  imports it. The `if __name__ == "__main__":` guard carries `# pragma: no
  cover`.
- The generator is **stdlib-only** — no PyYAML (confirmed absent from the env).
- `summary:` frontmatter is **single-line** (no YAML folding) so the hand
  parser stays trivial.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context)
  <noreply@anthropic.com>`.

---

### Task 1: Flatten `changes/`, backfill `summary:`, rewrite cross-links

**Files:**
- Move: every `planning/changes/archive/<bundle>/` → `planning/changes/<bundle>/`
- Delete: `planning/changes/active/`, `planning/changes/archive/` (and their
  `.gitkeep`)
- Modify: the `design.md` frontmatter of all 15 moved bundles (add `summary:`)
- Modify: internal `changes/archive/...` links in bundle bodies

Collapse the two-level layout to a flat one, move each bundle's curated Index
line into its own `summary:` frontmatter, and repair links that referenced the
old `archive/` path. No generator yet — this task just reshapes the data.

- [ ] **Step 1: Move every bundle up one level**

  ```bash
  git mv planning/changes/archive/* planning/changes/
  git rm planning/changes/active/.gitkeep planning/changes/archive/.gitkeep
  rmdir planning/changes/active planning/changes/archive
  ```

  Verify: `ls planning/changes/` shows the 15 archived bundles **plus**
  `2026-06-20.01-flat-changes-generated-index/`, and no `active/` / `archive/`.

- [ ] **Step 2: Backfill `summary:` into each bundle's `design.md`**

  In each file below, insert a `summary:` line **immediately after** the
  `slug:` line in the frontmatter. Values (verbatim, single line):

  | bundle `design.md` | `summary:` value |
  |---|---|
  | `2026-06-19.02-docs-diataxis-nav` | `Dissolve the standalone Patterns nav section: fold the messaging-service case study into Guides and move the file to docs/usage/ (no section renames). Seven top-level sections to six.` |
  | `2026-06-19.01-messaging-service-patterns-doc` | `New docs/patterns/ section with one page composing the outbox in an anonymized chat/notifications service: transactional event relay, fire-unless-cancelled timer, nested test brokers.` |
  | `2026-06-16.01-actionable-schema-drift-error` | `validate_schema() appends a hand-written-migration pointer to its RuntimeError for Alembic-blind drift (the outbox_lease_ck CHECK and partial-index predicates autogenerate cannot remediate).` |
  | `2026-06-13.01-portable-planning-convention` | `Two-axis OpenSpec-shaped convention: architecture/ truth + changes/ folder bundles, .NN intra-day tiebreak, three lanes, dedicated audits/+retros/, portable README. Supersedes planning-conventions.` |
  | `2026-06-12.01-docs-tutorials` | `The two tutorials deferred from #56: Your first outbox app and Add a Kafka relay. Kill-Kafka step folded into an at-least-once callout after aiokafka absorbed the outage.` |
  | `2026-06-11.02-docs-tutorials-and-observability-split` | `Three-way split of usage/observability.md into Reference + How-to + Explanation; tutorials deferred to #58.` |
  | `2026-06-11.01-operator-pages` | `docs/operations/: Production checklist, Troubleshooting playbook, Alembic migrations. The B follow-on from #50.` |
  | `2026-06-10.02-docs-landing-and-comparison` | `Docs landing rewrite, four-section nav reshape, new Comparison page.` |
  | `2026-06-10.01-planning-conventions` | `Spec/plan boundary, active/archived/_templates layout, frontmatter, migration of the existing pairs. Superseded by portable-planning-convention.` |
  | `2026-06-09.02-drain-test-flaky-fetch-observation` | `Drain test waits via the fetched recorder instead of an SQL poll, killing a 3.14 coverage flake.` |
  | `2026-06-09.01-mkdocs-github-pages` | `Docs hosting moves from Read the Docs to GitHub Pages on faststream-outbox.modern-python.org.` |
  | `2026-06-04.02-foreign-broker-relay` | `OutboxSubscriber officially supports the FastStream-native decorator relay to Kafka/Rabbit/NATS/Redis with three guardrails.` |
  | `2026-06-04.01-faststream-0.7.1-testbroker-typing` | `Adopt FastStream 0.7.1 TestBroker[Broker, EnterType] typing fix; drop two ty:ignore directives.` |
  | `2026-06-03.02-faststream-0.7-migration` | `Migrate to faststream>=0.7,<0.8; fix mechanical break points; drop per-call middlewares= kwarg.` |
  | `2026-06-03.01-all-extra-and-planning-dir` | `Add faststream-outbox[all] aggregate extra; bootstrap the planning/ directory itself.` |

  Example (the `slug:` → `summary:` insertion in
  `2026-06-19.02-docs-diataxis-nav/design.md`):

  ```yaml
  status: shipped
  date: 2026-06-19
  slug: docs-diataxis-nav
  summary: Dissolve the standalone Patterns nav section: fold the messaging-service case study into Guides and move the file to docs/usage/ (no section renames). Seven top-level sections to six.
  supersedes: null
  superseded_by: null
  pr: 104
  ```

- [ ] **Step 3: Verify every bundle now has a summary**

  Run:

  ```bash
  for f in planning/changes/*/design.md planning/changes/*/change.md; do
    [ -f "$f" ] && grep -q '^summary:' "$f" || echo "MISSING: $f"
  done
  ```

  Expected: no output (every spec file has `summary:`; no `change.md` files
  exist yet, so the glob for them is harmless).

- [ ] **Step 4: Rewrite internal `changes/archive/...` links**

  Run `grep -rn 'changes/archive' planning CLAUDE.md` and rewrite each hit's
  path from `changes/archive/<bundle>/` to `changes/<bundle>/`. Known hits:
  - `planning/changes/2026-06-13.01-portable-planning-convention/design.md`
  - `planning/changes/2026-06-13.01-portable-planning-convention/plan.md`
  - `planning/changes/2026-06-16.01-actionable-schema-drift-error/plan.md`

  (CLAUDE.md hits are handled in Task 4; leave them for now.) Then verify the
  bundle bodies are clean:

  ```bash
  grep -rn 'changes/archive' planning/changes
  ```

  Expected: no output.

- [ ] **Step 5: Commit**

  ```bash
  git add -A planning/changes
  git commit -m "refactor(planning): flatten changes/ and add summary frontmatter

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: The index generator + `just index`

**Files:**
- Create: `planning/index.py`
- Create: `tests/test_planning_index.py`
- Modify: `Justfile` (add `index` recipe)

A stdlib-only script that reads bundle frontmatter and prints a Markdown
listing grouped by lifecycle status to stdout. TDD: tests first.

- [ ] **Step 1: Write the failing test**

  Create `tests/test_planning_index.py`:

  ```python
  import importlib.util
  import pathlib

  import pytest

  _INDEX_PATH = pathlib.Path(__file__).parent.parent / "planning" / "index.py"
  _spec = importlib.util.spec_from_file_location("planning_index", _INDEX_PATH)
  assert _spec is not None
  assert _spec.loader is not None
  index = importlib.util.module_from_spec(_spec)
  _spec.loader.exec_module(index)


  def test_parse_frontmatter_reads_fields_and_normalizes_null() -> None:
      text = '---\nstatus: shipped\nslug: x\npr: "41"\nsupersedes: null\n---\nbody\n'
      fields = index.parse_frontmatter(text)
      assert fields["status"] == "shipped"
      assert fields["slug"] == "x"
      assert fields["pr"] == "41"  # surrounding quotes stripped
      assert fields["supersedes"] == ""  # "null" normalized to empty


  def test_parse_frontmatter_no_frontmatter_returns_empty() -> None:
      assert index.parse_frontmatter("no leading marker\n") == {}


  def test_parse_frontmatter_skips_lines_without_separator() -> None:
      fields = index.parse_frontmatter("---\nstatus: draft\njunkline\n---\n")
      assert fields == {"status": "draft"}


  def test_load_bundles_reads_design_then_change_and_skips_others(
      tmp_path: pathlib.Path,
      monkeypatch: pytest.MonkeyPatch,
  ) -> None:
      changes = tmp_path / "changes"
      (changes / "a-bundle").mkdir(parents=True)
      (changes / "a-bundle" / "design.md").write_text(
          "---\nstatus: shipped\ndate: 2026-01-02\nslug: a\nsummary: A.\npr: 1\n---\n",
          encoding="utf-8",
      )
      (changes / "b-bundle").mkdir()
      (changes / "b-bundle" / "change.md").write_text(
          "---\nstatus: draft\ndate: 2026-01-01\nslug: b\nsummary: B.\n---\n",
          encoding="utf-8",
      )
      (changes / "empty-bundle").mkdir()  # no spec file -> skipped
      (changes / "loose.txt").write_text("not a dir bundle", encoding="utf-8")
      monkeypatch.setattr(index, "CHANGES_DIR", changes)

      bundles = index.load_bundles()

      slugs = {b["slug"] for b in bundles}
      assert slugs == {"a", "b"}
      a = next(b for b in bundles if b["slug"] == "a")
      assert a["path"] == "changes/a-bundle/design.md"
      b = next(b for b in bundles if b["slug"] == "b")
      assert b["path"] == "changes/b-bundle/change.md"


  def test_render_groups_sorts_and_renders_supersede_links() -> None:
      bundles = [
          {"status": "draft", "date": "2026-02-01", "slug": "wip",
           "pr": "", "summary": "Work in progress.", "path": "changes/wip/design.md"},
          {"status": "shipped", "date": "2026-01-02", "slug": "newer",
           "pr": "10", "summary": "Newer.", "path": "changes/newer/design.md",
           "supersedes": "older"},
          {"status": "shipped", "date": "2026-01-01", "slug": "older",
           "pr": "9", "summary": "Older.", "path": "changes/older/design.md"},
          {"status": "superseded", "date": "2026-01-01", "slug": "gone",
           "pr": "8", "summary": "", "path": "changes/gone/design.md",
           "superseded_by": "newer"},
      ]
      out = index.render(bundles)
      assert "## In progress" in out
      assert "## Shipped" in out
      assert "## Superseded" in out
      # In-progress entry has no pr -> em dash placeholder
      assert "- **[wip](changes/wip/design.md)** (#—, 2026-02-01) — Work in progress." in out
      # Shipped sorted newest first
      assert out.index("newer") < out.index("older")
      assert "_(supersedes older)_" in out
      # Missing summary -> placeholder
      assert "(no summary)" in out
      assert "_(superseded by newer)_" in out


  def test_render_empty_group_prints_none() -> None:
      out = index.render([{"status": "shipped", "date": "2026-01-01",
                           "slug": "s", "pr": "1", "summary": "S.",
                           "path": "changes/s/design.md"}])
      # No draft/approved bundles -> In progress group is None
      in_progress = out.split("## Shipped")[0]
      assert "_None._" in in_progress


  def test_main_writes_listing_to_stdout(capsys: pytest.CaptureFixture[str]) -> None:
      rc = index.main()
      captured = capsys.readouterr()
      assert rc == 0
      assert "# Change index" in captured.out
  ```

- [ ] **Step 2: Run the test to verify it fails**

  Run: `uv run pytest tests/test_planning_index.py -v --no-cov`
  Expected: collection-time `FileNotFoundError` / `ModuleNotFoundError` for
  `planning/index.py` (the file does not exist yet).

- [ ] **Step 3: Write the generator**

  Create `planning/index.py`:

  ```python
  """Generate the planning change index from bundle frontmatter.

  Run via ``just index``. Globs ``planning/changes/*/``, reads each bundle's
  ``design.md`` (falling back to ``change.md``) frontmatter, and prints a
  Markdown listing grouped by lifecycle status to stdout. Never writes a file:
  the listing is a query over the bundles, not a committed artifact.
  """

  import pathlib
  import sys

  CHANGES_DIR = pathlib.Path(__file__).parent / "changes"
  GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
      ("In progress", ("draft", "approved")),
      ("Shipped", ("shipped",)),
      ("Superseded", ("superseded",)),
  )


  def parse_frontmatter(text: str) -> dict[str, str]:
      """Parse a single-line-scalar YAML frontmatter block into a dict."""
      lines = text.splitlines()
      if not lines or lines[0].strip() != "---":
          return {}
      fields: dict[str, str] = {}
      for line in lines[1:]:
          if line.strip() == "---":
              break
          key, sep, value = line.partition(": ")
          if not sep:
              continue
          cleaned = value.strip().strip('"').strip("'")
          fields[key.strip()] = "" if cleaned == "null" else cleaned
      return fields


  def load_bundles() -> list[dict[str, str]]:
      """Read every bundle's spec frontmatter under ``CHANGES_DIR``."""
      bundles: list[dict[str, str]] = []
      for bundle in sorted(CHANGES_DIR.iterdir()):
          if not bundle.is_dir():
              continue
          spec = bundle / "design.md"
          if not spec.exists():
              spec = bundle / "change.md"
          if not spec.exists():
              continue
          fields = parse_frontmatter(spec.read_text(encoding="utf-8"))
          fields["path"] = f"changes/{bundle.name}/{spec.name}"
          bundles.append(fields)
      return bundles


  def format_row(bundle: dict[str, str]) -> str:
      """Render one bundle as a Markdown list item."""
      slug = bundle.get("slug", "?")
      path = bundle.get("path", "")
      pr = bundle.get("pr") or "—"
      date = bundle.get("date", "")
      summary = bundle.get("summary") or "(no summary)"
      line = f"- **[{slug}]({path})** (#{pr}, {date}) — {summary}"
      if bundle.get("supersedes"):
          line += f" _(supersedes {bundle['supersedes']})_"
      if bundle.get("superseded_by"):
          line += f" _(superseded by {bundle['superseded_by']})_"
      return line


  def render(bundles: list[dict[str, str]]) -> str:
      """Render the full grouped Markdown listing."""
      out = ["# Change index", "", "_Generated by `just index` — do not edit._", ""]
      for title, statuses in GROUPS:
          out += [f"## {title}", ""]
          rows = sorted(
              (b for b in bundles if b.get("status") in statuses),
              key=lambda b: (b.get("date", ""), b.get("slug", "")),
              reverse=True,
          )
          out += [format_row(b) for b in rows] if rows else ["_None._"]
          out.append("")
      return "\n".join(out).rstrip() + "\n"


  def main() -> int:
      """Print the listing to stdout."""
      sys.stdout.write(render(load_bundles()))
      return 0


  if __name__ == "__main__":  # pragma: no cover
      raise SystemExit(main())
  ```

- [ ] **Step 4: Run the test to verify it passes**

  Run: `uv run pytest tests/test_planning_index.py -v --no-cov`
  Expected: all tests PASS.

- [ ] **Step 5: Add the `just index` recipe**

  In `Justfile`, after the `lint-ci` block, add:

  ```just
  # Print the planning change index (grouped by status) to stdout.
  index:
      uv run python planning/index.py
  ```

- [ ] **Step 6: Smoke-run the generator against the real bundles**

  Run: `just index`
  Expected: a `# Change index` heading, an `## In progress` group listing
  `flat-changes-generated-index`, a `## Shipped` group with the 14 shipped
  bundles newest-first, and a `## Superseded` group with `planning-conventions`.
  No `(no summary)` placeholders.

- [ ] **Step 7: Confirm full-suite coverage stays at 100%**

  Run: `uv run pytest tests/test_unit.py tests/test_planning_index.py`
  Expected: PASS with coverage at 100% (the `__main__` guard is excluded via
  `# pragma: no cover`). If `planning/index.py` shows a missing line, add a
  test that exercises it.

- [ ] **Step 8: Lint**

  Run: `just lint`
  Expected: clean. Fix any ruff/ty findings on the two new files.

- [ ] **Step 9: Commit**

  ```bash
  git add planning/index.py tests/test_planning_index.py Justfile
  git commit -m "feat(planning): add stdlib index generator and just index recipe

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Slim the README and update templates

**Files:**
- Modify: `planning/README.md`
- Modify: `planning/_templates/design.md`
- Modify: `planning/_templates/change.md`

Delete the derived Index, rewrite the Conventions prose for the flat layout +
single-step lifecycle, and teach the templates the new `summary:` field.

- [ ] **Step 1: Delete the Index, point at the generator**

  In `planning/README.md`, delete the entire `## Index` section (the `###
  Active` and `### Archived (shipped)` subsections and every entry). Replace it
  with:

  ```markdown
  ## Index

  The change listing is **generated**, not maintained — run `just index` to
  print it (grouped by `status`: In progress / Shipped / Superseded). The
  frontmatter in each bundle is the single source of truth; there is no
  committed copy to drift.
  ```

- [ ] **Step 2: Rewrite the Conventions prose for the flat layout**

  In `planning/README.md`:
  - In **"Two axes, never mixed"**, change the `planning/changes/` bullet to
    describe a flat directory and drop "frozen once shipped / archives the
    bundle". Replace the "Shipping a change promotes…then archives the bundle"
    paragraph with: promotion into `architecture/<capability>.md` by hand is the
    **only** ship-time step; there is no folder move.
  - In **"Change bundles"**, change the path from
    `changes/active/YYYY-MM-DD.NN-<slug>/` to
    `changes/YYYY-MM-DD.NN-<slug>/`, and replace the "On merge the folder moves
    to `changes/archive/` … and its line moves from Active to Archived"
    paragraph with the single-step lifecycle: the implementing PR sets
    `status: shipped` and fills `pr` / `outcome` / `summary` **in the branch**,
    alongside the code and the `architecture/` promotion — no post-merge
    bookkeeping.
  - In **"Frontmatter"**, add `summary` (single line) to the `design.md` /
    `change.md` field list.

- [ ] **Step 3: Add `summary:` to the templates**

  In `planning/_templates/design.md` and `planning/_templates/change.md`,
  insert into the frontmatter immediately after the `slug: my-change` line:

  ```yaml
  summary: One line — shown in the generated index. Fill at ship time.
  ```

- [ ] **Step 4: Verify no stale references remain in the README**

  Run: `grep -nE 'active/|archive/|### Active|### Archived' planning/README.md`
  Expected: no output (all directory-split language is gone).

- [ ] **Step 5: Commit**

  ```bash
  git add planning/README.md planning/_templates
  git commit -m "docs(planning): slim README to conventions, point Index at just index

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: Update CLAUDE.md Workflow section

**Files:**
- Modify: `CLAUDE.md` (the `## Workflow` section)

Bring the repo's agent instructions in line with the flat layout and
single-step lifecycle.

- [ ] **Step 1: Rewrite the Workflow paragraph**

  In `CLAUDE.md` `## Workflow`:
  - Change the per-feature path `planning/changes/active/YYYY-MM-DD.NN-<slug>/`
    to `planning/changes/YYYY-MM-DD.NN-<slug>/` (both `design.md` and
    `plan.md` occurrences).
  - Replace "On merge, the bundle moves to `planning/changes/archive/` with
    `status: shipped`, `pr:`, and `outcome:` filled, **and** the change
    promotes its conclusions into the affected `architecture/<capability>.md`"
    with: the implementing PR sets `status: shipped` and fills `pr` / `outcome`
    / `summary` in-branch and promotes its conclusions into the affected
    `architecture/<capability>.md` — the promotion is the only ship-time step;
    there is no folder move.
  - Add a sentence: the change listing is generated — run `just index` (no
    committed Index).

- [ ] **Step 2: Verify no stale path references remain**

  Run: `grep -nE 'changes/active|changes/archive' CLAUDE.md`
  Expected: no output.

- [ ] **Step 3: Commit**

  ```bash
  git add CLAUDE.md
  git commit -m "docs: update CLAUDE.md workflow for flat changes/ and just index

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 5: Ship-time bookkeeping (in-branch, per the new convention)

**Files:**
- Modify: `planning/changes/2026-06-20.01-flat-changes-generated-index/design.md`
- Modify: `planning/changes/2026-06-20.01-flat-changes-generated-index/plan.md`

Eat the new convention's own dog food: flip this bundle to `shipped` in the
same branch, before merge. Do this **last**, once the PR number is known.

- [ ] **Step 1: Set the bundle frontmatter to shipped**

  In this bundle's `design.md` frontmatter set `status: shipped`, `pr: <N>`,
  and `outcome: <one line on what landed>`. In `plan.md` set `status: shipped`
  and `pr: <N>`. (`summary:` is already present.)

- [ ] **Step 2: Confirm the generator places it under Shipped**

  Run: `just index`
  Expected: `flat-changes-generated-index` now appears under `## Shipped`, and
  `## In progress` shows `_None._`.

- [ ] **Step 3: Final full verification**

  Run: `just lint-ci` and `just test`
  Expected: both green.

- [ ] **Step 4: Commit**

  ```bash
  git add planning/changes/2026-06-20.01-flat-changes-generated-index
  git commit -m "chore(planning): mark flat-changes-generated-index shipped

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

## Self-review notes

- **Spec coverage:** §1 flat dir → Task 1; §2 `summary:` → Task 1 (backfill) +
  Task 3 (templates); §3 generator → Task 2; §4 slim README → Task 3; §5
  single-step lifecycle → Tasks 3, 4, 5; §6 CLAUDE.md + cross-links → Task 1
  (bundle links) + Task 4 (CLAUDE.md). Testing/Risk greps map to the verify
  steps in Tasks 1–4.
- **Type consistency:** `parse_frontmatter`, `load_bundles`, `format_row`,
  `render`, `main`, `CHANGES_DIR`, `GROUPS` are referenced identically in the
  generator and its test.
