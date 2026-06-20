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
        {
            "status": "draft",
            "date": "2026-02-01",
            "slug": "wip",
            "pr": "",
            "summary": "Work in progress.",
            "path": "changes/wip/design.md",
        },
        {
            "status": "shipped",
            "date": "2026-01-02",
            "slug": "newer",
            "pr": "10",
            "summary": "Newer.",
            "path": "changes/newer/design.md",
            "supersedes": "older",
        },
        {
            "status": "shipped",
            "date": "2026-01-01",
            "slug": "older",
            "pr": "9",
            "summary": "Older.",
            "path": "changes/older/design.md",
        },
        {
            "status": "superseded",
            "date": "2026-01-01",
            "slug": "gone",
            "pr": "8",
            "summary": "",
            "path": "changes/gone/design.md",
            "superseded_by": "newer",
        },
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
    out = index.render(
        [
            {
                "status": "shipped",
                "date": "2026-01-01",
                "slug": "s",
                "pr": "1",
                "summary": "S.",
                "path": "changes/s/design.md",
            }
        ]
    )
    # No draft/approved bundles -> In progress group is None
    in_progress = out.split("## Shipped")[0]
    assert "_None._" in in_progress


def test_main_writes_listing_to_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    rc = index.main()
    captured = capsys.readouterr()
    assert rc == 0
    assert "# Change index" in captured.out
