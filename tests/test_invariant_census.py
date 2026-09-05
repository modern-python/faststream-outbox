"""Census of the invariant tests themselves.

Every ``INVARIANT:`` docstring states what breaks it: the claim alone is a label, and the second
paragraph is the anti-refactor warning. Every test name cited from code or a docstring must resolve
to a real test, so a citation cannot rot into a pointer at nothing.
"""

import ast
import pathlib
import re


_REPO_ROOT = pathlib.Path(__file__).parent.parent
_TESTS_DIR = _REPO_ROOT / "tests"
_PACKAGE_DIR = _REPO_ROOT / "faststream_outbox"

_INVARIANT = "INVARIANT:"
# The claim paragraph, then the "what breaks it" paragraph -- fewer than two means the second is missing.
_MIN_PARAGRAPHS = 2
# Node-id form only -- the shape a citation takes. A bare ``test_``-prefixed word is as likely
# to be a fixture, a module, or a table name.
_CITATION = re.compile(r"::(test_[a-z0-9_]+)")


def _test_functions() -> list[tuple[pathlib.Path, ast.FunctionDef | ast.AsyncFunctionDef]]:
    found = []
    for path in sorted(_TESTS_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        found.extend(
            (path, node)
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name.startswith("test_")
        )
    return found


def test_every_invariant_states_what_breaks_it() -> None:
    """INVARIANT: an ``INVARIANT:`` docstring carries a second paragraph naming what breaks it.

    The claim alone is a label a refactor reads past. The rationale paragraph is the part that
    stops the change, and it has to be design rationale rather than a report of what this one test
    catches -- a sibling test may be the one that actually trips.
    """
    marked = [
        (path, node) for path, node in _test_functions() if (ast.get_docstring(node) or "").startswith(_INVARIANT)
    ]
    assert marked, "no test carries an INVARIANT: docstring; the convention is not in use"

    bare = sorted(
        f"{path.relative_to(_REPO_ROOT)}::{node.name}"
        for path, node in marked
        if len([part for part in (ast.get_docstring(node) or "").split("\n\n") if part.strip()]) < _MIN_PARAGRAPHS
    )
    assert not bare, f"INVARIANT tests with no 'what breaks it' paragraph: {bare}"


def test_every_cited_test_name_resolves() -> None:
    """INVARIANT: a node-id citation from source or a docstring resolves to a real test.

    Code comments cite tests instead of prose pages, so a citation is the only pointer a reader gets
    from the mechanism to the claim that pins it. A renamed or deleted test leaves the comment
    reading as an assurance that something is covered when nothing is.
    """
    known = {node.name for _, node in _test_functions()}
    dangling = sorted(
        f"{path.relative_to(_REPO_ROOT)}: {cited}"
        for path in [*_PACKAGE_DIR.rglob("*.py"), *_TESTS_DIR.rglob("*.py")]
        for cited in _CITATION.findall(path.read_text(encoding="utf-8"))
        if cited not in known
    )
    assert not dangling, f"cited test names that do not resolve: {dangling}"
