default: install lint build test

down:
    docker compose down --remove-orphans --volumes

sh:
    docker compose run --service-ports application bash

test *args: down && down
    docker compose run application uv run pytest {{ args }}

build:
    docker compose build application

install:
    uv lock --upgrade
    uv sync --all-extras --all-groups --frozen

lint:
    uv run eof-fixer .
    uv run ruff format
    uv run ruff check --fix
    uv run ty check

lint-ci:
    uv run eof-fixer . --check
    uv run ruff format --check
    uv run ruff check --no-fix
    uv run ty check
    uv run python planning/index.py --check

# Print the planning change index (flat, newest-first) to stdout.
index:
    uv run python planning/index.py

# Validate planning changes + decisions (frontmatter, lanes, spec links); CI runs this.
check-planning:
    uv run python planning/index.py --check

# Auth via PyPI Trusted Publishing (OIDC); uv publish auto-detects the CI id-token.
publish:
    rm -rf dist
    uv version $GITHUB_REF_NAME
    uv build
    uv publish

# Serve docs at http://127.0.0.1:8000 with hot-reload on save.
docs-serve:
    uvx --with-requirements docs/requirements.txt mkdocs serve

# One-shot strict build; fails on broken links or orphaned pages.
docs-build:
    uvx --with-requirements docs/requirements.txt mkdocs build --strict
