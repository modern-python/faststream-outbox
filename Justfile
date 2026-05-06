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

publish:
    rm -rf dist
    uv version $GITHUB_REF_NAME
    uv build
    uv publish --token $PYPI_TOKEN
