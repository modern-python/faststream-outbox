FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends iptables \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV UV_PROJECT_ENVIRONMENT=/code/.venv \
    UV_NO_MANAGED_PYTHON=1 \
    UV_NO_CACHE=true \
    UV_LINK_MODE=copy

WORKDIR /code

COPY pyproject.toml uv.lock ./

RUN uv sync --all-extras --frozen --no-install-project
