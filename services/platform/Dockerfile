# Small, non-root runtime image for the platform's Python services.
# Dependencies are resolved from uv.lock, so builds are reproducible.
# Both stages use the same python image, so a version bump (Dependabot) moves them
# together: a venv built for one Python can't run on another. uv comes in as a binary.
FROM python:3.14-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev

FROM python:3.14-slim-bookworm
RUN useradd --system --uid 10001 --home /app app
COPY --from=build --chown=app:app /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER 10001
WORKDIR /app
