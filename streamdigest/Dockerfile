# syntax=docker/dockerfile:1.7

# --- Build stage ----------------------------------------------------------
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install build deps first to maximize layer caching.
COPY pyproject.toml README.md ./
COPY streamdigest ./streamdigest

RUN pip install --upgrade pip && pip install --prefix=/install .

# --- Runtime stage --------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DUCKDB_PATH=/data/streamdigest.duckdb \
    OLLAMA_HOST=http://ollama:11434

# Non-root user.
RUN groupadd --system --gid 1000 app \
    && useradd --system --uid 1000 --gid app --home /app app \
    && mkdir -p /data \
    && chown -R app:app /data

COPY --from=builder /install /usr/local

WORKDIR /app
COPY --chown=app:app streamdigest ./streamdigest
COPY --chown=app:app pyproject.toml README.md ./

USER app
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD streamdigest doctor || exit 1

ENTRYPOINT ["streamdigest"]
CMD ["doctor"]
