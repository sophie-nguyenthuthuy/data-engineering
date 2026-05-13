FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --upgrade pip build && python -m build --wheel

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --system msc && useradd --system --gid msc --home /home/msc --create-home msc

WORKDIR /app
COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir "/tmp/*.whl[excel]" || pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

USER msc
ENTRYPOINT ["mscctl"]
CMD ["info"]
