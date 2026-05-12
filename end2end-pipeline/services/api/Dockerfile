FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install .

RUN useradd --create-home --uid 10002 app && chown -R app:app /app
USER app

CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${API_PORT:-8000}"]
