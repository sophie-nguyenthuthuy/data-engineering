FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir -e .

COPY scripts ./scripts
COPY tests ./tests

ENV IC_DATA_DIR=/data IC_DB_PATH=/data/inflation.duckdb
VOLUME ["/data"]

EXPOSE 8000

CMD ["ic", "serve", "--host", "0.0.0.0", "--port", "8000"]
