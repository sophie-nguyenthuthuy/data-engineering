FROM python:3.12-slim

WORKDIR /app

# Build-time deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir -e ".[dev]"

COPY tests/ tests/
COPY benchmarks/ benchmarks/
COPY examples/ examples/
COPY Makefile ./

ENTRYPOINT ["ppc"]
