FROM python:3.11-slim

WORKDIR /app

# System deps for lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev libxslt-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]" || pip install --no-cache-dir -e .

COPY . .

RUN mkdir -p data

# Default: run the API server.
# Override CMD to run the scheduler:  docker run ... sre schedule
ENV API_HOST=0.0.0.0
ENV API_PORT=8000

EXPOSE 8000

CMD ["sre", "serve", "--host", "0.0.0.0", "--port", "8000"]
