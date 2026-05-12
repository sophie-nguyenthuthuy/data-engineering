.PHONY: install dev lint test test-cov clean infra infra-down seed-s3 replay-dry

# ── Setup ─────────────────────────────────────────────────────────────────────
install:
	pip install -e .

dev:
	pip install -e ".[dev]"

# ── Code quality ──────────────────────────────────────────────────────────────
lint:
	ruff check src tests
	ruff format --check src tests

fmt:
	ruff format src tests
	ruff check --fix src tests

typecheck:
	mypy src

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	pytest -v

test-cov:
	pytest --cov=src/replay --cov-report=term-missing --cov-report=html -v

# ── Local infrastructure ──────────────────────────────────────────────────────
infra:
	docker compose up -d --wait

infra-ui:
	docker compose --profile ui up -d --wait

infra-down:
	docker compose down -v

# ── Seed a fake archive into LocalStack S3 ────────────────────────────────────
seed-s3:
	@echo "Creating bucket and uploading sample events to LocalStack..."
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
	  aws --endpoint-url=http://localhost:4566 s3 mb s3://my-kafka-archive-bucket 2>/dev/null || true
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
	  aws --endpoint-url=http://localhost:4566 s3 cp \
	    tests/fixtures/sample_events.jsonl \
	    s3://my-kafka-archive-bucket/orders/0000/orders+0000+00000000000000000001.json

# ── Dry-run against LocalStack ────────────────────────────────────────────────
replay-dry: infra seed-s3
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
	  replay run \
	    --bucket my-kafka-archive-bucket \
	    --topics orders \
	    --days 30 \
	    --target stdout \
	    --endpoint-url http://localhost:4566 \
	    --dry-run \
	    --verbose

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache .pytest_cache htmlcov .coverage dist build
