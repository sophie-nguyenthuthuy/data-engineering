.PHONY: setup seed lambda-demo kappa-demo backfill validate docker-up docker-down test clean

# ── Setup ─────────────────────────────────────────────────────────────────────
setup:
	pip install -r requirements.txt

# ── Data seeding ──────────────────────────────────────────────────────────────
seed:
	python scripts/seed_data.py

# ── Architecture demos (local mode — no Kafka required) ───────────────────────
lambda-demo: seed
	LOCAL_MODE=true python scripts/run_lambda_demo.py

kappa-demo: seed
	LOCAL_MODE=true python scripts/run_kappa_demo.py

# ── Backfill (writes to Kafka — requires Docker stack) ───────────────────────
backfill:
	python scripts/run_backfill.py

backfill-local:
	LOCAL_MODE=true python scripts/run_backfill.py --local

# ── Correctness validation ────────────────────────────────────────────────────
validate: seed
	LOCAL_MODE=true python scripts/run_validation.py --local

validate-kafka: seed
	python scripts/run_validation.py

# ── Docker ───────────────────────────────────────────────────────────────────
docker-up:
	docker-compose up -d
	@echo "Waiting for Kafka to be healthy..."
	@sleep 15

docker-down:
	docker-compose down -v

docker-logs:
	docker-compose logs -f

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache htmlcov .coverage
	rm -f data/local_kafka.jsonl
	rm -f data/historical/*.json
