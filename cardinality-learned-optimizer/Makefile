.PHONY: install test lint format clean db-up db-down setup-imdb generate-queries train reproduce

PYTHON := python3
PIP := pip3

install:
	$(PIP) install -e ".[dev]"

test:
	pytest tests/ -v --cov=cle --cov-report=term-missing

test-fast:
	pytest tests/ -v -x -q

lint:
	ruff check src/ tests/ scripts/ experiments/

format:
	ruff check --fix src/ tests/ scripts/ experiments/
	ruff format src/ tests/ scripts/ experiments/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -rf .pytest_cache .ruff_cache dist build *.egg-info

# ── Docker ────────────────────────────────────────────────────────────────────

db-up:
	docker compose up -d postgres
	@echo "Waiting for PostgreSQL..."
	@until docker compose exec -T postgres pg_isready -U postgres; do sleep 1; done
	@echo "PostgreSQL ready."

db-down:
	docker compose down

db-logs:
	docker compose logs -f postgres

# ── Data setup ────────────────────────────────────────────────────────────────

setup-imdb: db-up
	bash scripts/setup_imdb.sh

generate-queries:
	$(PYTHON) experiments/job_queries/generate_job_sample.py

# ── Training & experiments ────────────────────────────────────────────────────

train: generate-queries
	$(PYTHON) scripts/train_model.py \
		--queries experiments/job_queries \
		--epochs 500 \
		--output checkpoints/pretrained.pt

reproduce: generate-queries
	$(PYTHON) experiments/reproduce_bao.py \
		--queries experiments/job_queries \
		--results results/bao_comparison.json \
		--profile-cache results/plan_profiles.json \
		--train-steps 500

reproduce-no-adaptive: generate-queries
	$(PYTHON) experiments/reproduce_bao.py \
		--queries experiments/job_queries \
		--results results/bao_no_adaptive.json \
		--no-adaptive \
		--train-steps 500

# ── CI ────────────────────────────────────────────────────────────────────────

ci: lint test
