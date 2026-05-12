.PHONY: up down logs install test demo fail-kafka fail-warehouse fail-notification

up:
	docker compose up -d
	@echo "Waiting for services..."
	@sleep 10
	@echo "Stack ready. Run 'make demo' to start."

down:
	docker compose down -v

logs:
	docker compose logs -f

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v --tb=short

test-unit:
	pytest tests/test_recovery.py -v --tb=short -k "not TestRecoveryAgent"

demo:
	python scripts/run_demo.py

fail-kafka:
	python scripts/run_demo.py --fail kafka

fail-warehouse:
	python scripts/run_demo.py --fail warehouse

fail-notification:
	python scripts/run_demo.py --fail notification

simulate:
	python scripts/simulate_failures.py

psql:
	docker compose exec postgres psql -U pipeline -d payments
