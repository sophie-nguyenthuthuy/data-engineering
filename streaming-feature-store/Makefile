.PHONY: help up down logs test drift-check seed-training build

help:
	@echo "Streaming Feature Store"
	@echo ""
	@echo "  make up              Start all services"
	@echo "  make down            Stop all services and remove volumes"
	@echo "  make build           Rebuild Docker images"
	@echo "  make logs            Tail all service logs"
	@echo "  make test            Run unit tests (no Docker required)"
	@echo "  make seed-training   Generate and save a training snapshot"
	@echo "  make drift-check     Run nightly drift detection immediately"
	@echo "  make demo            Full demo: up + seed + wait for drift + check"

up:
	docker compose up -d
	@echo "Services started. API at http://localhost:8000"

down:
	docker compose down -v

build:
	docker compose build

logs:
	docker compose logs -f

test:
	pip install -q -r requirements.txt
	pytest tests/ -v --tb=short

seed-training:
	@echo "Generating training snapshot…"
	python scripts/seed_training.py

drift-check:
	@echo "Running drift check now…"
	docker compose exec scheduler python -m scheduler.nightly_job --run-now

demo: up
	@echo "Waiting for services to be ready…"
	sleep 15
	$(MAKE) seed-training
	@echo ""
	@echo "Simulator is sending events. After 500 events the distribution will drift."
	@echo "Check API: http://localhost:8000/drift/latest"
	@echo "To trigger drift check now: make drift-check"
