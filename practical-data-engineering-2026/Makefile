.PHONY: help up down logs build seed reset dashboard dagster shell test

help:
	@echo "Targets:"
	@echo "  up         start full stack (MinIO + Dagster + Streamlit)"
	@echo "  down       stop everything"
	@echo "  logs       tail logs"
	@echo "  build      rebuild images"
	@echo "  seed       run a one-shot ingest + transform outside Dagster"
	@echo "  reset      wipe MinIO + catalog + local data"
	@echo "  dashboard  open Streamlit"
	@echo "  dagster    open Dagster UI"

up:
	cp -n .env.example .env || true
	docker compose up -d --build
	@echo "Dagster:    http://localhost:3100"
	@echo "Streamlit:  http://localhost:8501"
	@echo "MinIO UI:   http://localhost:9101"

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

build:
	docker compose build

seed:
	docker compose run --rm dagster python -m re_pipeline.seed

reset:
	docker compose down -v
	rm -rf dagster_home/catalog.db dagster_home/storage dagster_home/history data/*.duckdb

dashboard:
	open http://localhost:8501 || xdg-open http://localhost:8501

dagster:
	open http://localhost:3100 || xdg-open http://localhost:3100
