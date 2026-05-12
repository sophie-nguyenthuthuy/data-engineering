.PHONY: help install test lint fmt run evals doctor docker-build compose-up compose-down clean

help:
	@echo "streamdigest make targets:"
	@echo "  install       pip install -e .[dev] and install pre-commit hooks"
	@echo "  test          pytest (smoke only, no Ollama required)"
	@echo "  lint          ruff check + ruff format --check + mypy"
	@echo "  fmt           ruff format + ruff check --fix"
	@echo "  run           streamdigest run (ingest + enrich)"
	@echo "  evals         streamdigest evals (needs local Ollama)"
	@echo "  doctor        streamdigest doctor"
	@echo "  docker-build  build the Docker image"
	@echo "  compose-up    start app + Ollama via docker-compose"
	@echo "  compose-down  stop the compose stack"
	@echo "  clean         remove caches and build artifacts"

install:
	pip install -e ".[dev]"
	pre-commit install

test:
	pytest -q -m "not integration"

lint:
	ruff check .
	ruff format --check .
	mypy streamdigest

fmt:
	ruff format .
	ruff check --fix .

run:
	streamdigest run

evals:
	streamdigest evals

doctor:
	streamdigest doctor

docker-build:
	docker build -t streamdigest:local .

compose-up:
	docker compose up -d

compose-down:
	docker compose down

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
