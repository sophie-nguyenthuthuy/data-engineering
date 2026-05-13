.PHONY: install dev test lint format type bench validate quantile clean docker

PY ?= python3

install:
	$(PY) -m pip install -e .

dev:
	$(PY) -m pip install -e ".[dev]"

test:
	$(PY) -m pytest tests -q

lint:
	$(PY) -m ruff check src tests

format:
	$(PY) -m ruff format src tests

type:
	$(PY) -m mypy src

bench: validate quantile

validate:
	$(PY) -m aqp.cli validate --rows 20000 --queries 200 --eps 0.05 --delta 0.01

quantile:
	$(PY) -m aqp.cli quantile --rows 50000 --eps 0.01

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

docker:
	docker build -t aqp-coreset-engine:latest .
