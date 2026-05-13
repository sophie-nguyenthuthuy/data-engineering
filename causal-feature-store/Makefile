.PHONY: install dev test lint format type demo partition clean docker

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

demo:
	$(PY) -m cfs.cli demo

partition:
	$(PY) -m cfs.cli partition

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

docker:
	docker build -t causal-feature-store:latest .
