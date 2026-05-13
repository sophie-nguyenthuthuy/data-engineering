.PHONY: install dev test lint format type run bench edges clean docker

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

run:
	$(PY) -m ace.cli run --trials 100

bench:
	$(PY) -m ace.cli bench --trials 200

edges:
	$(PY) -m ace.cli edges

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

docker:
	docker build -t adversarial-chaos-engine:latest .
