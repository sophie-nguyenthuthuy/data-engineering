.PHONY: install dev test lint format type bench simulate clean docker

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

bench:
	$(PY) -m llo.cli bench --rows 4096 --queries 800 --shift-every 200

simulate:
	$(PY) -m llo.cli simulate --rows 4096 --queries 800 --shift-every 200 --act-every 50

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

docker:
	docker build -t learned-layout-optimizer:latest .
