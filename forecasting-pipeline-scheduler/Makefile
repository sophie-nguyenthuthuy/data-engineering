.PHONY: install dev test lint format type bench regret forecast clean docker

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
	$(PY) -m fps.cli bench --layers 5 --width 4 --workers 3

regret:
	$(PY) -m fps.cli regret --n-dags 200 --layers 5 --width 4 --workers 3

forecast:
	$(PY) -m fps.cli forecast --samples 2000

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

docker:
	docker build -t forecasting-pipeline-scheduler:latest .
