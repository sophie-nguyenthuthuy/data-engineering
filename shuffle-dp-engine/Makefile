.PHONY: install dev test test-empirical lint format type bench demo clean docker

PY ?= python3

install:
	$(PY) -m pip install -e .

dev:
	$(PY) -m pip install -e ".[dev]"

test:
	$(PY) -m pytest tests -q

test-empirical:
	$(PY) -m pytest tests -q -m empirical

lint:
	$(PY) -m ruff check src tests

format:
	$(PY) -m ruff format src tests

type:
	$(PY) -m mypy src

bench:
	$(PY) -m sdp.cli amplification --eps0 2.0 --n 1000000 --delta 1e-6
	$(PY) -m sdp.cli demo --n 5000 --eps0 2.0

demo:
	$(PY) -m sdp.cli demo --n 5000 --eps0 2.0

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

docker:
	docker build -t shuffle-dp-engine:latest .
