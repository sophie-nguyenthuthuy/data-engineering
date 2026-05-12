.PHONY: install test lint format examples clean

install:
	pip install -e ".[dev]"

install-rocksdb:
	pip install -e ".[dev,rocksdb]"

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --tb=short --cov=ssb --cov-report=term-missing

lint:
	python -m py_compile src/ssb/**/*.py

format:
	python -m py_compile src/ssb/**/*.py && echo "Syntax OK"

example-word-count:
	python examples/word_count.py

example-windowed-join:
	python examples/windowed_join.py

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf dist/ build/
