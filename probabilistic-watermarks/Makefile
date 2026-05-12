.PHONY: install test test-quick lint format typecheck bench clean tlc

install:
	pip install -e ".[dev]"

test:
	pytest -q

test-quick:
	pytest -q -m "not calibration and not slow"

lint:
	ruff check src tests benchmarks

format:
	ruff format src tests benchmarks
	ruff check --fix src tests benchmarks

typecheck:
	mypy src/pwm

bench:
	python -m benchmarks.bench_throughput
	@echo
	python -m benchmarks.bench_calibration

tlc:
	@which tlc > /dev/null 2>&1 || (echo "Install TLA+ tools first"; exit 1)
	tlc -config spec/MCMono.cfg spec/monotonicity.tla

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache dist build *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
