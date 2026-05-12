.PHONY: install test demo

install:
	pip install -r requirements.txt

test:
	PYTHONPATH=. pytest tests -v

demo:
	PYTHONPATH=. python examples/pagerank.py
