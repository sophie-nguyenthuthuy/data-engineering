.PHONY: install test demo tlc

install:
	pip install -r requirements.txt

test:
	PYTHONPATH=. pytest tests -v

demo:
	PYTHONPATH=. python examples/demo.py

# Run TLC (requires Java + TLA+ tools)
tlc:
	@which tlc > /dev/null 2>&1 || (echo "Install TLA+ tools first"; exit 1)
	tlc -config spec/MCPipeline.cfg spec/pipeline.tla
