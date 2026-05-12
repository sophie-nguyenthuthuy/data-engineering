.PHONY: install test demo lint clean docker-up docker-down

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v

demo:
	python demo.py

lint:
	python -m py_compile src/kms/local_kms.py src/kms/client.py src/kms/key_registry.py \
		src/encryption/engine.py src/storage/s3_store.py \
		src/pipeline/ingest.py src/pipeline/rotation.py \
		src/rtbf/executor.py demo.py
	@echo "Syntax OK"

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf data/ .pytest_cache/

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down -v
