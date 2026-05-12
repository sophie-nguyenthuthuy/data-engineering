.PHONY: install ingest api dev docker clean

install:
	pip install -r requirements.txt

ingest:
	python -m src.ingest

api:
	uvicorn src.api:app --reload --port 8000

dev:
	@echo "run 'make ingest' and 'make api' in two terminals"

docker:
	docker compose up --build

clean:
	rm -rf data/*.db data/*.db-journal __pycache__ src/__pycache__
