.PHONY: help init up down restart logs ps nuke dbt-run dbt-test dbt-docs psql trigger keys

help:
	@echo "init      - copy .env.example -> .env and generate secrets"
	@echo "up        - build + start the stack"
	@echo "down      - stop the stack"
	@echo "restart   - down + up"
	@echo "logs      - tail all logs"
	@echo "ps        - list running services"
	@echo "nuke      - stop stack and wipe volumes (DESTRUCTIVE)"
	@echo "dbt-run   - run dbt models inside the airflow container"
	@echo "dbt-test  - run dbt tests inside the airflow container"
	@echo "dbt-docs  - generate and serve dbt docs on :8001"
	@echo "psql      - open psql shell to analytics db"
	@echo "trigger   - trigger the orders_pipeline DAG once"
	@echo "keys      - print freshly generated Fernet + secret keys"

init:
	@test -f .env || cp .env.example .env
	@echo ".env created. Run 'make keys' then paste values into .env."

keys:
	@python3 -c "from cryptography.fernet import Fernet; print('AIRFLOW_FERNET_KEY=' + Fernet.generate_key().decode())" 2>/dev/null || echo "pip install cryptography"
	@python3 -c "import secrets; print('AIRFLOW_SECRET_KEY=' + secrets.token_hex(32))"
	@python3 -c "import secrets; print('SUPERSET_SECRET_KEY=' + secrets.token_hex(32))"

up:
	docker compose up -d --build

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

nuke:
	docker compose down -v

dbt-run:
	docker compose exec airflow-scheduler dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

dbt-test:
	docker compose exec airflow-scheduler dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

dbt-docs:
	docker compose exec airflow-scheduler bash -c "dbt docs generate --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt && cd /opt/airflow/dbt && dbt docs serve --port 8001"

psql:
	docker compose exec postgres psql -U analytics -d analytics

trigger:
	docker compose exec airflow-scheduler airflow dags trigger orders_pipeline
