include .env
export

DATE  ?= $(shell date -u -v-1d +%Y-%m-%d 2>/dev/null || date -u -d 'yesterday' +%Y-%m-%d)
START ?= $(DATE)
END   ?= $(DATE)

.PHONY: help deps tf-init tf-apply tf-destroy up down ps logs ingest-batch stream-start stream-stop dbt-deps dbt-run dbt-test spark-rollup bruin-run kestra-trigger

help:
	@echo "Targets:"
	@echo "  deps              install python deps for ingestion + streaming + analytics"
	@echo "  tf-init / tf-apply / tf-destroy"
	@echo "  up / down / ps / logs"
	@echo "  ingest-batch DATE=YYYY-MM-DD          run dlt batch ingest for one day"
	@echo "  ingest-batch START=... END=...        run dlt for a date range"
	@echo "  stream-start / stream-stop            kafka producer + consumer"
	@echo "  dbt-deps / dbt-run / dbt-test"
	@echo "  spark-rollup START=... END=..."
	@echo "  bruin-run                             run everything via bruin"
	@echo "  kestra-trigger FLOW=gh_backfill       kick off a kestra flow"

# ---- python env ----
deps:
	pip install -r ingestion/requirements.txt -r streaming/requirements.txt
	pip install "dbt-bigquery>=1.7,<1.9"

# ---- terraform ----
tf-init:
	cd infra/terraform && terraform init

tf-apply:
	cd infra/terraform && terraform apply -var "project_id=$(GCP_PROJECT_ID)" -var "region=$(GCP_REGION)"

tf-destroy:
	cd infra/terraform && terraform destroy -var "project_id=$(GCP_PROJECT_ID)" -var "region=$(GCP_REGION)"

# ---- local stack ----
up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

# ---- ingestion ----
ingest-batch:
	python ingestion/dlt_batch.py --start $(START) --end $(END)

# ---- streaming ----
stream-start:
	@mkdir -p .run
	python streaming/producer.py & echo $$! > .run/producer.pid
	python streaming/consumer.py & echo $$! > .run/consumer.pid
	@echo "producer + consumer started"

stream-stop:
	-@kill `cat .run/producer.pid 2>/dev/null` 2>/dev/null && rm -f .run/producer.pid
	-@kill `cat .run/consumer.pid 2>/dev/null` 2>/dev/null && rm -f .run/consumer.pid

# ---- dbt ----
dbt-deps:
	cd analytics && dbt deps --profiles-dir .

dbt-run:
	cd analytics && dbt build --profiles-dir . --vars '{"start_date":"$(START)","end_date":"$(END)"}'

dbt-test:
	cd analytics && dbt test --profiles-dir .

# ---- spark ----
spark-rollup:
	docker compose exec spark-master \
	  spark-submit --master spark://spark-master:7077 \
	    --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.0 \
	    /opt/jobs/daily_rollups.py --start $(START) --end $(END)

# ---- bruin ----
bruin-run:
	cd platform && bruin run

# ---- kestra ----
kestra-trigger:
	curl -X POST "$(KESTRA_URL)/api/v1/executions/gh_pipeline/$(FLOW)" \
	  -H "Content-Type: application/json" \
	  -d '{"start_date":"$(START)","end_date":"$(END)"}'
