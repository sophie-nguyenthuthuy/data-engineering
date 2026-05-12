SHELL := /usr/bin/env bash
.ONESHELL:
.DEFAULT_GOAL := help

COMPOSE := docker compose
ENV_FILE := .env
PROJECT := pipeline

# Load .env if present so target env-interpolation works in commands.
ifneq (,$(wildcard $(ENV_FILE)))
include $(ENV_FILE)
export
endif

.PHONY: help
help:  ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: env
env:  ## Create .env from .env.example if missing
	@test -f $(ENV_FILE) && echo ".env already exists" || cp .env.example $(ENV_FILE)

.PHONY: up
up: env  ## Start the full stack
	$(COMPOSE) --project-name $(PROJECT) up -d --build

.PHONY: down
down:  ## Stop the stack, keep volumes
	$(COMPOSE) --project-name $(PROJECT) down

.PHONY: clean
clean:  ## Stop the stack and remove volumes (destructive)
	$(COMPOSE) --project-name $(PROJECT) down -v --remove-orphans

.PHONY: ps
ps:  ## Show running services
	$(COMPOSE) --project-name $(PROJECT) ps

.PHONY: logs
logs:  ## Tail logs (SERVICE=producer make logs)
	$(COMPOSE) --project-name $(PROJECT) logs -f --tail=200 $(SERVICE)

.PHONY: register
register:  ## Register the ClickHouse sink connector with Kafka Connect
	set -a; source $(ENV_FILE); set +a; \
	./scripts/register-connector.sh

.PHONY: connectors
connectors:  ## List active Kafka Connect connectors and their status
	@curl -fsS $${CONNECT_URL_EXTERNAL:-http://localhost:8083}/connectors | python3 -m json.tool

.PHONY: ch-count
ch-count:  ## Print the row count in the ClickHouse events table
	@curl -fsS -u "$${CLICKHOUSE_USER}:$${CLICKHOUSE_PASSWORD}" \
	  "http://localhost:$${CLICKHOUSE_PORT_HTTP:-8123}/?database=$${CLICKHOUSE_DB}" \
	  --data-binary "SELECT count() FROM user_interactions"

.PHONY: smoke
smoke: up register  ## Bring everything up and register the sink connector
	@echo
	@echo "Dashboard:  http://localhost:$${DASHBOARD_PORT:-8501}"
	@echo "API:        http://localhost:$${API_PORT:-8000}/healthz"
	@echo "Connect:    http://localhost:8083/connectors"
	@echo "Dagster:    http://localhost:$${DAGSTER_PORT:-3000}"
	@echo "MinIO:      http://localhost:$${MINIO_CONSOLE_PORT:-9001}"
	@echo "Grafana:    http://localhost:$${GRAFANA_PORT:-3001}    (anonymous viewer)"
	@echo "Prometheus: http://localhost:$${PROMETHEUS_PORT:-9090}"
	@echo "Tempo:      http://localhost:$${TEMPO_HTTP_PORT:-3200}"

# ----------------------------------------------------------------------------
# Phase 2: secure stack (SASL_SSL + Docker secrets + basic auth).
# ----------------------------------------------------------------------------

.PHONY: bootstrap
bootstrap:  ## Generate TLS certs + SCRAM passwords under secrets/ (idempotent)
	./scripts/bootstrap-secrets.sh

# Build a secure env file from the template + generated secrets, so SCRAM
# users/passwords are injected without being committed to .env.example.
.PHONY: env-secure
env-secure: env bootstrap  ## Render .env.secure with real SCRAM passwords
	@set -e; \
	USERS="$$(grep '^KAFKA_CLIENT_USERS=' .env.example | cut -d= -f2-)"; \
	if [ -z "$$USERS" ]; then echo "KAFKA_CLIENT_USERS missing from .env.example"; exit 1; fi; \
	PWDS=""; \
	for u in $$(echo $$USERS | tr ',' ' '); do \
	  f=secrets/clients/$${u}_password; \
	  [ -s "$$f" ] || { echo "missing $$f — run make bootstrap"; exit 1; }; \
	  PWDS="$$PWDS,$$(cat $$f)"; \
	done; \
	PWDS="$${PWDS#,}"; \
	ADMIN_PW="$$(cat secrets/clients/admin_password)"; \
	( sed -e '/^KAFKA_CLIENT_PASSWORDS=/d' \
	      -e '/^KAFKA_INTER_BROKER_PASSWORD=/d' \
	      -e 's|^KAFKA_CLIENT_USERS=.*|KAFKA_CLIENT_USERS='"$$USERS"'|' \
	      .env; \
	  echo "KAFKA_CLIENT_PASSWORDS=$$PWDS"; \
	  echo "KAFKA_INTER_BROKER_PASSWORD=$$ADMIN_PW"; \
	) > .env.secure
	@echo "wrote .env.secure"

.PHONY: up-secure
up-secure: env-secure  ## Start the SASL_SSL stack
	$(COMPOSE) --project-name $(PROJECT) --env-file .env.secure -f docker-compose.secure.yml up -d --build

.PHONY: down-secure
down-secure:  ## Stop the secure stack, keep volumes
	$(COMPOSE) --project-name $(PROJECT) -f docker-compose.secure.yml down

.PHONY: clean-secure
clean-secure:  ## Stop secure stack and remove volumes
	$(COMPOSE) --project-name $(PROJECT) -f docker-compose.secure.yml down -v --remove-orphans

.PHONY: register-secure
register-secure:  ## Register the sink connector against the secure stack
	./scripts/register-connector-secure.sh

.PHONY: smoke-secure
smoke-secure: up-secure register-secure  ## Bring up secure stack + register sink
	@echo
	@echo "Dashboard:  http://localhost:$${DASHBOARD_PORT:-8501}"
	@echo "API:        http://localhost:$${API_PORT:-8000}/healthz"
	@echo "Connect:    http://localhost:8083/connectors  (SR needs basic auth: admin:<admin_password>)"
	@echo "Dagster:    http://localhost:$${DAGSTER_PORT:-3000}"
	@echo "MinIO:      http://localhost:$${MINIO_CONSOLE_PORT:-9001}  (root cred in secrets/clients/minio_root_*)"
	@echo "Grafana:    http://localhost:$${GRAFANA_PORT:-3001}  (admin / cat secrets/clients/grafana_admin_password)"
	@echo "Prometheus: not exposed on secure stack — shell in for debugging"

# ----------------------------------------------------------------------------
# Phase 3: reliability operations.
# ----------------------------------------------------------------------------

.PHONY: dlq-peek
dlq-peek:  ## Dump up to N=10 DLQ messages with error headers (STACK=secure|dev, N=count)
	@N="$${N:-10}" STACK="$${STACK:-secure}" ./scripts/inspect-dlq.sh "$${N:-10}"

.PHONY: acls-list
acls-list:  ## List all Kafka ACLs (secure stack)
	@ADMIN_PW="$$(cat secrets/clients/admin_password)"; \
	docker exec kafka bash -c "cat >/tmp/c.properties <<EOF\n\
security.protocol=SASL_SSL\n\
sasl.mechanism=SCRAM-SHA-512\n\
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"$$ADMIN_PW\";\n\
ssl.truststore.type=PEM\n\
ssl.truststore.location=/opt/bitnami/kafka/config/certs/kafka.truststore.pem\n\
EOF\n\
/opt/bitnami/kafka/bin/kafka-acls.sh --bootstrap-server kafka:9092 --command-config /tmp/c.properties --list"

.PHONY: ch-dedup
ch-dedup:  ## OPTIMIZE TABLE FINAL to collapse ReplacingMergeTree duplicates
	@curl -fsS -u "pipeline:$$(cat secrets/clients/clickhouse_pipeline_password 2>/dev/null || echo $${CLICKHOUSE_PASSWORD})" \
	  "http://localhost:$${CLICKHOUSE_PORT_HTTP:-8123}/?database=$${CLICKHOUSE_DB:-events}" \
	  --data-binary "OPTIMIZE TABLE user_interactions FINAL DEDUPLICATE"
	@echo "OK"

# ----------------------------------------------------------------------------
# Phase 4: orchestration operations.
# ----------------------------------------------------------------------------

.PHONY: dagster-url
dagster-url:  ## Print the Dagster webserver URL
	@echo "Dagster: http://localhost:$${DAGSTER_PORT:-3000}"

.PHONY: minio-url
minio-url:  ## Print the MinIO console URL and creds location
	@echo "MinIO console: http://localhost:$${MINIO_CONSOLE_PORT:-9001}"
	@echo "Creds (secure stack): secrets/clients/minio_root_user, secrets/clients/minio_root_password"
	@echo "Creds (dev stack):    .env MINIO_ROOT_USER / MINIO_ROOT_PASSWORD"

.PHONY: analysis-run
analysis-run:  ## Kick the hourly_analysis_job against the latest partition (dev stack)
	@curl -fsS -X POST "http://localhost:$${DAGSTER_PORT:-3000}/graphql" \
	  -H 'Content-Type: application/json' \
	  -d '{"query":"mutation { launchPipelineExecution(executionParams: { selector: { repositoryLocationName: \"orchestrator\", repositoryName: \"__repository__\", pipelineName: \"hourly_analysis_job\" }, runConfigData: {}, mode: \"default\" }) { __typename } }"}'

# ----------------------------------------------------------------------------
# Phase 5: observability operations.
# ----------------------------------------------------------------------------

.PHONY: grafana-url
grafana-url:  ## Print the Grafana URL
	@echo "Grafana: http://localhost:$${GRAFANA_PORT:-3001}"
	@echo "Dev:    anonymous viewer (edit via admin / \$${GRAFANA_ADMIN_PASSWORD:-admin})"
	@echo "Secure: admin / \$$(cat secrets/clients/grafana_admin_password)"

.PHONY: otel-health
otel-health:  ## Check the OTel Collector is reachable + scraping its own metrics
	@curl -fsS "http://localhost:$${PROMETHEUS_PORT:-9090}/api/v1/query?query=up{job=\"otel-collector\"}" \
	  | python3 -m json.tool

.PHONY: traces
traces:  ## Show latest trace IDs for a service (SERVICE=api|producer|dagster)
	@curl -fsS "http://localhost:$${TEMPO_HTTP_PORT:-3200}/api/search?tags=service.name%3D$${SERVICE:-api}&limit=10" \
	  | python3 -m json.tool

.PHONY: test-producer
test-producer:  ## Run producer unit tests
	cd services/producer && pip install -e '.[dev]' >/dev/null && pytest -q

.PHONY: test-api
test-api:  ## Run API unit tests
	cd services/api && pip install -e '.[dev]' >/dev/null && pytest -q

.PHONY: test-orchestrator
test-orchestrator:  ## Run orchestrator unit tests (no Spark required)
	cd services/orchestrator && pip install -e '.[dev]' >/dev/null && pytest -q

.PHONY: test
test: test-producer test-api test-orchestrator  ## Run all unit tests

.PHONY: lint
lint:  ## Ruff + mypy across services
	cd services/producer && ruff check src tests && mypy src
	cd services/api && ruff check src tests && mypy src
	cd services/orchestrator && ruff check src tests

.PHONY: fmt
fmt:  ## Format with ruff
	cd services/producer && ruff format src tests
	cd services/api && ruff format src tests
	cd services/orchestrator && ruff format src tests
