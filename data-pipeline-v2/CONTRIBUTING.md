# Contributing

## Local dev loop

```bash
make init && make keys       # paste keys into .env
make up                      # build + start
make logs                    # follow
make trigger                 # kick the DAG
```

## Changing dbt models

1. Edit SQL under `dbt/models/`.
2. `make dbt-run` to rebuild.
3. `make dbt-test` to run schema + singular tests.
4. `make dbt-docs` to eyeball lineage at http://localhost:8001.

## Adding a new source

1. Extend the mock API (`mock_api/app.py`) with the new endpoint.
2. Add a landing table to `postgres/init/02_schemas.sql`.
3. Add the source to `dbt/models/bronze/_sources.yml`.
4. Create `bronze_*`, `silver_*`, `gold_*` models and tests.
5. Add the source to `SOURCES` in `airflow/dags/orders_pipeline.py`.

## CI

GitHub Actions validates:
- `docker compose config`
- `dbt parse` + `dbt compile` against a throwaway Postgres
- `ruff` on Python sources

Run locally before pushing:

```bash
docker compose config -q
ruff check airflow/dags mock_api
(cd dbt && dbt parse)
```
