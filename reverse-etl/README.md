# Reverse ETL

Move analytics results from your data warehouse back into operational tools — Slack, HubSpot, Salesforce, and email — on a schedule or via webhook trigger.

```
analytics DB / warehouse
        │
        ▼ SQL query
  ┌─────────────┐
  │  Extract    │  Postgres · BigQuery · Snowflake
  └──────┬──────┘
         │
         ▼ field mapping + transforms
  ┌─────────────┐
  │  Transform  │  rename, upper/lower/str/int casts
  └──────┬──────┘
         │
         ▼ upsert / post / send
  ┌─────────────┐
  │  Load       │  Slack · HubSpot · Salesforce · Email
  └─────────────┘
         ▲
  scheduler (APScheduler cron / interval)
  webhook API  (FastAPI)
```

## Quick start

```bash
cp .env.example .env   # fill in your credentials

pip install -r requirements.txt

# list configured pipelines
python main.py list

# run one pipeline immediately
python main.py run churned_users_slack

# run all enabled pipelines now
python main.py run-all

# start the scheduler (blocking)
python main.py schedule

# start the webhook API (also runs the scheduler)
python main.py serve --with-scheduler
```

## Architecture

| Layer | File(s) |
|---|---|
| CLI entry point | `main.py`, `src/main.py` |
| Pipeline model | `src/models.py` |
| Pipeline runner | `src/pipeline.py` |
| Source adapters | `src/sources/` |
| Destination adapters | `src/destinations/` |
| Field mapper | `src/transforms/mapper.py` |
| Scheduler | `src/scheduler/scheduler.py` |
| Webhook API | `src/triggers/webhook.py` |
| Pipeline configs | `pipelines/*.yaml` |

## Pipeline config format

```yaml
name: my_pipeline
description: "What this does"
enabled: true

source:
  type: postgres          # postgres | bigquery | snowflake
  query: |
    SELECT id, email, plan FROM churned_users
    WHERE churned_at >= NOW() - INTERVAL '24 hours';

destination:
  type: slack             # slack | hubspot | salesforce | email
  params:
    channel: "#alerts"
    message_template: "{email} churned — {plan} plan"
  field_mappings:         # optional renames + transforms
    - source: email
      destination: email
      transform: lower

schedule:
  type: cron
  cron: "0 9 * * 1-5"    # every weekday at 09:00 UTC

trigger:                  # optional webhook trigger
  type: webhook
  path: /trigger/my_pipeline
```

## Sources

| Type | Env vars needed |
|---|---|
| `postgres` | `POSTGRES_*` |
| `bigquery` | `BIGQUERY_PROJECT`, `BIGQUERY_CREDENTIALS_FILE` |
| `snowflake` | `SNOWFLAKE_*` |

## Destinations

| Type | Env vars needed | Key params |
|---|---|---|
| `slack` | `SLACK_BOT_TOKEN` | `channel`, `message_template`, `batch_summary` |
| `email` | `SENDGRID_API_KEY`, `EMAIL_FROM` | `to`, `subject`, `body_template` |
| `hubspot` | `HUBSPOT_ACCESS_TOKEN` | `object_type`, `id_property` |
| `salesforce` | `SALESFORCE_*` | `object_name`, `operation`, `external_id_field` |

## Webhook API

```bash
# Trigger a pipeline via HTTP
curl -X POST http://localhost:8000/trigger/churned_users_slack

# With signature verification
curl -X POST http://localhost:8000/trigger/churned_users_slack \
  -H "X-Hub-Signature-256: sha256=<hmac>"

# List pipelines
curl http://localhost:8000/pipelines

# Health check
curl http://localhost:8000/health
```

## Field transforms

Available built-in transforms: `upper`, `lower`, `str`, `int`, `float`, `bool`, `strip`.

## Tests

```bash
pytest -v
```

## Adding a new source / destination

1. Create a class in `src/sources/` or `src/destinations/` that extends `BaseSource` / `BaseDestination`
2. Register it in the corresponding `__init__.py` registry dict
3. Add example pipeline YAML in `pipelines/`

## Example pipelines

| File | What it does |
|---|---|
| `churned_users_slack.yaml` | Daily Slack alert for churned users |
| `high_value_leads_hubspot.yaml` | Hourly HubSpot contact upsert for high-ARR trials |
| `weekly_revenue_email.yaml` | Monday morning MRR/ARR email to finance |
| `opportunity_salesforce_sync.yaml` | Daily Salesforce Opportunity upsert for closed-won deals |
