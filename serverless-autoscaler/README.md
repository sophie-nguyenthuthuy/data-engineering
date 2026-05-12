# Serverless Data Pipeline Autoscaler with Predictive Warming

A control plane that predicts Spark/Flink job resource needs using ARIMA on historical run metrics, pre-allocates workers before scheduled jobs start, and continuously adjusts mid-run via the Kubernetes HPA API. Tracks cold-start cost savings.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Control Plane (Python)                        │
│                                                                  │
│  ┌──────────────┐   forecast   ┌─────────────────┐              │
│  │  Scheduler   │─────────────▶│  ARIMA Predictor│              │
│  │  (croniter)  │              │  (statsmodels)  │              │
│  └──────┬───────┘              └────────┬────────┘              │
│         │ prewarm/adjust                │ ResourceForecast       │
│         ▼                               ▼                        │
│  ┌──────────────┐             ┌──────────────────┐              │
│  │  HPA Client  │             │  Metrics Store   │              │
│  │  (k8s API)   │             │  (SQLAlchemy/DB) │              │
│  └──────┬───────┘             └────────┬─────────┘              │
│         │                              │                         │
│         ▼                              ▼                         │
│  ┌──────────────┐             ┌──────────────────┐              │
│  │  Kubernetes  │             │  Cost Tracker    │              │
│  │  HPA Objects │             │  (savings log)   │              │
│  └──────────────┘             └──────────────────┘              │
│                                                                  │
│  Prometheus metrics exposed on :9090/metrics                     │
└─────────────────────────────────────────────────────────────────┘
```

## How It Works

### 1. Predictive Warming (ARIMA)

The `ARIMAPredictor` fits a **SARIMA(2,1,2)(1,1,1,12)** model on the last N completed runs of each job. It forecasts `peak_workers` for the next scheduled execution, applies a configurable safety factor (default 1.15×), and derives CPU/memory from historical per-worker ratios.

Falls back to a **p95 percentile estimate** when history is too sparse or the model fails to converge.

### 2. Pre-allocation

The `PredictiveScheduler` polls the job registry every 30 seconds. When a job is within `PREWARM_LEAD_TIME_SECONDS` (default 5 min) of its cron-scheduled start time, it:

1. Fetches the ARIMA forecast.
2. Patches the job's HPA `minReplicas` → `predicted_peak_workers`.
3. Sets `maxReplicas` → `confidence_upper` to allow headroom.

Workers spin up immediately, eliminating the cold-start window.

### 3. Mid-Run Adjustment

While a job runs, the scheduler monitors its HPA's `desiredReplicas`. If demand approaches the current ceiling (>90% of `maxReplicas`), it expands `maxReplicas` by 30% to prevent throttling.

### 4. Post-Job Restoration

On job completion, HPA is restored to default bounds (`minReplicas=1, maxReplicas=10`) to avoid paying for idle pre-warmed workers.

### 5. Cost Savings Tracking

Every run where cold-start was avoided generates a `ColdStartSavingRecord`:

```
avoided_cost  = workers × cold_start_seconds × worker_$/s
prewarm_cost  = workers × idle_fraction × lead_time × worker_$/s
net_saving    = avoided_cost - prewarm_cost
```

Cumulative savings are queryable via `CostTracker.report()` and exposed as a Prometheus gauge.

## Metrics

| Metric | Type | Description |
|---|---|---|
| `autoscaler_prewarm_total` | Counter | Prewarm ops triggered, by job |
| `autoscaler_scaling_actions_total` | Counter | HPA patches applied, by job+reason |
| `autoscaler_active_jobs` | Gauge | Currently tracked running jobs |
| `autoscaler_predicted_workers` | Gauge | ARIMA forecast for next run |
| `autoscaler_job_duration_seconds` | Histogram | Observed wall-clock duration |
| `autoscaler_net_savings_usd_total` | Gauge | Cumulative net USD savings |

## Quick Start

### Local dev (stub k8s client)

```bash
# Install deps
pip install -r requirements-dev.txt

# Seed historical data
PYTHONPATH=src python scripts/simulate_jobs.py --jobs 3 --runs-per-job 30 --db sqlite:///dev.db

# Run the control plane
METRICS_DB_URL=sqlite:///dev.db \
PREWARM_LEAD_TIME_SECONDS=300 \
LOG_LEVEL=DEBUG \
PYTHONPATH=src python -m autoscaler.main
```

### Run tests

```bash
PYTHONPATH=src pytest tests/ -v --cov=autoscaler
```

### Deploy to Kubernetes

```bash
kubectl create namespace autoscaler
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/configmap.yaml  # edit jobs.yaml first
kubectl apply -f k8s/deployment.yaml
```

## Configuration

All settings are environment variables:

| Variable | Default | Description |
|---|---|---|
| `AUTOSCALER_NAMESPACE` | `default` | Kubernetes namespace |
| `METRICS_DB_URL` | `sqlite:///autoscaler_metrics.db` | SQLAlchemy DB URL |
| `PREWARM_LEAD_TIME_SECONDS` | `300` | How far ahead to prewarm |
| `PROMETHEUS_PORT` | `9090` | Metrics HTTP port |
| `LOG_LEVEL` | `INFO` | Python log level |

Advanced ARIMA and cost parameters can be tuned via `AppConfig` in code.

## Job Registry Format

Edit `k8s/configmap.yaml` to register your jobs:

```yaml
jobs:
  - job_id: my-spark-job
    name: "My Spark ETL"
    type: spark                    # spark | flink
    hpa_target: my-spark-hpa      # HPA object name in k8s
    cron: "0 3 * * *"             # standard cron expression
    namespace: spark-jobs
    tags:
      team: data-platform
```

## Project Structure

```
src/autoscaler/
├── config.py         — typed config dataclasses, env-var loading
├── models.py         — domain models (JobRun, ResourceForecast, …)
├── predictor.py      — ARIMA + percentile-fallback forecaster
├── scheduler.py      — main control loop, prewarm + mid-run logic
├── hpa_client.py     — Kubernetes HPA API wrapper
├── metrics_store.py  — SQLAlchemy persistence layer
├── cost_tracker.py   — cold-start savings accounting
├── telemetry.py      — Prometheus metrics
└── main.py           — entry point
```

## License

MIT
