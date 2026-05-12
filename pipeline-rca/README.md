# pipeline-rca

**Automated Root Cause Attribution for Data Pipeline Failures**

A meta-observability layer that, when a downstream metric degrades, automatically traces it back to a specific upstream table, column, or recent schema change using **Interrupted Time Series (ITS) causal impact analysis**. Generates a human-readable incident report with ranked root causes, effect sizes, and recommended remediation steps.

---

## How It Works

```
Downstream metric degrades
        │
        ▼
 MetricMonitor          ← rolling z-score + threshold check
        │  degradation detected
        ▼
 LineageTracer          ← metric → upstream tables/columns
        │  candidate tables
        ▼
 SchemaStore            ← recent schema changes, pipeline failures, late data
        │  candidate interventions
        ▼
 ITSAnalyzer            ← segmented OLS regression, counterfactual, p-value
        │  ranked CausalEstimates
        ▼
 RootCauseAttributor    ← orchestrates + ranks
        │
        ▼
 ReportGenerator        ← Jinja2 Markdown incident report
```

### Causal Impact Analysis (ITS)

For each candidate upstream change at time *t₀*, the analyzer fits a **segmented linear regression**:

```
y = β₀ + β₁·t + β₂·D + β₃·D·(t − t₀)
```

Where `D = 1` after the intervention. The pre-period coefficients are used to project a **counterfactual** (what would have happened without the change), and the causal effect is `mean(observed) − mean(counterfactual)`. A two-tailed t-test on β₂ gives the p-value.

---

## Installation

```bash
# Core
pip install -e .

# With BigQuery support
pip install -e ".[bigquery]"

# With Snowflake support
pip install -e ".[snowflake]"

# Development
pip install -e ".[dev]"
```

**Requires Python 3.10+**

---

## Quick Start

### 1. CLI demo (no warehouse needed)

```bash
pipeline-rca demo --drop-pct 0.35 --save
```

This generates a synthetic 17-point time series with a 35% drop, seeds a fake schema change, runs the full RCA pipeline, prints a summary table, and writes a Markdown incident report to `reports/`.

### 2. Programmatic API

```python
from pipeline_rca.attribution.root_cause import RootCauseAttributor
from pipeline_rca.lineage.tracer import LineageTracer
from pipeline_rca.models import MetricDegradation, DegradationKind
from pipeline_rca.monitors.metric_monitor import MetricMonitor
from pipeline_rca.monitors.schema_monitor import SchemaStore
from pipeline_rca.reporting.generator import ReportGenerator

# Build components
tracer = LineageTracer()
tracer.register_metric("revenue_usd", upstream_tables=["transactions", "products"])

store = SchemaStore("rca_store.sqlite")  # persists across runs

attributor = RootCauseAttributor(tracer=tracer, schema_store=store)
gen = ReportGenerator(output_dir="reports")

# When a metric degrades:
report = attributor.attribute(degradation)  # MetricDegradation from MetricMonitor
path = gen.save(report)
print(f"Report: {path}")
```

### 3. Config-driven workflow

```bash
cp config/config.example.yaml config/config.yaml
# Edit config.yaml with your warehouse details and metric queries
# Provide series JSON files: {"timestamp": "...", "value": ...} per point
pipeline-rca run config/config.yaml
```

---

## Schema Change Tracking

Record changes as they happen:

```python
# Schema diff (call after every table DDL)
store.snapshot_columns("transactions", connector.fetch_columns("transactions"))

# Pipeline events
from pipeline_rca.models import ChangeCategoryKind
store.record_pipeline_event("transactions", ChangeCategoryKind.PIPELINE_FAILURE, {"job": "daily_load"})
store.record_pipeline_event("user_events", ChangeCategoryKind.LATE_DATA, {"delay_hours": 5})
```

---

## Example Incident Report

```markdown
# Incident Report — A3F7B2C1

**Generated:** 2024-03-15 09:42 UTC
**Metric:** `daily_active_users`
**Degradation type:** DROP
**Detected at:** 2024-03-15 09:40 UTC

## Summary
The ITS causal analysis attributes the 35.0% drop with **34.8% relative effect**
(p=0.0031) to a **column_dropped** event on `user_events` (column `session_id`)
at 2024-03-14 20:00 UTC.

## Top Root Causes
| # | Candidate                              | Effect | p-value | Significant? |
|---|----------------------------------------|--------|---------|--------------|
| 1 | `user_events.session_id [column_dropped]` | 34.8% | 0.0031 | ✓ |
| 2 | `sessions [late_data]`                 | 12.1% | 0.1840  | — |

## Recommended Actions
- **[CRITICAL]** Column `session_id` was dropped from `user_events`.
  Restore it or update all downstream queries that reference it.
```

---

## Running Tests

```bash
pip install -e ".[dev]"
pytest
```

---

## Project Structure

```
src/pipeline_rca/
├── models.py                  # Shared dataclasses (MetricPoint, CausalEstimate, …)
├── cli.py                     # Click CLI (demo, run)
├── monitors/
│   ├── metric_monitor.py      # Rolling z-score + threshold degradation detection
│   └── schema_monitor.py      # SQLite-backed schema snapshot & diff
├── lineage/
│   └── tracer.py              # Metric → table → column lineage graph
├── analysis/
│   └── causal_impact.py       # Interrupted Time Series OLS regression
├── attribution/
│   └── root_cause.py          # Orchestration: detection → lineage → ITS → report
├── reporting/
│   ├── generator.py           # Jinja2 Markdown renderer
│   └── templates/
│       └── incident_report.md.j2
└── connectors/
    ├── base.py                # Abstract connector interface
    └── bigquery.py            # BigQuery implementation
```

---

## Configuration Reference

See [`config/config.example.yaml`](config/config.example.yaml) for all options.

| Key | Default | Description |
|-----|---------|-------------|
| `metrics[].degradation_threshold` | `0.15` | Minimum relative change to flag |
| `metrics[].baseline_window_days` | `14` | Days used as baseline |
| `causal_analysis.pre_period_days` | `14` | ITS pre-period length |
| `causal_analysis.confidence_level` | `0.95` | CI / significance level |
| `causal_analysis.min_effect_size` | `0.05` | Minimum effect to report |

---

## License

MIT
