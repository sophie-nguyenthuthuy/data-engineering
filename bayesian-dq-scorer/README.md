# Bayesian Data Quality Scorer

Instead of deterministic pass/fail checks, every quality dimension gets a **posterior Beta distribution** updated with each new batch. Anomaly alerts fire when P(healthy) drops below a threshold. Uncertainty is visualized at every step.

## Dimensions

| Dimension | What it tracks | Success = |
|-----------|---------------|-----------|
| **Completeness** | Non-null rate | Row has no null values |
| **Freshness** | On-time arrival rate | Record timestamp within `max_age` |
| **Uniqueness** | Distinct row rate | Row is not a duplicate |

## How it works

Each dimension maintains a **Beta(α, β)** posterior — the conjugate prior for Binomial observations.

After observing `k` successes out of `n` rows in a batch:

```
α_new = α_old + k
β_new = β_old + (n - k)
```

**P(healthy)** is computed as the probability that the true quality rate exceeds a configurable `health_threshold`:

```
P(healthy | data) = P(rate > θ) = 1 − Beta_CDF(θ; α, β)
```

An **alert fires** when `P(healthy) < alert_threshold` (default 0.20), with a configurable cooldown to prevent alert storms.

## Quickstart

```bash
pip install -e ".[dev]"
python examples/demo.py
```

### Code example

```python
from bayesian_dq import BayesianDQScorer
from bayesian_dq.models import BatchObservation, DQDimension

scorer = BayesianDQScorer(
    health_thresholds={
        DQDimension.COMPLETENESS: 0.90,
        DQDimension.FRESHNESS:    0.90,
        DQDimension.UNIQUENESS:   0.95,
    },
    alert_thresholds={dim: 0.20 for dim in DQDimension},
)

# Score a batch
result = scorer.score_batch(
    batch_id="batch_001",
    observations={
        DQDimension.COMPLETENESS: BatchObservation(
            dimension=DQDimension.COMPLETENESS,
            successes=950,   # non-null rows
            total=1000,
            batch_id="batch_001",
        ),
        DQDimension.FRESHNESS: BatchObservation(
            dimension=DQDimension.FRESHNESS,
            successes=980,   # rows within SLA window
            total=1000,
            batch_id="batch_001",
        ),
        DQDimension.UNIQUENESS: BatchObservation(
            dimension=DQDimension.UNIQUENESS,
            successes=999,   # distinct rows
            total=1000,
            batch_id="batch_001",
        ),
    }
)

print(result.summary())
# {
#   "batch_id": "batch_001",
#   "dimensions": {
#     "completeness": {"p_healthy": 0.994, "posterior_mean": 0.951, "posterior_std": 0.007},
#     ...
#   },
#   "alerts_fired": 0
# }
```

### Using a DataFrame

```python
import pandas as pd
from bayesian_dq.dimensions import CompletenessScorer, UniquenessScorer

scorer_c = CompletenessScorer()
obs = CompletenessScorer.from_dataframe(df)
obs.batch_id = "batch_001"
scorer_c.observe(obs)
print(f"P(healthy): {scorer_c.p_healthy():.3f}")
```

### Custom alert handler (e.g. Slack)

```python
def slack_handler(alert):
    import requests
    requests.post(SLACK_WEBHOOK, json={"text": alert.message})

scorer = BayesianDQScorer(alert_handlers=[slack_handler])
```

## Visualization

```python
from bayesian_dq.visualization import DQVisualizer

viz = DQVisualizer(scorer.scorers)

# Posterior PDFs
fig, axes = viz.plot_posteriors()

# P(healthy) over time
fig, ax = viz.plot_p_healthy_over_time(scorer.history)

# Posterior mean + uncertainty bands over time
fig, axes = viz.plot_posterior_mean_over_time(scorer.history)

# Full 3-panel dashboard saved to PDF
viz.dashboard(scorer.history, output_path="dq_report.pdf")
```

## Architecture

```
bayesian_dq/
├── models.py        — PosteriorState, BatchObservation, BatchResult, AlertEvent
├── dimensions.py    — CompletenessScorer, FreshnessScorer, UniquenessScorer
├── alerts.py        — AlertManager (threshold + cooldown logic)
├── scorer.py        — BayesianDQScorer (top-level facade)
└── visualization.py — DQVisualizer (posterior PDFs, time series, dashboard)
```

## Prior selection guide

| Scenario | Recommended prior |
|---|---|
| No historical data | Beta(2, 2) — weakly informative, centered at 0.5 |
| Known good baseline ~95% | Beta(19, 1) — centered at 0.95, moderate confidence |
| Tight SLA, strong prior | Beta(95, 5) — very confident at 0.95 |
| High anomaly rate expected | Beta(1, 1) — uniform, let data speak |

## Running tests

```bash
pytest
```

## License

MIT
