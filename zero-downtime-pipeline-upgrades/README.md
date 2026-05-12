# Zero-Downtime Pipeline Version Upgrades

A deployment system for stateful data pipelines where **v2 runs in shadow mode alongside v1**, the system compares outputs for divergence, and traffic gradually shifts to v2 only when the output diff is below a configurable threshold.

Inspired by blue/green deployments but adapted for **stateful stream processors** where you can't simply swap instances — you need to validate that the new version produces equivalent outputs before trusting it with live traffic.

---

## Architecture

```
                    ┌─────────────────────────────────────────────┐
  Incoming          │           DeploymentOrchestrator            │
  Records  ───────► │                                             │
                    │   ┌──────────────────────────────────────┐  │
                    │   │           ShadowRunner               │  │
                    │   │                                      │  │
                    │   │  record ──► hash % 10000 ──► route   │  │
                    │   │                                      │  │
                    │   │  v2% = 0%    │  v2% = 50%  │ v2%=100%│  │
                    │   │  primary=v1  │  mixed      │ prim=v2  │  │
                    │   │  shadow=v2   │             │ shadow=v1│  │
                    │   │      │               │            │    │  │
                    │   │   primary ◄──────────┘            │    │  │
                    │   │   (sync)                          │    │  │
                    │   │                   shadow (async thread) │  │
                    │   └──────────────┬───────────────────┘  │  │
                    │                  │                       │  │
                    │         DivergenceTracker                │  │
                    │         (rolling window, per-field diff) │  │
                    │                  │                       │  │
                    │         TrafficShifter (background)      │  │
                    │         • ticks every N seconds          │  │
                    │         • advances split if div < thresh │  │
                    │         • rolls back if div > rollback   │  │
                    └─────────────────────────────────────────┘
```

### Components

| Component | File | Role |
|---|---|---|
| `BasePipeline` | `pipeline.py` | ABC every pipeline version implements |
| `DeploymentConfig` | `config.py` | All tunable parameters in one place |
| `DivergenceTracker` | `comparator.py` | Rolling window of per-field output diffs |
| `ShadowRunner` | `shadow_runner.py` | Dual-dispatch with deterministic hash routing |
| `TrafficShifter` | `traffic_shifter.py` | Background thread driving gradual migration |
| `DeploymentOrchestrator` | `orchestrator.py` | Single entry-point wiring everything together |

---

## Deployment Lifecycle

```
IDLE ──► SHADOW_ONLY ──► SHIFTING ──► PROMOTED ──► (complete)
              │               │
              └───────────────┴──► ROLLED_BACK
```

1. **Shadow only** — 100 % of traffic goes to v1; v2 runs in parallel, outputs compared, no user impact.
2. **Shifting** — every `traffic_shift_interval_sec` seconds, if divergence < threshold and enough samples have been collected, v2 traffic increases by `traffic_shift_step`.
3. **Promoted** — v2 at 100 %; v1 still runs as a safety shadow.
4. **Rolled back** — divergence exceeded `rollback_threshold` while v2 had live traffic; v1 restored instantly.

---

## Quick Start

```python
from pipeline_deployer import DeploymentConfig, DeploymentOrchestrator
from my_pipelines import WordCountV1, WordCountV2

config = DeploymentConfig(
    divergence_threshold=0.01,       # block promotion if >1% of records differ
    rollback_threshold=0.05,         # auto-rollback if >5% divergence with live traffic
    traffic_shift_step=0.10,         # advance 10% per step
    traffic_shift_interval_sec=60.0, # one step per minute
    min_samples_for_promotion=500,   # need 500 shadow records before first shift
)

orchestrator = DeploymentOrchestrator(
    v1=WordCountV1(),
    v2=WordCountV2(),
    config=config,
)

orchestrator.start()

for record in kafka_consumer:          # your live stream
    output = orchestrator.process(record)
    downstream.send(output)

final_status = orchestrator.complete()
print(final_status)
```

---

## Configuration Reference

| Parameter | Default | Description |
|---|---|---|
| `divergence_threshold` | `0.01` | Max allowed window divergence rate before promotion is blocked |
| `rollback_threshold` | `0.05` | Divergence rate that triggers automatic rollback once v2 carries live traffic |
| `initial_v2_percentage` | `0.0` | Starting v2 traffic fraction (0 = pure shadow) |
| `traffic_shift_step` | `0.10` | How much to increase v2 traffic per step |
| `traffic_shift_interval_sec` | `60.0` | Seconds between automatic shift attempts |
| `min_samples_for_promotion` | `100` | Minimum shadow comparisons before first shift |
| `comparison_window_size` | `1000` | Rolling window size for divergence calculation |
| `enable_auto_promotion` | `True` | Allow automatic traffic shifts |
| `enable_auto_rollback` | `True` | Allow automatic rollback on divergence spike |
| `shadow_log_path` | `None` | Optional path to write per-record divergence logs (JSONL) |

---

## Implementing a Pipeline

```python
from pipeline_deployer import BasePipeline
from typing import Any, Dict

class MyPipelineV2(BasePipeline):

    @property
    def version(self) -> str:
        return "v2.1.0"

    def setup(self) -> None:
        # open connections, load models, warm caches
        self.model = load_model("model-v2.pkl")

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        # must be thread-safe
        return {"prediction": self.model.predict(record["features"])}

    def teardown(self) -> None:
        self.model.close()

    def snapshot_state(self) -> Dict[str, Any]:
        # optional — for checkpointing before traffic shifts
        return {"processed": self._count}

    def restore_state(self, snapshot: Dict[str, Any]) -> None:
        self._count = snapshot["processed"]
```

---

## Divergence Comparison

The `DivergenceTracker` computes per-field divergence scores:

- **Numeric fields** — compared with configurable relative tolerance (`1e-6` default)
- **String / bool fields** — exact equality
- **Nested dicts** — recursive field-level comparison
- **Lists** — element-wise mean (length mismatch = 1.0)
- **Missing keys** — counted as fully divergent (1.0)
- **Ignored keys** — pass `ignore_keys={"timestamp", "request_id"}` to skip non-deterministic fields

The **window divergence rate** (fraction of recent records with any difference) drives promotion/rollback decisions.  The **mean divergence score** tracks magnitude.

---

## Manual Control

```python
orch.pause()                  # freeze automatic shifting
orch.resume()                 # unfreeze
orch.force_shift(0.50)        # immediately set v2 to 50%
orch.rollback()               # immediately return all traffic to v1
status = orch.status()        # point-in-time snapshot
```

---

## Running the Demo

```bash
git clone https://github.com/YOUR_USERNAME/zero-downtime-pipeline-upgrades
cd zero-downtime-pipeline-upgrades
pip install -e .
python -m examples.run_demo
```

The demo upgrades a whitespace-tokenising word counter (v1) to a regex-based one (v2). Records with punctuation cause divergence; you can watch the shifter hold at the current split until the rolling divergence rate drops.

---

## Running Tests

```bash
pip install pytest
pytest tests/ -v
```

---

## Project Structure

```
zero-downtime-pipeline-upgrades/
├── pipeline_deployer/
│   ├── __init__.py          # Public API re-exports
│   ├── config.py            # DeploymentConfig dataclass
│   ├── pipeline.py          # BasePipeline ABC
│   ├── comparator.py        # DivergenceTracker + dict_divergence
│   ├── shadow_runner.py     # Dual-dispatch shadow runner
│   ├── traffic_shifter.py   # Gradual traffic migration engine
│   └── orchestrator.py      # Top-level coordinator
├── examples/
│   ├── word_count_v1.py     # Whitespace tokeniser pipeline
│   ├── word_count_v2.py     # Regex tokeniser pipeline (improved)
│   └── run_demo.py          # End-to-end demo script
├── tests/
│   ├── test_comparator.py
│   ├── test_shadow_runner.py
│   ├── test_traffic_shifter.py
│   └── test_orchestrator.py
├── requirements.txt
├── setup.py
└── README.md
```

---

## License

MIT
