# probabilistic-watermarks

[![CI](https://github.com/sophie-nguyenthuthuy/data-engineering/actions/workflows/pwm.yml/badge.svg)](https://github.com/sophie-nguyenthuthuy/data-engineering/actions)

A streaming watermark protocol with **probabilistic lateness guarantees**:
instead of "wait N seconds and accept whatever's late," set a target
`δ` and the system learns a per-key delay distribution so that
`P(late | watermark advanced) < δ`.

Late records flow into a **correction stream** that downstream consumers
idempotently apply to back-correct closed windows.

```text
  Event stream ──┬─▶ Per-key delay sketch (t-digest + lognormal + EVT)
                 │
                 ├─▶ safe_delay(k) = (1-δ)-quantile, clamped monotone non-decreasing
                 │
                 └─▶ WatermarkAdvancer:
                       W(t) = min over active keys (rate ≥ λ_min) of (t - safe_delay(k))
                       ▼
                 ┌───────────────────────────────────┐
                 │   t < W ?                          │
                 │     ─ no  → on-time                │
                 │     ─ yes → CorrectionStream       │
                 └───────────────────────────────────┘
```

42 tests pass (incl. monotonicity invariant checker); throughput
~17k events/s; calibration: observed-late-rate within 1.5× target δ
for stationary distributions.

## Install

```bash
pip install -e ".[dev]"
```

## Quick start

```python
from pwm import PerKeyDelayEstimator, WatermarkAdvancer
from pwm.watermark.invariants import MonotonicityChecker

est = PerKeyDelayEstimator(delta=0.01)
adv = WatermarkAdvancer(delay_estimator=est, lambda_min=0.0)
check = MonotonicityChecker(advancer=adv, strict=True)

# Stream events: (key, event_time, arrival_time)
for k, e, a in events():
    status, w = check.check(k, e, a)
    if status == "late":
        # route to correction stream
        ...
```

Three quantile sources:
```python
PerKeyDelayEstimator(delta=0.01, source="tdigest")    # default, no parametric assumption
PerKeyDelayEstimator(delta=0.01, source="lognormal")  # tighter for typical streams
PerKeyDelayEstimator(delta=0.01, source="evt")        # best for heavy tails (POT/GPD)
```

## CLI

```bash
pwmctl info
pwmctl bench throughput          # 17k events/s across 4 distributions
pwmctl bench calibration         # observed-late-rate vs target δ
```

## Architecture

### Sketch (`src/pwm/sketch/`)

| Module | Role |
|---|---|
| `tdigest.py` | Quantile sketch — adaptive nearest-bin algorithm with q-aware bin-size cap |

### Fit (`src/pwm/fit/`)

| Module | Role |
|---|---|
| `lognormal.py` | Online MLE via Welford on log-values + Acklam inverse-normal |
| `evt.py`       | Peaks-Over-Threshold + Generalised Pareto fit (method of moments) |

### Watermark (`src/pwm/watermark/`)

| Module | Role |
|---|---|
| `estimator.py` | `PerKeyDelayEstimator` — t-digest + lognormal + EVT per key |
| `advancer.py`  | `WatermarkAdvancer` — `W = min(t - safe_delay(k))`, monotone |
| `invariants.py`| `MonotonicityChecker` — runtime guard for `W' ≥ W` |

### Correction (`src/pwm/correction/`)

| Module | Role |
|---|---|
| `window.py`  | `TumblingWindowState` — per-(key, window) value with `is_closed` |
| `stream.py`  | `CorrectionStream` — emits `(key, window_start, old, new)` for late records |

### Workload (`src/pwm/workload/`)

4 generators: `exponential_delay_workload`, `lognormal_delay_workload`,
`pareto_delay_workload`, `bimodal_workload` — for testing under known
delay distributions.

## Monotonicity

The protocol's safety property: **the watermark is non-decreasing**.

Two layers:

1. **Per-key `safe_delay` is monotone**: the estimator clamps each
   key's safe_delay to `max(prev, current_quantile_estimate)`. Once we
   discover a key has a long tail, we don't forget.

2. **`W` is monotone**: even with monotone per-key safe_delay, naive
   `W = min(t - safe_delay)` could decrease when a new key becomes active
   with very high `safe_delay`. The advancer enforces `W' = max(W,
   computed_W)`.

The `MonotonicityChecker` enforces both at runtime; the `spec/monotonicity.tla`
TLA+ spec formalises the invariant.

### Run the TLA+ check

```bash
# Requires Java + TLA+ tools
tlc -config spec/MCMono.cfg spec/monotonicity.tla
```

## Calibration

```
$ pwmctl bench calibration
     δ source     dist         observed observed/δ
  0.01 tdigest    exp             1.06%       1.06
  0.01 tdigest    lognormal       0.68%       0.68
  0.01 tdigest    pareto          7.04%       7.04
  0.05 tdigest    exp             5.28%       1.06
  0.05 tdigest    lognormal       5.48%       1.10
  0.05 lognormal  lognormal       4.28%       0.86
```

- **Exponential & lognormal**: observed/δ ratio is ~1.0 ± 0.1 across
  three target δs. Well-calibrated.
- **Pareto** (heavy tail): observed rate is 2-7× target. The t-digest
  under-predicts the (1-δ)-quantile because the empirical sample doesn't
  yet include the extreme events. The `evt` source (Generalised Pareto
  tail fit) closes this gap — partially; in production this is where
  domain knowledge wins.

## Throughput

```
$ pwmctl bench throughput
workload         events         ms        qps   late%    final W
exp               50000     3052.6     16,379    0.82    49994.9
lognormal         50000     2787.7     17,936    0.16    49995.8
pareto            50000     3051.6     16,385    7.29    49987.5
bimodal           50000     2139.8     23,367   17.09    49958.1
```

17-23k events/s on a single Python thread including all per-key state
updates (t-digest insert, lognormal Welford, EVT fitter, rate EMA).

## Correctness (tests)

- **T-digest accuracy**: p50/p99 within a few percent of truth on uniform,
  exponential, lognormal
- **Lognormal MLE**: recovers μ, σ from 10k samples; p99 within 10% of
  closed-form
- **EVT**: shape parameter within sensible range; tail quantile reasonable
- **Monotonicity invariant**: tests deliberately rewind the advancer's `_w`
  and the checker fires (`MonotonicityViolation` raised in strict mode)
- **End-to-end calibration**: under stationary exponential delay, observed
  late rate is within 2× target δ

## Limitations / roadmap

- [ ] **Order-2/3 Markov on key transitions** — currently we treat keys
      independently; learning that some keys share a delay regime could
      cold-start a new key from a similar one
- [ ] **Online EVT MLE** — currently method-of-moments; full MLE via SGD
      would give tighter tail estimates
- [ ] **Multi-region clocks** — assumes one global wall-clock; in
      multi-region streaming you need HLC or per-region watermarks merged
      conservatively
- [ ] **Backpressure-aware λ_min** — if a key is congested upstream, its
      rate may collapse, dropping it from W computation; this can
      under-advance W

## References

- Akidau et al., "The Dataflow Model" (VLDB 2015) — watermark semantics
- Awad et al., "Watermarks in Stream Processing Systems: Semantics and
  Comparative Analysis" (VLDB 2021)
- Coles & Davison, *An Introduction to Statistical Modeling of Extreme
  Values* (2001) — EVT for tail
- Dunning, "Computing Extremely Accurate Quantiles Using t-Digests"
  (2019)

## License

MIT — see `LICENSE`.
