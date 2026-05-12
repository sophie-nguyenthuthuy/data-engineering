# Changelog

## [0.1.0] — Initial public release

### Added

- `TDigest`: adaptive nearest-bin quantile sketch with q-aware capacity cap
- `LognormalFitter`: online MLE via Welford's algorithm + Acklam inverse-normal
- `POTFitter`: Peaks-Over-Threshold + Generalised Pareto (method of moments)
- `PerKeyDelayEstimator`: 3-source quantile (t-digest / lognormal / EVT),
  monotone-non-decreasing `safe_delay` clamp, per-key rate EMA
- `WatermarkAdvancer`: `W = min over active keys (rate ≥ λ_min) of
  (t - safe_delay(k))`, monotone-clamped
- `MonotonicityChecker`: runtime invariant for W and per-key safe_delay,
  strict-mode raises `MonotonicityViolation`
- `TumblingWindowState` + `CorrectionStream`: late-record back-correction
- 4 synthetic workload generators (exponential, lognormal, Pareto, bimodal)
- TLA+ spec (`spec/monotonicity.tla`) formalising the safety invariant
- 42 tests across 8 modules
- Throughput + calibration benchmarks
- CLI: `pwmctl bench {throughput, calibration}`, `pwmctl info`
- GitHub Actions CI on Python 3.10/3.11/3.12
- Mypy strict, ruff lint, Dockerfile, docker-compose

### Bugs found while building

1. Initial t-digest implementation returned median for p99 query (centroid
   merge thresholds were too permissive); replaced with adaptive nearest-bin
   algorithm with q-aware caps.
2. Bimodal workload's default parameters weren't actually bimodal — the
   "heavy" mode wasn't separated enough from "light". Test now uses
   `μ_heavy=4.0, σ_heavy=0.5` which gives p99 > 10 × p50.

### Limitations

- Single-machine (no distributed time service)
- No backpressure-aware λ_min tuning
- EVT uses method-of-moments (faster but less accurate than MLE)
- No multi-region clock synchronisation
