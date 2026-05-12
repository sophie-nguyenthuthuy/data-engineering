# probabilistic-watermarks

Watermark protocol for stream processing that provides **probabilistic lateness guarantees** — `P(late arrival | watermark advanced) < 0.1%` — by learning per-key arrival-delay distributions instead of using a fixed `N`-second grace period.

> **Status:** Design / spec phase. Extends ideas from `out-of-order-stream-processor` (per-key watermarks) with formal probabilistic bounds.

## Why

Classic watermarks ("wait N seconds, then close the window") force a trade-off: large N = high latency, small N = late records dropped or backfilled. Neither captures the actual delay distribution, which is usually heavy-tailed and key-dependent.

This project replaces the fixed delay with a learned per-key model. The watermark advances when the model predicts `P(record with timestamp ≤ t arrives later) < δ`.

## Architecture

```
Event stream ──┬─▶ Per-key delay sketch (t-digest of arrival - event-time)
               │
               ├─▶ Online distribution fit (lognormal / Weibull / EVT tail)
               │       │
               │       └─▶ Per-key quantile predictor q(1-δ)
               │
               └─▶ Watermark advancer:
                       advance_to(t)  iff  for all keys with rate > λ_min,
                                           t - now ≥ q_key(1-δ)

After watermark:
  ──▶ Window close path (closes & emits results)
  ──▶ Correction stream  (out-of-band: late records → delta updates to closed windows)
```

## Components

| Module | Role |
|---|---|
| `src/sketch/tdigest.py` | Per-key delay sketch (bounded memory) |
| `src/fit/` | Online MLE for lognormal / Weibull + EVT for tail |
| `src/watermark/` | Per-key watermark store + global watermark advance |
| `src/proof/monotone.tla` | TLA+ proof that watermark advancement is monotone under arbitrary network delays |
| `src/correction/` | Late-record handler: deltas applied to downstream consumers |
| `src/eval/` | Empirical lateness measurement on real & synthetic streams |

## Monotonicity proof obligation

Given:
- `δ` is the configured tolerance (e.g., 10⁻³)
- `q_k(p)` is the p-quantile estimate of key `k`'s delay distribution
- `μ_k` is the current measured ingestion rate of key `k`

Claim: the watermark function `W(t) = min_k {t - q_k(1-δ) : μ_k > λ_min}` is monotone non-decreasing under arbitrary network delay.

Proof sketch (full proof in `src/proof/monotone.tla`):
- New observations can only *update* `q_k` upward (heavy tail discovery) or leave it unchanged within a window.
- A new key whose `q_k` is large only restricts `W` further; existing keys' `q_k` updates only delay `W`.
- Therefore `W` never moves backward.

The watermark is **conservative** — it never advances past a point where the model believes more than `δ` of records will arrive late.

## Correction stream

When a record arrives after its key's watermark:
1. Identify the closed window it belongs to.
2. Compute the delta: `f(new_record + old_window) - f(old_window)`.
3. Emit to a separate `corrections` topic; downstream consumers idempotently apply.

Downstream consumers must support delta-update operations (associative & commutative).

## Benchmarks (targets)

Workloads:
- **Synthetic:** keys with exponential / lognormal / Pareto delay; verify empirical lateness < δ.
- **Real:** NYC taxi (10⁻³ tolerance) + GitHub event stream.

| Metric | Fixed watermark (60s) | Probabilistic (δ=10⁻³) |
|---|---|---|
| p99 window latency | 60 s | target 5–10 s |
| Actual late-record rate | 0.1% | target ≤ 0.1% |
| Correction stream rate | 0 | target ≤ δ |

## References

- Akidau et al., "The Dataflow Model" (VLDB 2015) — watermark semantics
- Awad et al., "Watermarks in Stream Processing Systems: Semantics and Comparative Analysis" (VLDB 2021)
- Coles & Davison, *An Introduction to Statistical Modeling of Extreme Values* (2001) — EVT for tail

## Roadmap

- [ ] Per-key t-digest delay sketch
- [ ] Online lognormal / Weibull fit
- [ ] EVT tail estimator
- [ ] Watermark store + advance protocol
- [ ] TLA+ monotonicity proof + TLC check
- [ ] Correction stream + downstream delta apply
- [ ] Empirical lateness measurement harness
