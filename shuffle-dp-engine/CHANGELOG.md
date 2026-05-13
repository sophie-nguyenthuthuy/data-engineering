# Changelog

All notable changes to **shuffle-dp-engine** are documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] — 2026-05-13

### Added

- **Local randomisers** (`sdp.local.randomizers`)
  - `LocalConfig(eps0, domain_size)` with input validation.
  - `randomized_response(x, cfg, rng)` — k-ary RR.
  - `laplace_noise(value, sensitivity, eps, rng)` — pure ε-DP additive noise.
  - `gaussian_noise(value, sensitivity, eps, delta, rng)` — analytic (ε,δ)-DP.
- **Cryptographic shuffler** (`sdp.shuffler.mix`)
  - 3-stage onion mix network without external crypto dependencies (HMAC-XOR
    layered keystream over fixed-size 64-byte payloads).
  - `MixNode.fresh()`, `encrypt(record, nodes)`, `shuffle(records, nodes, rng)`.
  - Length-hiding via padding / truncation.
- **Privacy analyzer** (`sdp.analyzer.balle`, `sdp.analyzer.composition`)
  - Erlingsson-style closed-form amplification bound + inverse solver
    (`required_eps0_for_target`).
  - `composed_bound` with basic and DRV03 advanced composition.
- **Private queries** (`sdp.queries.histogram`)
  - `private_histogram` (debiased k-RR estimator, projection to simplex).
  - `private_mean` (Laplace mean with clipping to [lo, hi]).
- **Empirical validator** (`sdp.empirical`)
  - Membership-inference adversary advantage vs theoretical ε₀-DP bound.
- **CLI** (`shufflectl`)
  - `info`, `amplification`, `demo` subcommands.
- **Quality**
  - 29 pytest tests (28 fast + 1 empirical, 30 k trials).
  - mypy `--strict`, ruff lint + format clean.
  - Multi-stage Dockerfile, non-root runtime.
  - GitHub Actions CI matrix (Python 3.10 / 3.11 / 3.12) with Docker build step.

### Notes

- Composition validates `method` *before* short-circuiting on an empty bounds
  list so `composed_bound([], method="snake-oil")` raises `ValueError` rather
  than silently returning a zero-mechanism result.
- TriCS-style ill-formed inputs are rejected up-front:
  `LocalConfig(eps0=0)`, `LocalConfig(domain_size=1)`,
  `gaussian_noise(eps=1.5, ...)` (Gaussian needs ε ≤ 1), etc.
