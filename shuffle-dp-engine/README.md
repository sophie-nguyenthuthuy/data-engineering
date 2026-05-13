# shuffle-dp-engine

A **shuffle-model differential privacy** system: each user locally randomises a
record with parameter ε₀, an anonymous cryptographic shuffler permutes the
batch, and the aggregator answers central-DP queries with parameter
ε ≪ ε₀. Implements local randomisers (k-ary RR, Laplace, Gaussian), a 3-stage
onion mix network, the Erlingsson/Balle privacy-amplification analyzer, basic
and Dwork–Rothblum–Vadhan advanced composition, private histograms / means,
and an empirical membership-inference validator.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why shuffle DP

Three DP regimes:

| Regime         | Privacy unit                          | ε for analyst utility       | Trust assumption          |
| -------------- | ------------------------------------- | --------------------------- | ------------------------- |
| **Central**    | curator runs noisy aggregation        | ε ≈ 0.5                     | trust the curator         |
| **Local**      | each user randomises locally          | ε per user ≈ 4–8            | trust nobody              |
| **Shuffle**    | anonymous shuffler between user / agg | ε ≈ 0.5 + amplification     | trust the shuffler crypto |

Shuffle DP gives central-DP utility without trusting a curator — only an
honest-majority mix network.

## Architecture

```
┌────────┐   ┌────────┐         ┌────────┐
│ User 1 │   │ User 2 │   ...   │ User n │
│ ε₀-LDP │   │ ε₀-LDP │         │ ε₀-LDP │
└────┬───┘   └────┬───┘         └────┬───┘
     │ x₁'        │ x₂'              │ xₙ'
     ▼            ▼                  ▼
     ┌──────────────────────────────────┐
     │   Cryptographic shuffler         │
     │   (3-stage onion mix network)    │
     └──────────────┬───────────────────┘
                    │ permuted batch
                    ▼
            ┌──────────────────┐
            │  Aggregator      │   ← Balle / Erlingsson analyzer
            └──────────────────┘
                    │
              answer (ε-CDP)
```

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. Single runtime dependency: `numpy`.

## CLI

```bash
shufflectl info                                       # show package metadata
shufflectl amplification --eps0 2.0 --n 1_000_000 \
                         --delta 1e-6                 # central-ε bound
shufflectl demo --n 5000 --eps0 2.0                   # end-to-end histogram run
```

## Library

```python
from sdp.local.randomizers import LocalConfig, randomized_response
from sdp.shuffler.mix import MixNode, shuffle
from sdp.analyzer.balle import shuffle_amplification, required_eps0_for_target
from sdp.analyzer.composition import composed_bound
from sdp.queries.histogram import private_histogram, private_mean

# 1. Local randomisation
cfg = LocalConfig(eps0=2.0, domain_size=4)
y = randomized_response(x=2, cfg=cfg)

# 2. Cryptographic shuffler (3-stage onion mix)
nodes = [MixNode.fresh() for _ in range(3)]
shuffled = shuffle([b"vote-0", b"vote-1", b"vote-2"], nodes)

# 3. Balle / Erlingsson amplification
bound = shuffle_amplification(eps0=2.0, n=1_000_000, delta=1e-6)
print(bound.eps_central)            # ≈ 0.04
print(bound.amplification)          # ε₀ / ε_central

# 4. Inverse solver
eps0 = required_eps0_for_target(eps_target=0.5, n=10_000, delta=1e-6)

# 5. Composition (basic and DRV03 advanced)
total = composed_bound([bound] * 100, method="advanced", target_delta=1e-5)

# 6. Private histogram + mean (debiased k-RR / Laplace)
pmf  = private_histogram(samples=[0, 1, 1, 2, 3, 3, 3], cfg=cfg)
mean = private_mean(values=[..], lo=0.0, hi=100.0, eps=1.0)
```

## Components

| Module                       | Role                                                                |
| ---------------------------- | ------------------------------------------------------------------- |
| `sdp.local.randomizers`      | k-ary randomized response, Laplace, Gaussian                        |
| `sdp.shuffler.mix`           | 3-stage onion mix (HMAC-XOR layered encryption + per-hop shuffle)   |
| `sdp.analyzer.balle`         | Erlingsson-style closed-form ε ≈ 8 ε₀ √(eᵉ⁰ log(4/δ)/n) + inverse   |
| `sdp.analyzer.composition`   | Sequential composition: basic (Σε,Σδ) + DRV03 advanced              |
| `sdp.queries.histogram`      | Debiased private histogram + Laplace private mean                   |
| `sdp.empirical`              | Membership-inference adversary advantage vs theoretical bound       |
| `sdp.cli`                    | `shufflectl` entry point                                            |

## Privacy amplification

For `n` users each ε₀-LDP, the shuffle output is ε-CDP with (Erlingsson et
al., SODA 2019):

```
ε ≤ min(ε₀,  8 · ε₀ · √( eᵉ⁰ · log(4/δ) / n ))
δ' = δ
```

So ε₀ = 4 with n = 10⁶ → central ε ≈ 0.41.  Drop ε₀ to 2 and get ε ≈ 0.04.

`required_eps0_for_target` runs binary search over this bound to find the
largest ε₀ that still meets a desired central ε under a given (n, δ).

## Composition

`composed_bound(bounds, method=...)` accepts a list of `ShuffleBound`s:

- **basic** — (ε_total, δ_total) = (Σ ε_i, Σ δ_i).
- **advanced** (DRV03) — requires `target_delta` δ' > 0; returns
  ε_total ≤ √(2k ln(1/δ')) · ε_max + k · ε_max · (eᵉᵐᵃˣ − 1)
  and δ_total = k · δ_max + δ'.

The advanced bound beats basic at k ≳ 30; both are exposed for accounting.

## Cryptographic shuffler

`sdp.shuffler.mix` implements a 3-stage mix network without external crypto
dependencies (HMAC-XOR layered encryption keyed per mix node + per-record
nonces). Each node:

1. Receives a batch of onions.
2. Peels one layer (HMAC keystream XOR over the fixed-size payload).
3. Shuffles the batch with a fresh permutation.
4. Forwards the inner batch to the next node.

The honest-majority assumption (≥ 1 of 3 nodes uncorrupted) is the standard
unlinkability premise behind Balle / Erlingsson amplification.  Payloads are
padded / truncated to `PAYLOAD_SIZE = 64` bytes so length is not a side channel.

## Empirical validation

`empirical_advantage_rr` runs a membership-inference adversary against the
k-ary randomised-response mechanism. For `n_trials` independent draws it
estimates the adversary's advantage and compares it to the theoretical
ε₀-DP bound (eᵉ⁰ − 1) / (eᵉ⁰ + k − 1).

Marked `@pytest.mark.empirical` — the suite verifies empirical ≤ theoretical
for ε₀ ∈ {1, 2, 4}.

## Quality

```bash
make lint        # ruff   (E, W, F, I, B, UP, SIM, RUF, TCH)
make format      # ruff format
make type        # mypy --strict
make test        # 28 fast tests
make test-empirical   # +1 empirical-DP test (30k trials)
make bench       # CLI amplification + demo
make docker      # production image
```

- 29 tests, 0 failing
- mypy `--strict`, ruff clean
- Python 3.10 / 3.11 / 3.12 CI matrix
- Distroless-style slim Docker image, non-root user

## References

- Erlingsson, Feldman, Mironov, Raghunathan, Talwar, Thakurta. *Amplification
  by Shuffling: From Local to Central DP via Anonymity.* SODA 2019.
- Balle, Bell, Gascón, Nissim. *The Privacy Blanket of the Shuffle Model.*
  CRYPTO 2019.
- Cheu, Smith, Ullman, Zeber, Zhilyaev. *Distributed Differential Privacy via
  Shuffling.* EUROCRYPT 2019.
- Dwork, Rothblum, Vadhan. *Boosting and Differential Privacy.* FOCS 2010.

## License

MIT — see [LICENSE](LICENSE).
