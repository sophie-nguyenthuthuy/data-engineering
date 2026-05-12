# shuffle-dp-engine

A **Shuffle Model** differential privacy system: records are anonymously shuffled by a cryptographic mixer before reaching the aggregator. The shuffle provides **privacy amplification** — local randomization with parameter ε₀ ends up with central-DP-equivalent ε ≈ O(ε₀ / √n). Implements Balle et al.'s optimal analyzer; cryptographic shuffler proved secure.

> **Status:** Design / spec phase. Extends [`differential-privacy-budget-manager`](../differential-privacy-budget-manager/) (central DP budget tracking) into the shuffle regime, where the privacy story is fundamentally stronger.

## Why shuffle DP

Three DP regimes:

| Regime | Privacy unit | ε for analyst utility | Trust assumption |
|---|---|---|---|
| **Central DP** | curator runs noisy aggregation | ε ≈ 0.5 | trust the curator |
| **Local DP** | each user randomizes locally | ε per user ≈ 4–8 | trust nobody |
| **Shuffle DP** | anonymous shuffler between users & curator | ε ≈ 0.5 + amplification | trust the shuffler (cryptographic) |

Shuffle DP is the practically interesting middle ground: same end-utility as central DP, no need to trust a curator.

## Architecture

```
┌────────┐   ┌────────┐         ┌────────┐
│ User 1 │   │ User 2 │   ...   │ User n │
│ ε₀-DP  │   │ ε₀-DP  │         │ ε₀-DP  │
└────┬───┘   └────┬───┘         └────┬───┘
     │ x₁'        │ x₂'              │ xₙ'   (locally randomized)
     │            │                  │
     ▼            ▼                  ▼
     ┌──────────────────────────────────┐
     │     Cryptographic Shuffler       │   ← unlinkability proof
     │       (mix network)              │
     └──────────────┬───────────────────┘
                    │ permuted batch
                    ▼
            ┌──────────────────┐
            │   Aggregator     │  ← Balle analyzer
            └──────────────────┘
                    │
              answer (ε-DP)
```

## The cryptographic shuffler

A 3-stage mix network. Each mix node:

1. Receives a batch of ciphertexts.
2. Decrypts one layer (onion encryption).
3. Permutes randomly.
4. Forwards.

After all 3 stages, the link between input position and output position is hidden from any non-colluding majority. Implementation: `pynacl` for sealed boxes, threshold honest-majority assumption.

**Proof obligation:** unlinkability holds against any adversary that controls ≤ 1 of the 3 mix nodes. This + local ε₀-randomization → Balle's amplification theorem.

## Components

| Module | Role |
|---|---|
| `src/local/` | Local randomizers: RR (randomized response), Gaussian, Laplace |
| `src/shuffler/mix.py` | 3-stage cryptographic mix network |
| `src/shuffler/proof.py` | Sanity-check that ciphertexts arrive unlinkable (Bayes-decision adversary) |
| `src/analyzer/balle.py` | Balle et al. optimal central-ε computation given (ε₀, n) |
| `src/analyzer/composition.py` | Multi-query composition with shuffle-amplified accounting |
| `src/queries/` | Histogram, mean, quantile under shuffle DP |
| `src/eval/` | Empirical privacy test (membership inference against shuffled output) |

## Privacy amplification

Balle et al. (2019) prove: for `n` users each ε₀-LDP, the output of the shuffler is ε-CDP with:

```
ε ≈ ε₀ · √(8 ln(2/δ) / n)     (when ε₀ is small)
```

So local ε₀=4 with n=10⁶ users → central ε ≈ 4 × √(8 × 0.7 / 10⁶) ≈ 0.01. Better than central with one user.

The engine takes (n, ε₀, target δ) and computes the achievable central ε analytically.

## End-to-end proof

Compose:

1. Local randomizer is ε₀-LDP (mechanism-specific, standard).
2. Shuffler is unlinkable against ≤ 1 corrupted mix (cryptographic).
3. Therefore output is ε-CDP with ε from Balle's analyzer.

Formal statement in `docs/proof.md`; mechanized check (where feasible) in Lean.

## Empirical validation

Membership-inference adversary: given an output and a target user's record, distinguish "user contributed" from "user did not". If our claimed ε holds, the adversary's advantage is bounded by ε.

Run 10⁶ trials; verify empirical advantage ≤ ε.

## References

- Cheu, Smith, Ullman, Zeber, Zhilyaev, "Distributed Differential Privacy via Shuffling" (EUROCRYPT 2019)
- Balle, Bell, Gascón, Nissim, "The Privacy Blanket of the Shuffle Model" (CRYPTO 2019)
- Erlingsson et al., "Amplification by Shuffling: From Local to Central Differential Privacy via Anonymity" (SODA 2019)

## Roadmap

- [ ] Local randomizers (RR, Gaussian, Laplace)
- [ ] Cryptographic mix-net (3-stage onion)
- [ ] Balle analyzer (Python + closed-form expressions)
- [ ] Histogram / mean / quantile queries
- [ ] Composition accountant
- [ ] Empirical membership-inference test
- [ ] Formal proof sketch (Lean for the analyzer, paper-style for unlinkability)
