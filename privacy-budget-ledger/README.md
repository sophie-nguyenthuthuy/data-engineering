# Privacy Budget Ledger

Advanced differential-privacy budget accounting for data query pipelines.

Extends basic ε-composition with **Rényi DP (RDP)** and **zero-concentrated DP (zCDP)** theorems that provide substantially tighter bounds. A smart query planner gates every query: it either accepts, **rewrites** (adjusts noise upward to fit the remaining budget), or **rejects** queries that would exceed the budget.

## Why tighter composition matters

Basic composition charges each query at face value: 100 queries at ε=0.1 costs ε=10. Under RDP/zCDP the true cost scales roughly as **O(√k · ε)** for Gaussian-mechanism workloads, so the same 100 queries may cost only ε≈1.5–2.0. This headroom lets analysts run more queries before exhausting their privacy budget.

| Accountant | Bound | Best for |
|---|---|---|
| Basic ε-composition | O(kε) | Any mechanism, pessimistic |
| **Rényi DP (RDP)** | O(√k·ε) for Gaussian | General workloads, multi-mechanism |
| **zCDP** | O(√k·ε) for Gaussian | Pure-Gaussian workloads, simpler math |

## Key features

- **Three parallel accountants** — basic ε, RDP, zCDP always tracked together
- **RDP per-α budget curves** — 16 Rényi orders from α=1.01 to α=10⁶; optimal α chosen automatically at query time
- **zCDP ρ ledger** — single scalar tracks Gaussian workload cost exactly
- **Balle et al. 2020 RDP→DP conversion** — tighter than the Mironov 2017 bound
- **Query planner** — `accept / rewrite / reject` with σ binary-search rewriting
- **Audit trail** — every query stores projected ε under all three accountants + savings vs basic composition
- **Composition summary** — per-(dataset, analyst) view of remaining headroom under each accountant

## Stack

| Layer | Technology |
|---|---|
| Backend | FastAPI + SQLAlchemy + SQLite |
| Accounting | NumPy + pure Python (no external DP libs) |
| Tests | pytest + httpx (56 tests) |

## Quick start

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Seed demo data
python seed_data.py

# Start API
uvicorn app.main:app --reload
# → http://localhost:8000/docs
```

## API overview

### Query planner

```
POST /planner/plan     Dry-run: evaluate a query without debiting budget
POST /planner/execute  Execute through the planner gateway (accept/rewrite/reject)
GET  /planner/logs     Audit log with planner decisions
```

**Plan response fields:**
```json
{
  "decision": "accept | rewrite | reject",
  "epsilon_requested": 0.3,
  "epsilon_feasible": 0.18,
  "sigma_feasible": 26.9,
  "projected_epsilon_basic": 3.3,
  "projected_epsilon_rdp": 1.12,
  "projected_epsilon_zcdp": 1.05,
  "savings_vs_basic": 2.25,
  "explanation": "Query rewritten: requested ε=0.3 exceeds budget ..."
}
```

### Budget & ledger

```
POST /budgets/                          Create allocation (grants ε budget to analyst)
GET  /budgets/{dataset_id}/{analyst_id}/summary   Composition summary with savings
GET  /budgets/{dataset_id}/{analyst_id}/ledger    Per-query RDP/zCDP audit trail
POST /budgets/{dataset_id}/{analyst_id}/reset     Reset counters (data owner only)
```

## Composition math

### Rényi DP

The Gaussian mechanism with sensitivity Δ and noise σ satisfies **(α, αΔ²/2σ²)-RDP** for all α > 1.

After k queries: ε_RDP(α) = Σᵢ αΔᵢ²/2σᵢ².

Converting to (ε,δ)-DP (Balle et al. 2020):
```
ε_dp = ε_rdp + log((α-1)/α) - (log(δ) + log(α)) / (α-1)
```
The ledger optimises over α to find the tightest bound.

### zCDP

The Gaussian mechanism satisfies **ρ-zCDP** with ρ = Δ²/2σ².

Composition is additive: k queries → ρ_total = Σ ρᵢ.

Converting to (ε,δ)-DP (Bun & Steinke 2016):
```
ε_dp = ρ + 2√(ρ · log(1/δ))
```

For 100 queries at ρ=0.005 each (σ≈10):
- Basic ε = 100 × ε_single ≈ 48.4
- zCDP ε ≈ 0.5 + 2√(0.5 × 11.5) ≈ **5.3**  ← 9× tighter

### Query rewriter

When a query would exceed budget at the requested ε, the planner binary-searches for the largest σ (most noise, least accuracy) that still fits. The analyst gets a result with higher noise instead of an outright rejection. Rewrites are capped at `max_sigma_factor × original_sigma` (default 200×) to avoid near-random results.

## Running tests

```bash
source .venv/bin/activate
pytest tests/ -v
# 56 passed
```

Test coverage includes:
- RDP formula correctness (Gaussian & Laplace)
- RDP→(ε,δ)-DP conversion accuracy
- zCDP formula, composition, and roundtrip
- Ledger commit/plan/rewrite state transitions
- Full API integration (accept, rewrite, reject, audit log)

## References

- Mironov (2017) — [Rényi Differential Privacy of the Gaussian Mechanism](https://arxiv.org/abs/1702.07476)
- Bun & Steinke (2016) — [Concentrated Differential Privacy](https://arxiv.org/abs/1605.02065)
- Balle, Barthe & Gaboardi (2020) — [Hypothesis Testing Interpretations and Renyi Differential Privacy](https://arxiv.org/abs/1905.09982)
- Wang, Balle & Kasiviswanathan (2019) — [Subsampled Rényi Differential Privacy](https://arxiv.org/abs/1808.00087)
