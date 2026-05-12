"""
Unified composition ledger.

Maintains three parallel views of privacy cost for a (dataset, analyst) pair:
  1. Basic ε-composition   — pessimistic O(k·ε) upper bound
  2. RDP composition       — tight O(√k·ε) for Gaussian; stored as per-α moments
  3. zCDP composition      — equally tight for Gaussian; single ρ scalar

The ledger is the authoritative source of truth for how much budget has been
consumed. The query planner reads from it to decide accept/rewrite/reject.

In-memory state only (the DB models persist the same numbers via LedgerEntry
rows, loaded when the service starts or the planner reconstructs history).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .rdp import (
    ALPHA_ORDERS,
    best_rdp_to_dp,
    compose_rdp,
    rdp_curve_for_gaussian,
    rdp_curve_for_laplace,
    projected_dp_epsilon,
    current_dp_epsilon,
)
from .zcdp import (
    compose_zcdp,
    zcdp_to_dp,
    zcdp_gaussian,
    zcdp_laplace_approx,
    rho_for_dp_target,
    sigma_for_rho,
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class QueryCost:
    """Privacy cost of a single query under all three accountants."""
    # Basic ε-composition
    epsilon_basic: float
    delta_basic: float = 0.0

    # RDP: list of (α, ε(α)) pairs
    rdp_curve: List[Tuple[float, float]] = field(default_factory=list)

    # zCDP
    rho: float = 0.0

    # Metadata
    mechanism: str = "gaussian"
    sensitivity: float = 1.0
    sigma: Optional[float] = None  # Gaussian only


@dataclass
class BudgetAllocationSpec:
    """What the data owner has granted."""
    total_epsilon: float          # (ε,δ)-DP budget cap
    total_delta: float = 1e-5
    # Optional explicit ρ cap (zCDP); derived from (ε,δ) if not set
    total_rho: Optional[float] = None
    exhaustion_policy: str = "block"  # "block" | "inject_noise"
    # Rewrites that require sigma > max_sigma_factor × original_sigma are rejected
    # (a σ=10000 rewrite produces near-useless noise anyway)
    max_sigma_factor: float = 200.0


@dataclass
class CompositionState:
    """
    Live accounting state.  All three accountants are kept in sync
    every time a query is committed.
    """
    # Basic
    consumed_epsilon_basic: float = 0.0
    consumed_delta_basic: float = 0.0

    # RDP: accumulated per-α ε (initialised to zeros over ALPHA_ORDERS)
    accumulated_rdp: List[Tuple[float, float]] = field(
        default_factory=lambda: [(a, 0.0) for a in ALPHA_ORDERS]
    )

    # zCDP
    consumed_rho: float = 0.0

    # History of individual query costs (for audit / reconstruction)
    history: List[QueryCost] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------

class CompositionLedger:
    """
    Core ledger for one (dataset, analyst) budget allocation.

    Usage:
        ledger = CompositionLedger(spec)
        plan   = ledger.plan_query(cost)     # dry-run
        ledger.commit_query(cost)            # debit budget
    """

    def __init__(self, spec: BudgetAllocationSpec):
        self.spec = spec
        self.state = CompositionState()

        # If total_rho not specified, derive a generous zCDP cap from (ε,δ).
        if spec.total_rho is None:
            spec.total_rho = rho_for_dp_target(spec.total_epsilon, spec.total_delta)

    # ------------------------------------------------------------------
    # Current budget consumption (read-only views)
    # ------------------------------------------------------------------

    def consumed_dp_epsilon_basic(self) -> float:
        return self.state.consumed_epsilon_basic

    def consumed_dp_epsilon_rdp(self) -> float:
        """Tightest (ε,δ)-DP implied by the accumulated RDP curve."""
        if all(e == 0.0 for _, e in self.state.accumulated_rdp):
            return 0.0
        return current_dp_epsilon(self.state.accumulated_rdp, self.spec.total_delta)

    def consumed_dp_epsilon_zcdp(self) -> float:
        if self.state.consumed_rho == 0:
            return 0.0
        return zcdp_to_dp(self.state.consumed_rho, self.spec.total_delta)

    def remaining_budget_rdp(self) -> float:
        return max(0.0, self.spec.total_epsilon - self.consumed_dp_epsilon_rdp())

    def remaining_budget_zcdp(self) -> float:
        return max(0.0, self.spec.total_epsilon - self.consumed_dp_epsilon_zcdp())

    def remaining_rho(self) -> float:
        return max(0.0, self.spec.total_rho - self.state.consumed_rho)

    def savings_vs_basic(self) -> float:
        """
        Budget headroom gained by using RDP instead of basic composition.
        Positive means RDP is tighter/better for the analyst.
        """
        return self.consumed_dp_epsilon_basic() - self.consumed_dp_epsilon_rdp()

    # ------------------------------------------------------------------
    # Plan (dry-run — does NOT mutate state)
    # ------------------------------------------------------------------

    def plan_query(self, cost: QueryCost) -> "QueryPlan":
        """
        Evaluate whether a query with the given cost can be admitted.
        Returns a QueryPlan describing the decision and any rewrite.
        """
        # Project epsilon after adding this query under each accountant
        new_basic = self.state.consumed_epsilon_basic + cost.epsilon_basic
        new_rdp_eps = projected_dp_epsilon(
            self.state.accumulated_rdp, cost.rdp_curve, self.spec.total_delta
        )
        new_rho = self.state.consumed_rho + cost.rho
        new_zcdp_eps = zcdp_to_dp(new_rho, self.spec.total_delta) if new_rho > 0 else 0.0

        # Tightest projected ε
        projected_tight = min(new_rdp_eps, new_zcdp_eps) if cost.rho > 0 else new_rdp_eps

        fits = projected_tight <= self.spec.total_epsilon

        # ── Accept ──────────────────────────────────────────────────
        if fits:
            return QueryPlan(
                decision="accept",
                epsilon_requested=cost.epsilon_basic,
                epsilon_feasible=cost.epsilon_basic,
                sigma_feasible=cost.sigma,
                projected_epsilon_basic=new_basic,
                projected_epsilon_rdp=new_rdp_eps,
                projected_epsilon_zcdp=new_zcdp_eps,
                savings_vs_basic=new_basic - projected_tight,
                explanation=(
                    f"Query accepted. Projected budget after query: "
                    f"ε_basic={new_basic:.4f}, ε_RDP={new_rdp_eps:.4f}, "
                    f"ε_zCDP={new_zcdp_eps:.4f} (limit {self.spec.total_epsilon:.4f}). "
                    f"RDP/zCDP saves {new_basic - projected_tight:.4f}ε vs basic composition."
                ),
            )

        # ── Rewrite: find max feasible ε ─────────────────────────────
        rewrite = self._find_rewrite(cost)
        if rewrite is not None:
            eps_rw, sigma_rw, rho_rw = rewrite
            rw_rdp = projected_dp_epsilon(
                self.state.accumulated_rdp,
                _make_rdp_curve(cost.mechanism, cost.sensitivity, sigma_rw),
                self.spec.total_delta,
            )
            rw_rho_total = self.state.consumed_rho + rho_rw
            rw_zcdp = zcdp_to_dp(rw_rho_total, self.spec.total_delta) if rw_rho_total > 0 else 0.0
            rw_basic = self.state.consumed_epsilon_basic + eps_rw
            return QueryPlan(
                decision="rewrite",
                epsilon_requested=cost.epsilon_basic,
                epsilon_feasible=eps_rw,
                sigma_feasible=sigma_rw,
                projected_epsilon_basic=rw_basic,
                projected_epsilon_rdp=rw_rdp,
                projected_epsilon_zcdp=rw_zcdp,
                savings_vs_basic=rw_basic - min(rw_rdp, rw_zcdp),
                explanation=(
                    f"Query rewritten: requested ε={cost.epsilon_basic:.4f} exceeds budget "
                    f"under tight accounting. Feasible ε={eps_rw:.4f} with σ={sigma_rw:.4f} "
                    f"(more noise, lower accuracy). Projected ε_RDP={rw_rdp:.4f} ≤ "
                    f"limit {self.spec.total_epsilon:.4f}."
                ),
            )

        # ── Reject ───────────────────────────────────────────────────
        return QueryPlan(
            decision="reject",
            epsilon_requested=cost.epsilon_basic,
            epsilon_feasible=None,
            sigma_feasible=None,
            projected_epsilon_basic=new_basic,
            projected_epsilon_rdp=new_rdp_eps,
            projected_epsilon_zcdp=new_zcdp_eps,
            savings_vs_basic=None,
            explanation=(
                f"Query rejected. Even maximum noise cannot fit within remaining budget "
                f"(ε_RDP={new_rdp_eps:.4f} > limit {self.spec.total_epsilon:.4f}). "
                f"Remaining ε_RDP headroom: {self.remaining_budget_rdp():.4f}."
            ),
        )

    # ------------------------------------------------------------------
    # Commit (mutates state — call only after plan says accept/rewrite)
    # ------------------------------------------------------------------

    def commit_query(self, cost: QueryCost) -> None:
        self.state.consumed_epsilon_basic += cost.epsilon_basic
        self.state.consumed_delta_basic += cost.delta_basic
        self.state.consumed_rho += cost.rho

        composed = compose_rdp([self.state.accumulated_rdp, cost.rdp_curve])
        self.state.accumulated_rdp = composed
        self.state.history.append(cost)

    # ------------------------------------------------------------------
    # Reconstruct from history (e.g. after DB load)
    # ------------------------------------------------------------------

    def rebuild_from_history(self, costs: List[QueryCost]) -> None:
        self.state = CompositionState()
        for c in costs:
            self.commit_query(c)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _find_rewrite(
        self, cost: QueryCost, steps: int = 64
    ) -> Optional[Tuple[float, float, float]]:
        """
        Binary-search for the largest σ (or equivalently smallest ε) such that
        adding the rewritten query keeps the tightest projected ε within budget.

        Returns (eps_feasible, sigma_feasible, rho_feasible) or None.
        """
        if cost.mechanism not in ("gaussian",):
            # For Laplace, fall back to simple epsilon rescaling
            return self._rescale_epsilon_laplace(cost, steps)

        # Gaussian: search over σ ∈ [sigma_lo, sigma_hi]
        sens = cost.sensitivity
        original_sigma = cost.sigma if cost.sigma else 1.0
        sigma_ceiling = original_sigma * self.spec.max_sigma_factor
        lo, hi = original_sigma, sigma_ceiling

        # Quick feasibility check: even at hi σ (near-zero ρ), does it fit?
        rho_at_hi = zcdp_gaussian(sens, hi)
        rdp_at_hi = rdp_curve_for_gaussian(sens, hi)
        proj_rdp_hi = projected_dp_epsilon(self.state.accumulated_rdp, rdp_at_hi, self.spec.total_delta)
        proj_zcdp_hi = zcdp_to_dp(self.state.consumed_rho + rho_at_hi, self.spec.total_delta)
        if min(proj_rdp_hi, proj_zcdp_hi) > self.spec.total_epsilon:
            return None  # hopeless

        # Binary search
        for _ in range(steps):
            mid = (lo + hi) / 2
            rho_mid = zcdp_gaussian(sens, mid)
            rdp_mid = rdp_curve_for_gaussian(sens, mid)
            proj_rdp = projected_dp_epsilon(self.state.accumulated_rdp, rdp_mid, self.spec.total_delta)
            proj_zcdp = zcdp_to_dp(self.state.consumed_rho + rho_mid, self.spec.total_delta)
            projected = min(proj_rdp, proj_zcdp)

            if projected <= self.spec.total_epsilon:
                hi = mid  # still feasible, try smaller σ (more accuracy)
            else:
                lo = mid  # too tight, need larger σ

        sigma_star = hi
        rho_star = zcdp_gaussian(sens, sigma_star)
        # ε_basic for this sigma: back out from Gaussian noise σ → ε = Δ√(2ln(1.25/δ))/σ
        eps_star = _sigma_to_epsilon_basic(sens, sigma_star, cost.delta_basic or self.spec.total_delta)
        return (eps_star, sigma_star, rho_star)

    def _rescale_epsilon_laplace(
        self, cost: QueryCost, steps: int = 64
    ) -> Optional[Tuple[float, float, float]]:
        """Binary-search over ε ∈ (0, cost.epsilon_basic] for Laplace mechanism."""
        sens = cost.sensitivity
        lo, hi = 0.0, cost.epsilon_basic

        # Check minimum feasibility at ε=1e-6
        b_min = sens / 1e-6
        rdp_min = rdp_curve_for_laplace(sens, b_min)
        if projected_dp_epsilon(self.state.accumulated_rdp, rdp_min, self.spec.total_delta) > self.spec.total_epsilon:
            return None

        for _ in range(steps):
            mid = (lo + hi) / 2
            b = sens / mid if mid > 0 else math.inf
            rdp_mid = rdp_curve_for_laplace(sens, b)
            proj = projected_dp_epsilon(self.state.accumulated_rdp, rdp_mid, self.spec.total_delta)
            if proj <= self.spec.total_epsilon:
                lo = mid  # feasible, try larger ε (less noise)
            else:
                hi = mid

        eps_star = lo
        b_star = sens / eps_star if eps_star > 0 else math.inf
        rho_star = zcdp_laplace_approx(sens, eps_star)
        return (eps_star, b_star, rho_star)


# ---------------------------------------------------------------------------
# QueryPlan result
# ---------------------------------------------------------------------------

@dataclass
class QueryPlan:
    decision: str                        # "accept" | "rewrite" | "reject"
    epsilon_requested: float
    epsilon_feasible: Optional[float]    # None if rejected
    sigma_feasible: Optional[float]      # None for non-Gaussian or rejected
    projected_epsilon_basic: float
    projected_epsilon_rdp: float
    projected_epsilon_zcdp: float
    savings_vs_basic: Optional[float]
    explanation: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rdp_curve(
    mechanism: str, sensitivity: float, sigma_or_b: float
) -> List[Tuple[float, float]]:
    if mechanism == "gaussian":
        return rdp_curve_for_gaussian(sensitivity, sigma_or_b)
    return rdp_curve_for_laplace(sensitivity, sigma_or_b)


def _sigma_to_epsilon_basic(sensitivity: float, sigma: float, delta: float) -> float:
    """Invert the Gaussian calibration: σ = Δ·√(2 ln(1.25/δ)) / ε → ε."""
    if delta <= 0:
        delta = 1e-5
    return sensitivity * math.sqrt(2 * math.log(1.25 / delta)) / sigma


def make_query_cost_gaussian(
    sensitivity: float, sigma: float, delta: float = 1e-5
) -> QueryCost:
    eps_basic = _sigma_to_epsilon_basic(sensitivity, sigma, delta)
    return QueryCost(
        epsilon_basic=eps_basic,
        delta_basic=delta,
        rdp_curve=rdp_curve_for_gaussian(sensitivity, sigma),
        rho=zcdp_gaussian(sensitivity, sigma),
        mechanism="gaussian",
        sensitivity=sensitivity,
        sigma=sigma,
    )


def make_query_cost_laplace(
    sensitivity: float, epsilon: float
) -> QueryCost:
    b = sensitivity / epsilon
    return QueryCost(
        epsilon_basic=epsilon,
        delta_basic=0.0,
        rdp_curve=rdp_curve_for_laplace(sensitivity, b),
        rho=zcdp_laplace_approx(sensitivity, epsilon),
        mechanism="laplace",
        sensitivity=sensitivity,
        sigma=None,
    )
