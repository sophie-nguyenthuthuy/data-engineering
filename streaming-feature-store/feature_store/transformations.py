"""
Shared feature computation logic — identical functions used by both
batch and streaming paths to prevent training-serving skew.

Each feature is a pure function:
    compute(record: dict, context: dict) -> value

`record`  – the raw event (user_id, amount, timestamp, category, …)
`context` – optional pre-computed stats (global mean, stddev, etc.)
            provided by the serving layer at inference time.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone

from feature_store.registry import FeatureDefinition, FeatureRegistry, FeatureType


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _safe_float(val: object, default: float = 0.0) -> float:
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _parse_ts(ts: object) -> datetime:
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(float(ts), tz=timezone.utc)
    return datetime.fromisoformat(str(ts))


# ---------------------------------------------------------------------------
# Feature compute functions
# ---------------------------------------------------------------------------

def _amount_log1p(record: dict, _ctx: dict) -> float:
    """log(1 + amount) — compresses heavy-tailed transaction amounts."""
    return round(math.log1p(_safe_float(record.get("amount", 0))), 6)


def _amount_bucket(record: dict, _ctx: dict) -> str:
    """Coarse bucket for transaction amount."""
    amount = _safe_float(record.get("amount", 0))
    if amount < 10:
        return "micro"
    if amount < 100:
        return "small"
    if amount < 1000:
        return "medium"
    if amount < 10000:
        return "large"
    return "whale"


def _amount_zscore(record: dict, ctx: dict) -> float:
    """Z-score using population stats stored in context."""
    mu = _safe_float(ctx.get("amount_mean", 250.0))
    sigma = _safe_float(ctx.get("amount_stddev", 200.0))
    if sigma == 0:
        return 0.0
    return round((_safe_float(record.get("amount", 0)) - mu) / sigma, 6)


def _hour_of_day(record: dict, _ctx: dict) -> int:
    """UTC hour of the transaction timestamp."""
    return _parse_ts(record.get("timestamp", datetime.now(tz=timezone.utc))).hour


def _day_of_week(record: dict, _ctx: dict) -> str:
    """Day name of the transaction."""
    return _parse_ts(
        record.get("timestamp", datetime.now(tz=timezone.utc))
    ).strftime("%A")


def _is_weekend(record: dict, _ctx: dict) -> int:
    """1 if weekend, 0 otherwise."""
    dow = _parse_ts(
        record.get("timestamp", datetime.now(tz=timezone.utc))
    ).weekday()
    return int(dow >= 5)


def _category_encoded(record: dict, _ctx: dict) -> str:
    """Normalised transaction category."""
    return str(record.get("category", "unknown")).lower().strip()


def _user_age_bucket(record: dict, _ctx: dict) -> str:
    """Age bracket for the user."""
    age = int(_safe_float(record.get("user_age", 30)))
    if age < 18:
        return "minor"
    if age < 25:
        return "young_adult"
    if age < 40:
        return "adult"
    if age < 60:
        return "middle_aged"
    return "senior"


def _account_tenure_days(record: dict, _ctx: dict) -> float:
    """Days since account was created (from context or record)."""
    created_at = record.get("account_created_at")
    if created_at is None:
        return _safe_float(ctx.get("account_tenure_days", 365)) if False else 365.0
    ts = _parse_ts(record.get("timestamp", datetime.now(tz=timezone.utc)))
    created = _parse_ts(created_at)
    return max(0.0, (ts - created).total_seconds() / 86400)


def _spending_velocity(record: dict, ctx: dict) -> float:
    """Ratio of current amount to 30-day mean spend (from context)."""
    mean_30d = _safe_float(ctx.get("user_mean_spend_30d", 0))
    if mean_30d == 0:
        return 1.0
    return round(_safe_float(record.get("amount", 0)) / mean_30d, 6)


def _is_high_risk_category(record: dict, _ctx: dict) -> int:
    """1 if category is in a high-risk list."""
    HIGH_RISK = {"gambling", "crypto", "forex", "adult", "firearms"}
    return int(str(record.get("category", "")).lower() in HIGH_RISK)


# ---------------------------------------------------------------------------
# Registry construction
# ---------------------------------------------------------------------------

def build_registry() -> FeatureRegistry:
    registry = FeatureRegistry()

    registry.register(FeatureDefinition(
        name="amount_log1p",
        feature_type=FeatureType.CONTINUOUS,
        description="log(1 + transaction amount)",
        compute_fn=_amount_log1p,
        default_value=0.0,
        tags=["amount", "numerical"],
    ))
    registry.register(FeatureDefinition(
        name="amount_bucket",
        feature_type=FeatureType.CATEGORICAL,
        description="Coarse bucket for transaction amount",
        compute_fn=_amount_bucket,
        default_value="unknown",
        tags=["amount", "categorical"],
    ))
    registry.register(FeatureDefinition(
        name="amount_zscore",
        feature_type=FeatureType.CONTINUOUS,
        description="Z-score of transaction amount using population stats",
        compute_fn=_amount_zscore,
        default_value=0.0,
        tags=["amount", "numerical", "normalised"],
    ))
    registry.register(FeatureDefinition(
        name="hour_of_day",
        feature_type=FeatureType.CONTINUOUS,
        description="UTC hour of the transaction (0–23)",
        compute_fn=_hour_of_day,
        default_value=12,
        tags=["temporal"],
    ))
    registry.register(FeatureDefinition(
        name="day_of_week",
        feature_type=FeatureType.CATEGORICAL,
        description="Day name of the transaction",
        compute_fn=_day_of_week,
        default_value="Monday",
        tags=["temporal", "categorical"],
    ))
    registry.register(FeatureDefinition(
        name="is_weekend",
        feature_type=FeatureType.CATEGORICAL,
        description="1 if transaction occurred on a weekend",
        compute_fn=_is_weekend,
        default_value=0,
        tags=["temporal", "binary"],
    ))
    registry.register(FeatureDefinition(
        name="category_encoded",
        feature_type=FeatureType.CATEGORICAL,
        description="Normalised transaction category string",
        compute_fn=_category_encoded,
        default_value="unknown",
        tags=["category", "categorical"],
    ))
    registry.register(FeatureDefinition(
        name="user_age_bucket",
        feature_type=FeatureType.CATEGORICAL,
        description="Age bracket for the user",
        compute_fn=_user_age_bucket,
        default_value="adult",
        tags=["user", "categorical"],
    ))
    registry.register(FeatureDefinition(
        name="account_tenure_days",
        feature_type=FeatureType.CONTINUOUS,
        description="Days since account was created",
        compute_fn=_account_tenure_days,
        default_value=365.0,
        tags=["user", "numerical"],
    ))
    registry.register(FeatureDefinition(
        name="spending_velocity",
        feature_type=FeatureType.CONTINUOUS,
        description="Ratio of current amount to user's 30-day mean spend",
        compute_fn=_spending_velocity,
        default_value=1.0,
        tags=["user", "numerical", "ratio"],
    ))
    registry.register(FeatureDefinition(
        name="is_high_risk_category",
        feature_type=FeatureType.CATEGORICAL,
        description="1 if transaction category is high-risk",
        compute_fn=_is_high_risk_category,
        default_value=0,
        tags=["risk", "binary"],
    ))

    return registry
