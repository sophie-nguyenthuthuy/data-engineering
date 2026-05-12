"""
Stateless and stateful fraud-detection rules.
Stateful rules rely on Redis for sliding-window counters and last-seen location.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import redis

RISK_THRESHOLDS = {
    "HIGH_AMOUNT": 5000,
    "CARD_NOT_PRESENT_HIGH": 2000,
    "ODD_HOURS_START": 2,   # UTC hour, inclusive
    "ODD_HOURS_END": 5,     # UTC hour, exclusive
    "VELOCITY_WINDOW_SEC": 600,   # 10-minute window
    "VELOCITY_MAX_TX": 6,
    "GEO_WINDOW_SEC": 1800,       # 30-minute window
    "GEO_MIN_KM": 400,            # impossible travel threshold
    "ROUND_MULTIPLES": {100, 500, 1000, 2000, 5000, 10000},
    "HIGH_RISK_CATEGORIES": {"wire_transfer", "crypto", "gambling", "unknown"},
}


@dataclass
class FraudSignal:
    rule: str
    severity: str          # LOW | MEDIUM | HIGH | CRITICAL
    score: int             # additive risk score 1-100
    detail: str
    metadata: dict = field(default_factory=dict)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class FraudDetector:
    def __init__(self, redis_host: str = "redis", redis_port: int = 6379):
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    def analyze(self, tx: dict) -> list[FraudSignal]:
        signals: list[FraudSignal] = []
        signals.extend(self._stateless_rules(tx))
        signals.extend(self._velocity_check(tx))
        signals.extend(self._geo_velocity_check(tx))
        return signals

    # ------------------------------------------------------------------
    # Stateless rules
    # ------------------------------------------------------------------
    def _stateless_rules(self, tx: dict) -> list[FraudSignal]:
        signals = []
        amount = tx.get("amount", 0)
        category = tx.get("merchant_category", "")
        card_present = tx.get("card_present", True)

        ts_str = tx.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_str)
            hour = ts.astimezone(timezone.utc).hour
        except Exception:
            hour = -1

        # Rule 1 — unusually large transaction
        if amount >= RISK_THRESHOLDS["HIGH_AMOUNT"]:
            severity = "CRITICAL" if amount >= 20000 else "HIGH"
            signals.append(FraudSignal(
                rule="HIGH_AMOUNT",
                severity=severity,
                score=min(100, int(amount / 500)),
                detail=f"Transaction amount ${amount:,.2f} exceeds threshold",
            ))

        # Rule 2 — card-not-present + high amount
        if not card_present and amount >= RISK_THRESHOLDS["CARD_NOT_PRESENT_HIGH"]:
            signals.append(FraudSignal(
                rule="CARD_NOT_PRESENT_HIGH",
                severity="HIGH",
                score=60,
                detail=f"Card-not-present transaction of ${amount:,.2f}",
            ))

        # Rule 3 — transaction at odd hours (2–5 AM UTC)
        if RISK_THRESHOLDS["ODD_HOURS_START"] <= hour < RISK_THRESHOLDS["ODD_HOURS_END"]:
            signals.append(FraudSignal(
                rule="ODD_HOURS",
                severity="MEDIUM",
                score=30,
                detail=f"Transaction at {hour:02d}:00 UTC",
            ))

        # Rule 4 — exact round-number amounts
        if amount in RISK_THRESHOLDS["ROUND_MULTIPLES"]:
            signals.append(FraudSignal(
                rule="ROUND_NUMBER",
                severity="LOW",
                score=20,
                detail=f"Exact round-number amount ${amount:,.0f}",
            ))

        # Rule 5 — high-risk merchant category
        if category in RISK_THRESHOLDS["HIGH_RISK_CATEGORIES"]:
            signals.append(FraudSignal(
                rule="HIGH_RISK_MERCHANT",
                severity="MEDIUM",
                score=40,
                detail=f"High-risk merchant category: {category}",
                metadata={"merchant": tx.get("merchant")},
            ))

        return signals

    # ------------------------------------------------------------------
    # Velocity check — sliding window via Redis sorted sets
    # ------------------------------------------------------------------
    def _velocity_check(self, tx: dict) -> list[FraudSignal]:
        account = tx.get("account_id", "")
        tx_id = tx.get("transaction_id", "")
        now_ts = datetime.now(timezone.utc).timestamp()
        window = RISK_THRESHOLDS["VELOCITY_WINDOW_SEC"]
        key = f"vel:{account}"

        pipe = self.r.pipeline()
        pipe.zadd(key, {tx_id: now_ts})
        pipe.zremrangebyscore(key, 0, now_ts - window)
        pipe.zcard(key)
        pipe.expire(key, window * 2)
        _, _, count, _ = pipe.execute()

        if count > RISK_THRESHOLDS["VELOCITY_MAX_TX"]:
            return [FraudSignal(
                rule="VELOCITY",
                severity="HIGH",
                score=70,
                detail=f"{count} transactions in {window // 60} minutes",
                metadata={"count": count, "window_minutes": window // 60},
            )]
        return []

    # ------------------------------------------------------------------
    # Geographic velocity — impossible travel check
    # ------------------------------------------------------------------
    def _geo_velocity_check(self, tx: dict) -> list[FraudSignal]:
        account = tx.get("account_id", "")
        lat = tx.get("latitude")
        lon = tx.get("longitude")
        if lat is None or lon is None:
            return []

        key = f"geo:{account}"
        now_ts = datetime.now(timezone.utc).timestamp()
        window = RISK_THRESHOLDS["GEO_WINDOW_SEC"]

        prev_raw = self.r.get(key)
        self.r.setex(key, window * 2, json.dumps({"lat": lat, "lon": lon, "ts": now_ts}))

        if not prev_raw:
            return []

        prev = json.loads(prev_raw)
        elapsed = now_ts - prev["ts"]
        if elapsed <= 0 or elapsed > window:
            return []

        dist_km = _haversine_km(prev["lat"], prev["lon"], lat, lon)
        if dist_km >= RISK_THRESHOLDS["GEO_MIN_KM"]:
            speed_kmh = dist_km / (elapsed / 3600)
            return [FraudSignal(
                rule="GEO_VELOCITY",
                severity="CRITICAL",
                score=90,
                detail=f"Impossible travel: {dist_km:,.0f} km in {elapsed / 60:.1f} min ({speed_kmh:,.0f} km/h)",
                metadata={"distance_km": round(dist_km, 1), "elapsed_sec": round(elapsed)},
            )]
        return []


def aggregate_risk(signals: list[FraudSignal]) -> tuple[int, str]:
    """Return (total_score, overall_severity)."""
    if not signals:
        return 0, "NONE"
    total = min(100, sum(s.score for s in signals))
    if total >= 80:
        return total, "CRITICAL"
    if total >= 60:
        return total, "HIGH"
    if total >= 35:
        return total, "MEDIUM"
    return total, "LOW"
