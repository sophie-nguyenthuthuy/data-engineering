"""Access pattern tracker using an exponential moving average (EMA) of daily frequency."""
from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class KeyStats:
    key: str
    access_count: int = 0
    first_seen: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    # EMA of accesses-per-day; α = 0.3 gives ~3-day memory
    ema_freq: float = 0.0

    @property
    def age_days(self) -> float:
        return max((time.time() - self.first_seen) / 86400, 1e-9)

    @property
    def idle_days(self) -> float:
        return (time.time() - self.last_accessed) / 86400

    @property
    def lifetime_freq(self) -> float:
        """Raw accesses-per-day over entire lifetime."""
        return self.access_count / self.age_days


class AccessPatternTracker:
    """
    In-memory (optionally persisted to JSON) tracker of per-key access stats.

    Each recorded access updates:
      - access_count  (monotonic)
      - last_accessed (epoch)
      - ema_freq      (exponential moving average of daily frequency)

    The EMA is computed with a daily decay:
        ema_new = α * daily_rate + (1-α) * ema_old
    where daily_rate is 1/elapsed_days since the last access.
    """

    EMA_ALPHA = 0.3

    def __init__(self, persist_path: Optional[str] = None):
        self._stats: dict[str, KeyStats] = {}
        self._persist_path = Path(persist_path) if persist_path else None
        if self._persist_path and self._persist_path.exists():
            self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_access(self, key: str) -> KeyStats:
        now = time.time()
        if key not in self._stats:
            self._stats[key] = KeyStats(key=key, first_seen=now, last_accessed=now)

        stats = self._stats[key]
        elapsed_days = max((now - stats.last_accessed) / 86400, 1e-9)
        daily_rate = 1.0 / elapsed_days  # one access over elapsed period

        stats.ema_freq = (
            self.EMA_ALPHA * daily_rate + (1 - self.EMA_ALPHA) * stats.ema_freq
        )
        stats.access_count += 1
        stats.last_accessed = now
        return stats

    def get(self, key: str) -> Optional[KeyStats]:
        return self._stats.get(key)

    def get_or_create(self, key: str) -> KeyStats:
        if key not in self._stats:
            self._stats[key] = KeyStats(key=key)
        return self._stats[key]

    def keys_below_freq(self, threshold: float) -> list[str]:
        """Return keys whose EMA frequency has dropped below threshold (accesses/day)."""
        return [
            k for k, s in self._stats.items()
            if s.ema_freq < threshold and s.age_days > 1
        ]

    def keys_idle_for(self, days: float) -> list[str]:
        """Return keys not accessed for at least `days` days."""
        return [k for k, s in self._stats.items() if s.idle_days >= days]

    def hottest_keys(self, n: int = 10) -> list[KeyStats]:
        """Return the n most frequently accessed keys."""
        return sorted(self._stats.values(), key=lambda s: s.ema_freq, reverse=True)[:n]

    def coldest_keys(self, n: int = 10) -> list[KeyStats]:
        """Return the n least frequently accessed keys."""
        return sorted(self._stats.values(), key=lambda s: s.ema_freq)[:n]

    def remove(self, key: str) -> None:
        self._stats.pop(key, None)

    def snapshot(self) -> dict[str, KeyStats]:
        return dict(self._stats)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self) -> None:
        if not self._persist_path:
            return
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        data = {k: asdict(v) for k, v in self._stats.items()}
        self._persist_path.write_text(json.dumps(data, indent=2))

    def _load(self) -> None:
        data = json.loads(self._persist_path.read_text())
        for k, v in data.items():
            self._stats[k] = KeyStats(**v)
