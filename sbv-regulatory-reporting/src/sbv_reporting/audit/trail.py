"""Immutable audit trail with per-entry SHA-256 chained hashes.

Each log entry contains:
  - timestamp, event type, operator, details
  - sha256 of (previous_hash + canonical entry JSON)

This produces a tamper-evident chain: altering any past entry breaks all
subsequent hashes, detectable on verification.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import numpy as np
    _NP_TYPES = (np.integer, np.floating, np.bool_)
except ImportError:
    _NP_TYPES = ()


class _SafeEncoder(json.JSONEncoder):
    """Serialise NumPy scalars and other non-standard types."""
    def default(self, obj):
        if _NP_TYPES and isinstance(obj, _NP_TYPES):
            return obj.item()
        return super().default(obj)

from sbv_reporting.utils.config import get_config


class AuditTrail:
    _lock = threading.Lock()

    def __init__(self, run_id: str, log_dir: str | Path | None = None):
        self.run_id = run_id
        cfg = get_config()
        base = Path(log_dir or cfg["audit"]["log_dir"])
        base.mkdir(parents=True, exist_ok=True)
        self.log_path = base / f"audit_{run_id}.jsonl"
        self._chain_hash = "0" * 64  # genesis hash

        if self.log_path.exists():
            # resume chain from last entry
            lines = self.log_path.read_text(encoding="utf-8").strip().splitlines()
            if lines:
                last = json.loads(lines[-1])
                self._chain_hash = last["entry_hash"]

    # ------------------------------------------------------------------
    def log(
        self,
        event: str,
        details: dict[str, Any],
        operator: str = "SYSTEM",
        level: str = "INFO",
    ) -> dict:
        entry: dict[str, Any] = {
            "run_id": self.run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "event": event,
            "operator": operator,
            "details": details,
        }
        canonical = json.dumps(entry, sort_keys=True, ensure_ascii=False, cls=_SafeEncoder)
        entry["prev_hash"] = self._chain_hash
        entry["entry_hash"] = hashlib.sha256(
            (self._chain_hash + canonical).encode()
        ).hexdigest()

        with self._lock:
            self._chain_hash = entry["entry_hash"]
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False, cls=_SafeEncoder) + "\n")

        return entry

    # ------------------------------------------------------------------
    def verify(self) -> tuple[bool, list[str]]:
        """Replay the chain and detect tampering. Returns (ok, errors)."""
        errors: list[str] = []
        lines = self.log_path.read_text(encoding="utf-8").strip().splitlines()
        if not lines:
            return True, []

        prev_hash = "0" * 64
        for i, raw in enumerate(lines):
            entry = json.loads(raw)
            stored_hash = entry.pop("entry_hash")
            stored_prev = entry.pop("prev_hash")

            if stored_prev != prev_hash:
                errors.append(f"Line {i+1}: prev_hash mismatch (chain broken)")

            canonical = json.dumps(entry, sort_keys=True, ensure_ascii=False, cls=_SafeEncoder)
            computed = hashlib.sha256((prev_hash + canonical).encode()).hexdigest()
            if computed != stored_hash:
                errors.append(f"Line {i+1}: entry_hash mismatch (entry tampered)")

            prev_hash = stored_hash

        return len(errors) == 0, errors

    # ------------------------------------------------------------------
    def summary(self) -> dict:
        lines = self.log_path.read_text(encoding="utf-8").strip().splitlines()
        events: dict[str, int] = {}
        for raw in lines:
            e = json.loads(raw)["event"]
            events[e] = events.get(e, 0) + 1
        return {
            "run_id": self.run_id,
            "log_path": str(self.log_path),
            "total_entries": len(lines),
            "events": events,
            "chain_hash": self._chain_hash,
        }
