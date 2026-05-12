"""
Customer key registry — tracks which CMK version each customer is on,
rotation state, and RTBF audit trail.
"""

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class KeyVersion:
    def __init__(self, data: dict):
        self.version: int = data["version"]
        self.cmk_id: str = data["cmk_id"]
        self.created_at: str = data["created_at"]
        self.status: str = data["status"]  # active | rotating_out | retired | deleted
        self.retired_at: Optional[str] = data.get("retired_at")

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "cmk_id": self.cmk_id,
            "created_at": self.created_at,
            "status": self.status,
            "retired_at": self.retired_at,
        }


class CustomerKeyRecord:
    def __init__(self, data: dict):
        self.customer_id: str = data["customer_id"]
        self.current_version: int = data["current_version"]
        self.rotation_in_progress: bool = data.get("rotation_in_progress", False)
        self.forgotten: bool = data.get("forgotten", False)
        self.forgotten_at: Optional[str] = data.get("forgotten_at")
        self.versions: list[KeyVersion] = [KeyVersion(v) for v in data.get("versions", [])]

    def active_version(self) -> Optional[KeyVersion]:
        for v in self.versions:
            if v.version == self.current_version:
                return v
        return None

    def previous_version(self) -> Optional[KeyVersion]:
        """Returns the version being rotated out (if rotation is in progress)."""
        for v in self.versions:
            if v.status == "rotating_out":
                return v
        return None

    def get_version(self, version: int) -> Optional[KeyVersion]:
        for v in self.versions:
            if v.version == version:
                return v
        return None

    def to_dict(self) -> dict:
        return {
            "customer_id": self.customer_id,
            "current_version": self.current_version,
            "rotation_in_progress": self.rotation_in_progress,
            "forgotten": self.forgotten,
            "forgotten_at": self.forgotten_at,
            "versions": [v.to_dict() for v in self.versions],
        }


class KeyRegistry:
    """Thread-safe JSON-backed registry of customer CMK versions."""

    def __init__(self, registry_path: str):
        self._path = Path(registry_path)
        self._lock = threading.Lock()
        self._data: dict[str, dict] = self._load()

    # ------------------------------------------------------------------
    # Customer lifecycle
    # ------------------------------------------------------------------

    def register_customer(self, customer_id: str, cmk_id: str) -> CustomerKeyRecord:
        with self._lock:
            if customer_id in self._data:
                raise ValueError(f"Customer {customer_id} already registered")
            record = {
                "customer_id": customer_id,
                "current_version": 1,
                "rotation_in_progress": False,
                "forgotten": False,
                "forgotten_at": None,
                "versions": [
                    {
                        "version": 1,
                        "cmk_id": cmk_id,
                        "created_at": _now_iso(),
                        "status": "active",
                        "retired_at": None,
                    }
                ],
            }
            self._data[customer_id] = record
            self._save()
            return CustomerKeyRecord(record)

    def get_customer(self, customer_id: str) -> CustomerKeyRecord:
        with self._lock:
            if customer_id not in self._data:
                raise KeyError(f"Customer not found: {customer_id}")
            return CustomerKeyRecord(self._data[customer_id])

    def list_customers(self) -> list[str]:
        with self._lock:
            return list(self._data.keys())

    # ------------------------------------------------------------------
    # Rotation state machine
    # ------------------------------------------------------------------

    def begin_rotation(self, customer_id: str, new_cmk_id: str) -> tuple[KeyVersion, KeyVersion]:
        """
        Marks old version as 'rotating_out', creates new version as 'active'.
        Returns (old_version, new_version).
        """
        with self._lock:
            record = self._data[customer_id]
            old_version_num = record["current_version"]
            new_version_num = old_version_num + 1

            for v in record["versions"]:
                if v["version"] == old_version_num:
                    v["status"] = "rotating_out"

            record["versions"].append({
                "version": new_version_num,
                "cmk_id": new_cmk_id,
                "created_at": _now_iso(),
                "status": "active",
                "retired_at": None,
            })
            record["current_version"] = new_version_num
            record["rotation_in_progress"] = True
            self._save()

            cr = CustomerKeyRecord(record)
            return cr.get_version(old_version_num), cr.get_version(new_version_num)

    def complete_rotation(self, customer_id: str, old_version_num: int) -> None:
        """Marks old version as 'retired' and clears rotation_in_progress flag."""
        with self._lock:
            record = self._data[customer_id]
            for v in record["versions"]:
                if v["version"] == old_version_num:
                    v["status"] = "retired"
                    v["retired_at"] = _now_iso()
            record["rotation_in_progress"] = False
            self._save()

    # ------------------------------------------------------------------
    # Right to be forgotten
    # ------------------------------------------------------------------

    def mark_forgotten(self, customer_id: str) -> None:
        with self._lock:
            record = self._data[customer_id]
            record["forgotten"] = True
            record["forgotten_at"] = _now_iso()
            for v in record["versions"]:
                if v["status"] in ("active", "rotating_out"):
                    v["status"] = "deleted"
            record["rotation_in_progress"] = False
            self._save()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if self._path.exists():
            return json.loads(self._path.read_text())
        return {}

    def _save(self) -> None:
        self._path.write_text(json.dumps(self._data, indent=2))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
