from pydantic import BaseModel, Field
from typing import Optional
import time


class VectorClock(BaseModel):
    """Logical clock tracking causality across regions."""
    clocks: dict[str, int] = Field(default_factory=dict)

    def increment(self, region_id: str) -> "VectorClock":
        clocks = dict(self.clocks)
        clocks[region_id] = clocks.get(region_id, 0) + 1
        return VectorClock(clocks=clocks)

    def merge(self, other: "VectorClock") -> "VectorClock":
        all_keys = set(self.clocks) | set(other.clocks)
        merged = {k: max(self.clocks.get(k, 0), other.clocks.get(k, 0)) for k in all_keys}
        return VectorClock(clocks=merged)

    def dominates(self, other: "VectorClock") -> bool:
        """True if self is causally after other (happened-after)."""
        all_keys = set(self.clocks) | set(other.clocks)
        at_least_one_greater = any(
            self.clocks.get(k, 0) > other.clocks.get(k, 0) for k in all_keys
        )
        all_gte = all(
            self.clocks.get(k, 0) >= other.clocks.get(k, 0) for k in all_keys
        )
        return at_least_one_greater and all_gte

    def concurrent_with(self, other: "VectorClock") -> bool:
        """True if neither dominates — genuine concurrent conflict.
        Equal clocks represent the same event, not a conflict."""
        if self.clocks == other.clocks:
            return False
        return not self.dominates(other) and not other.dominates(self)


class AccountRecord(BaseModel):
    """Core financial account — the data product written by each region."""
    account_id: str
    owner: str
    balance: float = 0.0
    currency: str = "USD"
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)

    # Causality + wall-clock
    vector_clock: VectorClock = Field(default_factory=VectorClock)
    wall_time: float = Field(default_factory=time.time)
    origin_region: str = ""
    last_writer_region: str = ""

    # CRDT PN-Counter: monotonic per-region credit/debit accumulator
    crdt_credits: dict[str, float] = Field(default_factory=dict)
    crdt_debits:  dict[str, float] = Field(default_factory=dict)

    @property
    def crdt_balance(self) -> float:
        return sum(self.crdt_credits.values()) - sum(self.crdt_debits.values())


class AccountCreate(BaseModel):
    owner: str
    balance: float = 0.0
    currency: str = "USD"
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)


class BalanceUpdate(BaseModel):
    delta: float
    note: str = ""


class ReplicationPayload(BaseModel):
    """Batch of records pushed from one region to a peer."""
    source_region: str
    records: list[AccountRecord]
    sent_at: float = Field(default_factory=time.time)


class ConflictEvent(BaseModel):
    account_id: str
    strategy_used: str
    local_wall_time: float
    remote_wall_time: float
    local_region: str
    remote_region: str
    resolution: str          # "kept_local" | "kept_remote" | "merged"
    resolved_at: float = Field(default_factory=time.time)


class PeerStatus(BaseModel):
    peer_url: str
    reachable: bool
    last_success_at: Optional[float] = None
    last_lag_seconds: Optional[float] = None


class RegionHealth(BaseModel):
    region_id: str
    status: str              # "healthy" | "degraded"
    conflict_strategy: str
    replication_peers: list[PeerStatus]
    last_replication_at: Optional[float]
    max_lag_seconds: Optional[float]
    total_accounts: int
    conflicts_resolved: int
    records_replicated_in: int
    records_replicated_out: int
    uptime_seconds: float
    recent_conflicts: list[ConflictEvent] = Field(default_factory=list)
