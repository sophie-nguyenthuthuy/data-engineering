"""
Three pluggable conflict-resolution strategies.

Each strategy receives the LOCAL record and the INCOMING remote record
and returns the record that should be stored, plus a resolution label.
"""
import time
from src.models import AccountRecord, VectorClock, ConflictEvent


# ------------------------------------------------------------------ #
#  Strategy 1 – Last-Write-Wins (wall clock)                          #
# ------------------------------------------------------------------ #

def resolve_lww(
    local: AccountRecord,
    remote: AccountRecord,
    local_region: str,
) -> tuple[AccountRecord, ConflictEvent]:
    """Winner = whichever record has the higher wall_time."""
    if remote.wall_time > local.wall_time:
        winner = remote
        resolution = "kept_remote"
    else:
        winner = local
        resolution = "kept_local"

    evt = ConflictEvent(
        account_id=local.account_id,
        strategy_used="lww",
        local_wall_time=local.wall_time,
        remote_wall_time=remote.wall_time,
        local_region=local_region,
        remote_region=remote.last_writer_region,
        resolution=resolution,
        resolved_at=time.time(),
    )
    # Merge vector clocks regardless of winner
    merged_vc = local.vector_clock.merge(remote.vector_clock)
    winner = winner.model_copy(update={"vector_clock": merged_vc})
    return winner, evt


# ------------------------------------------------------------------ #
#  Strategy 2 – CRDT PN-Counter (balance is a PN-Counter)            #
# ------------------------------------------------------------------ #

def resolve_crdt(
    local: AccountRecord,
    remote: AccountRecord,
    local_region: str,
) -> tuple[AccountRecord, ConflictEvent]:
    """
    Merge per-region credit and debit counters (take the max of each entry).
    The balance is always derived; no information is lost.
    Non-numeric fields fall back to LWW on wall_time.
    """
    merged_credits: dict[str, float] = {}
    merged_debits: dict[str, float] = {}

    all_regions = set(local.crdt_credits) | set(remote.crdt_credits)
    for r in all_regions:
        merged_credits[r] = max(
            local.crdt_credits.get(r, 0.0),
            remote.crdt_credits.get(r, 0.0),
        )

    all_regions = set(local.crdt_debits) | set(remote.crdt_debits)
    for r in all_regions:
        merged_debits[r] = max(
            local.crdt_debits.get(r, 0.0),
            remote.crdt_debits.get(r, 0.0),
        )

    merged_balance = sum(merged_credits.values()) - sum(merged_debits.values())
    merged_vc = local.vector_clock.merge(remote.vector_clock)

    # For non-numeric fields take the latest wall_time version
    base = remote if remote.wall_time > local.wall_time else local

    merged = base.model_copy(update={
        "balance": merged_balance,
        "crdt_credits": merged_credits,
        "crdt_debits": merged_debits,
        "vector_clock": merged_vc,
        "wall_time": max(local.wall_time, remote.wall_time),
    })

    evt = ConflictEvent(
        account_id=local.account_id,
        strategy_used="crdt",
        local_wall_time=local.wall_time,
        remote_wall_time=remote.wall_time,
        local_region=local_region,
        remote_region=remote.last_writer_region,
        resolution="merged",
        resolved_at=time.time(),
    )
    return merged, evt


# ------------------------------------------------------------------ #
#  Strategy 3 – Business-rule merge (fintech semantics)              #
# ------------------------------------------------------------------ #

def resolve_business(
    local: AccountRecord,
    remote: AccountRecord,
    local_region: str,
) -> tuple[AccountRecord, ConflictEvent]:
    """
    Fintech rules:
    1. Balance = sum of all deltas from both sides (never lose a transaction).
       We reconstruct this via CRDT counters, same as strategy 2.
    2. Tags   = union (OR-Set semantics, additive only).
    3. Metadata = last-writer-wins per key (higher wall_time wins each key).
    4. Owner   = keep whichever was set first (origin_region is authoritative).
    """
    # 1. Balance via CRDT counters
    merged_credits: dict[str, float] = {}
    merged_debits: dict[str, float] = {}

    for r in set(local.crdt_credits) | set(remote.crdt_credits):
        merged_credits[r] = max(
            local.crdt_credits.get(r, 0.0),
            remote.crdt_credits.get(r, 0.0),
        )
    for r in set(local.crdt_debits) | set(remote.crdt_debits):
        merged_debits[r] = max(
            local.crdt_debits.get(r, 0.0),
            remote.crdt_debits.get(r, 0.0),
        )
    merged_balance = sum(merged_credits.values()) - sum(merged_debits.values())

    # 2. Tags: union
    merged_tags = sorted(set(local.tags) | set(remote.tags))

    # 3. Metadata: per-key LWW
    merged_meta: dict[str, str] = dict(local.metadata)
    if remote.wall_time >= local.wall_time:
        merged_meta.update(remote.metadata)

    # 4. Owner: first writer wins (origin_region is set at creation)
    owner = local.owner  # local is the existing record → was created first

    merged_vc = local.vector_clock.merge(remote.vector_clock)

    merged = local.model_copy(update={
        "owner": owner,
        "balance": merged_balance,
        "crdt_credits": merged_credits,
        "crdt_debits": merged_debits,
        "tags": merged_tags,
        "metadata": merged_meta,
        "vector_clock": merged_vc,
        "wall_time": max(local.wall_time, remote.wall_time),
        "last_writer_region": (
            remote.last_writer_region
            if remote.wall_time > local.wall_time
            else local.last_writer_region
        ),
    })

    evt = ConflictEvent(
        account_id=local.account_id,
        strategy_used="business",
        local_wall_time=local.wall_time,
        remote_wall_time=remote.wall_time,
        local_region=local_region,
        remote_region=remote.last_writer_region,
        resolution="merged",
        resolved_at=time.time(),
    )
    return merged, evt


# ------------------------------------------------------------------ #
#  Dispatcher                                                          #
# ------------------------------------------------------------------ #

STRATEGIES = {
    "lww":      resolve_lww,
    "crdt":     resolve_crdt,
    "business": resolve_business,
}


def resolve_conflict(
    strategy: str,
    local: AccountRecord,
    remote: AccountRecord,
    local_region: str,
) -> tuple[AccountRecord, ConflictEvent]:
    fn = STRATEGIES.get(strategy, resolve_lww)
    return fn(local, remote, local_region)
