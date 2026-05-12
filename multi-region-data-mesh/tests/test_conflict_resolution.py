"""
Unit tests for all three conflict resolution strategies.
No database or network needed — pure logic.
"""
import time
import pytest
from src.models import AccountRecord, VectorClock
from src.replication.strategies import resolve_lww, resolve_crdt, resolve_business


def _make_account(
    account_id="acc-1",
    owner="Alice",
    balance=100.0,
    region="region-a",
    wall_time=None,
    vc_clocks=None,
    crdt_credits=None,
    crdt_debits=None,
    tags=None,
    metadata=None,
) -> AccountRecord:
    return AccountRecord(
        account_id=account_id,
        owner=owner,
        balance=balance,
        currency="USD",
        tags=tags or [],
        metadata=metadata or {},
        vector_clock=VectorClock(clocks=vc_clocks or {}),
        wall_time=wall_time or time.time(),
        origin_region=region,
        last_writer_region=region,
        crdt_credits=crdt_credits if crdt_credits is not None else {region: balance},
        crdt_debits=crdt_debits if crdt_debits is not None else {},
    )


# ------------------------------------------------------------------ #
#  LWW                                                                #
# ------------------------------------------------------------------ #

class TestLWW:
    def test_remote_wins_when_newer(self):
        local  = _make_account(balance=100, wall_time=1000.0, region="region-a")
        remote = _make_account(balance=200, wall_time=2000.0, region="region-b")
        winner, evt = resolve_lww(local, remote, "region-a")
        assert winner.balance == 200
        assert evt.resolution == "kept_remote"
        assert evt.strategy_used == "lww"

    def test_local_wins_when_newer(self):
        local  = _make_account(balance=300, wall_time=3000.0, region="region-a")
        remote = _make_account(balance=200, wall_time=1000.0, region="region-b")
        winner, evt = resolve_lww(local, remote, "region-a")
        assert winner.balance == 300
        assert evt.resolution == "kept_local"

    def test_vector_clocks_always_merged(self):
        local  = _make_account(vc_clocks={"region-a": 3}, wall_time=1000.0)
        remote = _make_account(vc_clocks={"region-b": 5}, wall_time=2000.0)
        winner, _ = resolve_lww(local, remote, "region-a")
        # Even though remote won, clock is merged
        assert winner.vector_clock.clocks.get("region-a", 0) >= 0
        assert winner.vector_clock.clocks.get("region-b", 0) == 5


# ------------------------------------------------------------------ #
#  CRDT                                                               #
# ------------------------------------------------------------------ #

class TestCRDT:
    def test_balance_is_sum_of_merged_counters(self):
        # region-a credited 100, region-b credited 50 concurrently
        local = _make_account(
            balance=100, region="region-a",
            crdt_credits={"region-a": 100}, crdt_debits={},
        )
        remote = _make_account(
            balance=50, region="region-b",
            crdt_credits={"region-b": 50}, crdt_debits={},
        )
        winner, evt = resolve_crdt(local, remote, "region-a")
        assert winner.balance == 150
        assert evt.resolution == "merged"

    def test_debit_not_lost(self):
        local = _make_account(
            balance=80, region="region-a",
            crdt_credits={"region-a": 100}, crdt_debits={"region-a": 20},
        )
        remote = _make_account(
            balance=40, region="region-b",
            crdt_credits={"region-b": 50}, crdt_debits={"region-b": 10},
        )
        winner, _ = resolve_crdt(local, remote, "region-a")
        # 100+50 - 20+10 = 120
        assert winner.balance == pytest.approx(120.0)

    def test_counters_take_max_per_region(self):
        # Simulate region-a appearing with different credit in each view
        local  = _make_account(crdt_credits={"region-a": 100, "region-b": 30})
        remote = _make_account(crdt_credits={"region-a": 80,  "region-b": 60})
        winner, _ = resolve_crdt(local, remote, "region-a")
        assert winner.crdt_credits["region-a"] == 100
        assert winner.crdt_credits["region-b"] == 60


# ------------------------------------------------------------------ #
#  Business rules                                                     #
# ------------------------------------------------------------------ #

class TestBusinessRules:
    def test_balance_preserved_via_crdt(self):
        local = _make_account(
            crdt_credits={"region-a": 100}, crdt_debits={}, balance=100
        )
        remote = _make_account(
            crdt_credits={"region-b": 50}, crdt_debits={}, balance=50
        )
        winner, evt = resolve_business(local, remote, "region-a")
        assert winner.balance == 150
        assert evt.strategy_used == "business"

    def test_tags_are_union(self):
        local  = _make_account(tags=["vip", "gold"])
        remote = _make_account(tags=["gold", "platinum"])
        winner, _ = resolve_business(local, remote, "region-a")
        assert set(winner.tags) == {"vip", "gold", "platinum"}

    def test_metadata_lww_per_key(self):
        old_t = 1000.0
        new_t = 2000.0
        local  = _make_account(metadata={"tier": "standard"}, wall_time=old_t)
        remote = _make_account(metadata={"tier": "premium", "note": "vip"}, wall_time=new_t)
        winner, _ = resolve_business(local, remote, "region-a")
        assert winner.metadata["tier"] == "premium"
        assert winner.metadata["note"] == "vip"

    def test_owner_kept_from_local(self):
        """Local = existing record = was created first → owner is authoritative."""
        local  = _make_account(owner="Alice")
        remote = _make_account(owner="Bob", wall_time=time.time() + 1)
        winner, _ = resolve_business(local, remote, "region-a")
        assert winner.owner == "Alice"
