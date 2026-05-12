"""Integration tests for the regional SQLite store."""
import time
import pytest
import pytest_asyncio
from src.store.database import RegionalStore
from src.models import AccountRecord, VectorClock, ConflictEvent


@pytest_asyncio.fixture
async def store(tmp_path):
    s = RegionalStore(str(tmp_path / "test.db"))
    await s.open()
    yield s
    await s.close()


def _rec(account_id="acc-1", balance=100.0, wall_time=None) -> AccountRecord:
    return AccountRecord(
        account_id=account_id,
        owner="Test User",
        balance=balance,
        currency="USD",
        vector_clock=VectorClock(clocks={"region-a": 1}),
        wall_time=wall_time or time.time(),
        origin_region="region-a",
        last_writer_region="region-a",
        crdt_credits={"region-a": balance},
        crdt_debits={},
    )


@pytest.mark.asyncio
async def test_upsert_and_get(store):
    rec = _rec()
    await store.upsert_account(rec)
    fetched = await store.get_account("acc-1")
    assert fetched is not None
    assert fetched.balance == 100.0
    assert fetched.owner == "Test User"


@pytest.mark.asyncio
async def test_upsert_overwrites(store):
    await store.upsert_account(_rec(balance=100.0))
    updated = _rec(balance=250.0)
    await store.upsert_account(updated)
    fetched = await store.get_account("acc-1")
    assert fetched.balance == 250.0


@pytest.mark.asyncio
async def test_list_accounts(store):
    await store.upsert_account(_rec("acc-1"))
    await store.upsert_account(_rec("acc-2"))
    await store.upsert_account(_rec("acc-3"))
    accounts = await store.list_accounts()
    assert len(accounts) == 3


@pytest.mark.asyncio
async def test_count_accounts(store):
    assert await store.count_accounts() == 0
    await store.upsert_account(_rec("acc-1"))
    assert await store.count_accounts() == 1


@pytest.mark.asyncio
async def test_get_records_since(store):
    t0 = time.time()
    await store.upsert_account(_rec("acc-old", wall_time=t0 - 100))
    await store.upsert_account(_rec("acc-new", wall_time=t0 + 1))
    results = await store.get_all_records_since(t0)
    ids = {r.account_id for r in results}
    assert "acc-new" in ids
    assert "acc-old" not in ids


@pytest.mark.asyncio
async def test_conflict_log(store):
    evt = ConflictEvent(
        account_id="acc-1",
        strategy_used="lww",
        local_wall_time=1000.0,
        remote_wall_time=2000.0,
        local_region="region-a",
        remote_region="region-b",
        resolution="kept_remote",
        resolved_at=time.time(),
    )
    await store.log_conflict(evt)
    count = await store.count_conflicts()
    assert count == 1
    recent = await store.recent_conflicts(5)
    assert recent[0].account_id == "acc-1"
    assert recent[0].resolution == "kept_remote"


@pytest.mark.asyncio
async def test_replication_log(store):
    await store.log_replication("in", "http://peer:8000", 5, lag_seconds=0.12)
    await store.log_replication("out", "http://peer:8000", 3)
    assert await store.count_replicated("in") == 5
    assert await store.count_replicated("out") == 3
    lag = await store.latest_lag()
    assert lag == pytest.approx(0.12)
