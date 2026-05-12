"""
Consumer-facing API  +  internal replication endpoints.

Consumers always read from their local region — no cross-region hops.
"""
import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from src.config import settings
from src.models import (
    AccountCreate, AccountRecord, BalanceUpdate,
    ReplicationPayload, VectorClock,
)

router = APIRouter()


def _get_engine(request: Request):
    return request.app.state.engine


def _get_store(request: Request):
    return request.app.state.store


# ------------------------------------------------------------------ #
#  Consumer endpoints  (reads are always local)                       #
# ------------------------------------------------------------------ #

@router.post("/accounts", response_model=AccountRecord, status_code=201,
             tags=["Consumer API"])
async def create_account(body: AccountCreate, request: Request):
    """Create a new account. Written locally; replicated to peers asynchronously."""
    store = _get_store(request)
    engine = _get_engine(request)

    account_id = str(uuid.uuid4())
    vc = VectorClock().increment(settings.region_id)

    rec = AccountRecord(
        account_id=account_id,
        owner=body.owner,
        balance=body.balance,
        currency=body.currency,
        tags=body.tags,
        metadata=body.metadata,
        vector_clock=vc,
        wall_time=time.time(),
        origin_region=settings.region_id,
        last_writer_region=settings.region_id,
        crdt_credits={settings.region_id: body.balance} if body.balance > 0 else {},
        crdt_debits={},
    )
    await store.upsert_account(rec)
    await engine.push_to_peers(rec)
    return rec


@router.get("/accounts", response_model=list[AccountRecord], tags=["Consumer API"])
async def list_accounts(request: Request):
    """List all accounts — served from local region storage."""
    return await _get_store(request).list_accounts()


@router.get("/accounts/{account_id}", response_model=AccountRecord, tags=["Consumer API"])
async def get_account(account_id: str, request: Request):
    """Get a single account — always a local read."""
    rec = await _get_store(request).get_account(account_id)
    if not rec:
        raise HTTPException(404, detail=f"Account {account_id} not found")
    return rec


@router.patch("/accounts/{account_id}/balance", response_model=AccountRecord,
              tags=["Consumer API"])
async def update_balance(account_id: str, body: BalanceUpdate, request: Request):
    """
    Apply a balance delta to an account.
    Uses CRDT PN-Counter internally so every delta is preserved regardless
    of which region processes it.
    """
    store = _get_store(request)
    engine = _get_engine(request)

    rec = await store.get_account(account_id)
    if not rec:
        raise HTTPException(404, detail=f"Account {account_id} not found")

    # Advance vector clock
    new_vc = rec.vector_clock.increment(settings.region_id)

    # Update CRDT counters
    crdt_credits = dict(rec.crdt_credits)
    crdt_debits  = dict(rec.crdt_debits)
    if body.delta >= 0:
        crdt_credits[settings.region_id] = (
            crdt_credits.get(settings.region_id, 0.0) + body.delta
        )
    else:
        crdt_debits[settings.region_id] = (
            crdt_debits.get(settings.region_id, 0.0) + abs(body.delta)
        )

    new_balance = sum(crdt_credits.values()) - sum(crdt_debits.values())

    meta = dict(rec.metadata)
    if body.note:
        meta["last_note"] = body.note

    updated = rec.model_copy(update={
        "balance": new_balance,
        "crdt_credits": crdt_credits,
        "crdt_debits": crdt_debits,
        "vector_clock": new_vc,
        "wall_time": time.time(),
        "last_writer_region": settings.region_id,
        "metadata": meta,
    })

    await store.upsert_account(updated)
    await engine.push_to_peers(updated)
    return updated


@router.put("/accounts/{account_id}/tags", response_model=AccountRecord,
            tags=["Consumer API"])
async def set_tags(account_id: str, tags: list[str], request: Request):
    """Replace tag list on an account."""
    store = _get_store(request)
    engine = _get_engine(request)

    rec = await store.get_account(account_id)
    if not rec:
        raise HTTPException(404, detail=f"Account {account_id} not found")

    updated = rec.model_copy(update={
        "tags": tags,
        "vector_clock": rec.vector_clock.increment(settings.region_id),
        "wall_time": time.time(),
        "last_writer_region": settings.region_id,
    })
    await store.upsert_account(updated)
    await engine.push_to_peers(updated)
    return updated


# ------------------------------------------------------------------ #
#  Internal replication endpoints  (peer-to-peer only)               #
# ------------------------------------------------------------------ #

@router.get("/internal/records", response_model=ReplicationPayload,
            tags=["Internal Replication"])
async def export_records(
    request: Request,
    since: float = Query(default=0.0, description="Return records with wall_time > since"),
):
    """Pull endpoint: peers call this to fetch delta records."""
    store = _get_store(request)
    records = await store.get_all_records_since(since)
    return ReplicationPayload(
        source_region=settings.region_id,
        records=records,
        sent_at=time.time(),
    )


@router.post("/internal/records", status_code=204, tags=["Internal Replication"])
async def receive_records(payload: ReplicationPayload, request: Request):
    """Push endpoint: peers post records here for faster convergence."""
    engine = _get_engine(request)
    for rec in payload.records:
        await engine._apply_record(rec)
