"""
Health + metrics endpoints consumed by the dashboard.
"""
import time
from fastapi import APIRouter, Request
from src.config import settings
from src.models import RegionHealth, PeerStatus

router = APIRouter()
_start_time = time.time()


@router.get("/health", response_model=RegionHealth, tags=["Health"])
async def health(request: Request):
    store = request.app.state.store
    engine = request.app.state.engine

    peer_statuses = [
        PeerStatus(
            peer_url=p.url,
            reachable=p.reachable,
            last_success_at=p.last_success_at,
            last_lag_seconds=p.last_lag_seconds,
        )
        for p in engine.peers.values()
    ]

    max_lag = None
    lags = [p.last_lag_seconds for p in engine.peers.values() if p.last_lag_seconds is not None]
    if lags:
        max_lag = max(lags)

    last_rep = max(
        (p.last_success_at for p in engine.peers.values() if p.last_success_at),
        default=None,
    )

    all_reachable = all(p.reachable for p in engine.peers.values()) if engine.peers else True
    status = "healthy" if all_reachable else "degraded"

    return RegionHealth(
        region_id=settings.region_id,
        status=status,
        conflict_strategy=settings.conflict_strategy,
        replication_peers=peer_statuses,
        last_replication_at=last_rep,
        max_lag_seconds=max_lag,
        total_accounts=await store.count_accounts(),
        conflicts_resolved=await store.count_conflicts(),
        records_replicated_in=await store.count_replicated("in"),
        records_replicated_out=await store.count_replicated("out"),
        uptime_seconds=time.time() - _start_time,
        recent_conflicts=await store.recent_conflicts(10),
    )


@router.get("/ping", tags=["Health"])
async def ping():
    return {"region": settings.region_id, "ts": time.time()}
