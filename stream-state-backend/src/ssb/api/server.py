"""FastAPI read API for querying live operator state."""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from ..state.serializer import decode_key, decode_value, encode_key, is_tombstone

if TYPE_CHECKING:
    from ..manager import StateBackendManager


def create_app(manager: "StateBackendManager") -> FastAPI:
    """
    Create and return the FastAPI application.

    Parameters
    ----------
    manager:
        The ``StateBackendManager`` instance.  All route handlers close
        over this reference.
    """
    app = FastAPI(
        title="Stream State Backend",
        description="Read API for querying live operator state",
        version="0.1.0",
    )

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    @app.get("/health", tags=["meta"])
    async def health() -> dict:
        topo = manager.current_topology
        version = topo.version if topo else 0
        return {"status": "ok", "version": version}

    # ------------------------------------------------------------------
    # Topology
    # ------------------------------------------------------------------

    @app.get("/topology", tags=["topology"])
    async def get_topology() -> dict:
        topo = manager.current_topology
        if topo is None:
            return {"version": 0, "operators": {}}
        return topo.to_dict()

    @app.get("/topology/migrations", tags=["topology"])
    async def get_migrations() -> dict:
        migrator = manager.migrator
        if migrator is None:
            return {"active": None, "history": []}
        active = migrator.active.to_dict() if migrator.active else None
        history = [t.to_dict() for t in migrator.history]
        return {"active": active, "history": history}

    # ------------------------------------------------------------------
    # Operators
    # ------------------------------------------------------------------

    @app.get("/operators", tags=["state"])
    async def list_operators() -> list[str]:
        topo = manager.current_topology
        if topo is None:
            return []
        return sorted(topo.operators.keys())

    @app.get("/operators/{op_id}/state-names", tags=["state"])
    async def list_state_names(op_id: str) -> list[str]:
        topo = manager.current_topology
        if topo is None or op_id not in topo.operators:
            raise HTTPException(status_code=404, detail=f"Operator '{op_id}' not found")
        return topo.operators[op_id].state_names

    # ------------------------------------------------------------------
    # State entries
    # ------------------------------------------------------------------

    @app.get("/operators/{op_id}/{state_name}/keys", tags=["state"])
    async def list_keys(
        op_id: str,
        state_name: str,
        limit: int = Query(default=100, ge=1, le=10_000),
        cursor: str | None = Query(default=None),
    ) -> dict:
        """
        Paginate over record keys in the given state.

        The *cursor* is a base64-encoded raw key bytes from the previous
        page.  The next cursor is returned as ``next_cursor`` in the
        response (``null`` when the last page is reached).
        """
        cf = _require_cf(manager, op_id, state_name)
        start_key = base64.b64decode(cursor) if cursor else None

        keys: list[Any] = []
        last_raw_k: bytes | None = None

        for raw_k, raw_v in manager.backend.scan(
            cf, start_key=start_key, limit=limit + 1
        ):
            # Skip map-entry sub-keys (those containing \xff suffix)
            if b"\xff" in raw_k:
                continue
            if is_tombstone(raw_v):
                continue
            keys.append(decode_key(raw_k))
            last_raw_k = raw_k
            if len(keys) >= limit:
                break

        next_cursor: str | None = None
        # Check if there are more results
        if last_raw_k is not None and len(keys) == limit:
            # Peek one more to see if we're at the end
            remaining = list(
                manager.backend.scan(cf, start_key=last_raw_k, limit=2)
            )
            # remaining[0] is last_raw_k itself; if len > 1 there are more
            if len(remaining) > 1:
                next_cursor = base64.b64encode(last_raw_k).decode()

        return {"keys": keys, "next_cursor": next_cursor}

    @app.get("/operators/{op_id}/{state_name}", tags=["state"])
    async def scan_state(
        op_id: str,
        state_name: str,
        limit: int = Query(default=100, ge=1, le=10_000),
        cursor: str | None = Query(default=None),
    ) -> dict:
        """Scan all entries in a state, returning decoded key-value pairs."""
        cf = _require_cf(manager, op_id, state_name)
        start_key = base64.b64decode(cursor) if cursor else None

        entries: list[dict] = []
        last_raw_k: bytes | None = None

        for raw_k, raw_v in manager.backend.scan(
            cf, start_key=start_key, limit=limit + 1
        ):
            if b"\xff" in raw_k:
                # Map sub-entry — include under its parent key
                continue
            if is_tombstone(raw_v):
                continue
            try:
                ts, value = decode_value(raw_v)
            except ValueError:
                continue
            entries.append(
                {
                    "key": decode_key(raw_k),
                    "value": value,
                    "timestamp_ms": ts,
                }
            )
            last_raw_k = raw_k
            if len(entries) >= limit:
                break

        next_cursor = None
        if last_raw_k is not None and len(entries) == limit:
            remaining = list(
                manager.backend.scan(cf, start_key=last_raw_k, limit=2)
            )
            if len(remaining) > 1:
                next_cursor = base64.b64encode(last_raw_k).decode()

        return {"entries": entries, "next_cursor": next_cursor}

    @app.get("/operators/{op_id}/{state_name}/{key}", tags=["state"])
    async def get_state_entry(op_id: str, state_name: str, key: str) -> dict:
        """
        Decode and return the current value for *key*.

        The *key* path parameter is JSON-encoded (e.g. a plain string
        ``hello``, an integer ``42``, etc.) OR a base64-encoded raw
        msgpack key produced by the SDK.
        """
        cf = _require_cf(manager, op_id, state_name)

        # Try the key as a JSON literal first, then as a raw string
        import json

        raw_k: bytes | None = None
        try:
            parsed_key = json.loads(key)
            raw_k = encode_key(parsed_key)
        except (json.JSONDecodeError, ValueError):
            raw_k = encode_key(key)

        raw_v = manager.backend.get(cf, raw_k)
        if raw_v is None or is_tombstone(raw_v):
            raise HTTPException(
                status_code=404,
                detail=f"Key {key!r} not found in {op_id}/{state_name}",
            )

        try:
            ts, value = decode_value(raw_v)
        except ValueError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        return {"key": key, "value": value, "timestamp_ms": ts}

    return app


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _require_cf(manager: "StateBackendManager", op_id: str, state_name: str) -> str:
    """
    Return the CF name for *(op_id, state_name)*, raising 404 if the
    operator or state is not registered in the current topology.
    """
    topo = manager.current_topology
    if topo is None:
        raise HTTPException(status_code=404, detail="No topology registered")
    if op_id not in topo.operators:
        raise HTTPException(status_code=404, detail=f"Operator '{op_id}' not found")
    op = topo.operators[op_id]
    if state_name not in op.state_names:
        raise HTTPException(
            status_code=404,
            detail=f"State '{state_name}' not found on operator '{op_id}'",
        )
    return f"{op_id}::{state_name}"
