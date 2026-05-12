"""Multi-tenant JWT auth. Seeded with one user per tenant on first boot.
Tokens carry (sub, tenant_id, role). Row-level filtering in the dashboard is
enforced by reading `tenant_id` out of the verified token, never from query args.
"""
from __future__ import annotations

import hashlib
import hmac
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import jwt
from fastapi import Depends, Header, HTTPException, status

from .config import DATA_DIR, load, tenant_ids

_DB_PATH = DATA_DIR / "auth.duckdb"


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(_DB_PATH))


def _hash(password: str, salt: bytes) -> str:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 120_000).hex()


def init() -> None:
    with _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                username    VARCHAR PRIMARY KEY,
                tenant_id   VARCHAR NOT NULL,
                role        VARCHAR NOT NULL,  -- admin | viewer
                salt        BLOB    NOT NULL,
                pw_hash     VARCHAR NOT NULL,
                created_at  TIMESTAMP DEFAULT now()
            )
            """
        )
        # seed one admin per tenant (username = {tenant}-admin, password = "changeme")
        for tid in tenant_ids():
            username = f"{tid}-admin"
            exists = c.execute("SELECT 1 FROM users WHERE username = ?", [username]).fetchone()
            if exists:
                continue
            salt = os.urandom(16)
            c.execute(
                "INSERT INTO users (username, tenant_id, role, salt, pw_hash) VALUES (?, ?, 'admin', ?, ?)",
                [username, tid, salt, _hash("changeme", salt)],
            )


def authenticate(username: str, password: str) -> dict[str, Any] | None:
    with _conn() as c:
        row = c.execute(
            "SELECT username, tenant_id, role, salt, pw_hash FROM users WHERE username = ?",
            [username],
        ).fetchone()
    if not row:
        return None
    _, tenant_id, role, salt, pw_hash = row
    if not hmac.compare_digest(_hash(password, salt), pw_hash):
        return None
    return {"username": username, "tenant_id": tenant_id, "role": role}


def mint_token(user: dict[str, Any]) -> str:
    cfg = load()["auth"]
    payload = {
        "sub": user["username"],
        "tenant_id": user["tenant_id"],
        "role": user["role"],
        "exp": datetime.now(timezone.utc) + timedelta(hours=cfg["token_ttl_hours"]),
    }
    return jwt.encode(payload, cfg["jwt_secret"], algorithm="HS256")


def _decode(token: str) -> dict[str, Any]:
    cfg = load()["auth"]
    try:
        return jwt.decode(token, cfg["jwt_secret"], algorithms=["HS256"])
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))


def current_user(authorization: str = Header(default="")) -> dict[str, Any]:
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    return _decode(authorization.split(" ", 1)[1])


def require_admin(user: dict = Depends(current_user)) -> dict:
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="admin required")
    return user
