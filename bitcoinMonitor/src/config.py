"""Centralised env-driven config."""
from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    assets: tuple[str, ...]
    vs_currency: str
    poll_seconds: int
    db_path: str

    @classmethod
    def from_env(cls) -> "Settings":
        raw = os.getenv("ASSETS", "bitcoin,ethereum,solana")
        assets = tuple(a.strip() for a in raw.split(",") if a.strip())
        return cls(
            assets=assets,
            vs_currency=os.getenv("VS_CURRENCY", "usd"),
            poll_seconds=int(os.getenv("POLL_SECONDS", "15")),
            db_path=os.getenv("DB_PATH", "data/prices.db"),
        )


settings = Settings.from_env()
