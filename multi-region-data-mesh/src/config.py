from pydantic_settings import BaseSettings
from typing import Literal


class Settings(BaseSettings):
    region_id: str = "region-a"
    host: str = "0.0.0.0"
    port: int = 8000
    db_path: str = "/tmp/mesh_region_a.db"

    # Comma-separated list of peer base URLs, e.g. "http://region-b:8001"
    peer_urls: str = ""
    replication_interval_seconds: float = 2.0

    # Conflict resolution strategy: lww | crdt | business
    conflict_strategy: Literal["lww", "crdt", "business"] = "lww"

    class Config:
        env_prefix = "MESH_"

    @property
    def peer_url_list(self) -> list[str]:
        if not self.peer_urls:
            return []
        return [u.strip() for u in self.peer_urls.split(",") if u.strip()]


settings = Settings()
