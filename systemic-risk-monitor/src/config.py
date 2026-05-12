from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    memgraph_host: str = Field(default="localhost", alias="MEMGRAPH_HOST")
    memgraph_port: int = Field(default=7687, alias="MEMGRAPH_PORT")

    num_institutions: int = Field(default=20, alias="NUM_INSTITUTIONS")
    transaction_interval_ms: int = Field(default=200, alias="TRANSACTION_INTERVAL_MS")

    # Risk thresholds
    cycle_alert_min_length: int = 3          # alert on cycles of length >= 3
    concentration_hhi_threshold: float = 0.25  # HHI > 0.25 → concentrated market
    betweenness_threshold: float = 0.35       # node betweenness > 35% → systemic node
    liquidity_shock_pct: float = 0.30         # simulate 30% shock for contagion
    contagion_cascade_threshold: float = 0.50 # institution fails if >50% of in-flow lost

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    model_config = {"populate_by_name": True, "env_file": ".env"}


settings = Settings()
