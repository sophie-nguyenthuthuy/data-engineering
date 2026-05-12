from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="IC_", env_file=".env", extra="ignore")

    data_dir: Path = Field(default=Path("./data"))
    db_path: Path = Field(default=Path("./data/inflation.duckdb"))

    cc_index_bucket: str = "commoncrawl"
    cc_s3_endpoint: str = "https://data.commoncrawl.org"

    bls_api_key: str | None = None
    anthropic_api_key: str | None = None

    fetch_concurrency: int = 16
    fetch_timeout: float = 30.0
    user_agent: str = "inflation-crawler/0.1 (+https://example.com/bot)"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def extracted_dir(self) -> Path:
        return self.data_dir / "extracted"

    def ensure_dirs(self) -> None:
        for d in (self.data_dir, self.raw_dir, self.extracted_dir):
            d.mkdir(parents=True, exist_ok=True)


settings = Settings()
