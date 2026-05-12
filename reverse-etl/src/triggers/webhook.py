import hashlib
import hmac
from typing import Any
from fastapi import FastAPI, HTTPException, Header, Request
from ..models import PipelineConfig, RunResult
from ..pipeline import run_pipeline
from ..settings import settings
from ..logger import get_logger

logger = get_logger(__name__)


def _verify_signature(body: bytes, signature: str | None) -> bool:
    if not signature:
        return False
    expected = hmac.new(settings.webhook_secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)


def create_webhook_app(pipelines: list[PipelineConfig]) -> FastAPI:
    app = FastAPI(title="Reverse ETL Webhook API", version="1.0.0")
    pipeline_map: dict[str, PipelineConfig] = {p.name: p for p in pipelines}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/pipelines")
    async def list_pipelines() -> list[dict[str, Any]]:
        return [
            {"name": p.name, "enabled": p.enabled, "description": p.description}
            for p in pipelines
        ]

    @app.post("/trigger/{pipeline_name}", response_model=RunResult)
    async def trigger_pipeline(
        pipeline_name: str,
        request: Request,
        x_hub_signature_256: str | None = Header(default=None),
    ) -> RunResult:
        body = await request.body()

        # Signature validation (optional — skip if secret is default placeholder)
        if settings.webhook_secret != "changeme":
            if not _verify_signature(body, x_hub_signature_256):
                raise HTTPException(status_code=401, detail="Invalid webhook signature")

        config = pipeline_map.get(pipeline_name)
        if not config:
            raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_name}' not found")
        if not config.enabled:
            raise HTTPException(status_code=400, detail=f"Pipeline '{pipeline_name}' is disabled")

        logger.info(f"Webhook trigger: {pipeline_name}")
        return run_pipeline(config)

    return app
