"""
MLOps Data Layer — main entry point.

Usage:
    python main.py                         # API + background drift monitor
    python main.py --mode api              # API only
    python main.py --mode monitor          # Drift monitor + consumer only
    python main.py --model my_model --version v2
"""
from __future__ import annotations
import argparse
import asyncio
import signal
import structlog
import uvicorn

from src.config import settings
from src.api.app import create_app

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
        if settings.log_format == "console"
        else structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(__import__("logging"), settings.log_level)
    ),
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger(__name__)


async def main(model_name: str, model_version: str) -> None:
    app = create_app(model_name=model_name, model_version=model_version)

    config = uvicorn.Config(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    log.info(
        "mlops_data_layer_starting",
        model=model_name,
        version=model_version,
        host=settings.api_host,
        port=settings.api_port,
    )

    serve_task = asyncio.create_task(server.serve())
    await asyncio.wait(
        [serve_task, asyncio.create_task(stop_event.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )
    server.should_exit = True
    await serve_task
    log.info("mlops_data_layer_stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MLOps Data Layer")
    parser.add_argument("--model", default="fraud_detection", help="Model name")
    parser.add_argument("--version", default="v1", help="Model version")
    args = parser.parse_args()
    asyncio.run(main(args.model, args.version))
