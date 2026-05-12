"""OpenTelemetry setup for the orchestrator.

Dagster doesn't ship an OTel integration out of the box, so this module
hooks the asset-execution event hook: we expose a `track_asset(context)`
context manager that opens a span + records a duration histogram keyed
by the asset key. Call it from inside each asset. Setup is idempotent
and env-driven — tests and local `dagster dev` without a Collector get
no-op instruments.

Metrics emitted:
  * `dagster.asset.duration`      histogram, seconds, attr `asset_key`
  * `dagster.asset.runs.total`    counter, attr `asset_key`, `outcome`=ok|error
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

_initialized = False
_instruments: dict[str, Any] = {}


def _endpoint() -> str | None:
    return os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")


def _service_name() -> str:
    return os.environ.get("OTEL_SERVICE_NAME", "dagster")


def setup() -> None:
    """Install OTel providers. No-op when disabled or already initialized."""
    global _initialized
    if _initialized:
        return
    endpoint = _endpoint()
    if not endpoint:
        _initialized = True
        return

    from opentelemetry import metrics, trace
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create({"service.name": _service_name()})

    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(tp)

    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=endpoint, insecure=True), export_interval_millis=10_000
    )
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))

    meter = metrics.get_meter(_service_name())
    _instruments["duration"] = meter.create_histogram(
        "dagster.asset.duration",
        unit="s",
        description="Wall time of a single asset materialization",
    )
    _instruments["runs"] = meter.create_counter(
        "dagster.asset.runs",
        description="Asset materialization outcomes",
    )
    _instruments["tracer"] = trace.get_tracer(_service_name())
    _initialized = True


@contextmanager
def track_asset(context: Any) -> Iterator[None]:
    """Wrap an asset body. Records duration + outcome + a span.

    Falls back to a plain pass-through when OTel isn't initialized or
    didn't configure (endpoint unset). Takes the Dagster `AssetExecutionContext`
    only to pull the asset key label.
    """
    setup()
    asset_key = getattr(context, "asset_key", None)
    label = asset_key.to_user_string() if asset_key is not None else "unknown"

    tracer = _instruments.get("tracer")
    if tracer is None:
        yield
        return

    t0 = time.perf_counter()
    outcome = "ok"
    with tracer.start_as_current_span(f"asset:{label}") as span:
        span.set_attribute("asset_key", label)
        try:
            yield
        except Exception:
            outcome = "error"
            raise
        finally:
            elapsed = time.perf_counter() - t0
            _instruments["duration"].record(elapsed, {"asset_key": label})
            _instruments["runs"].add(1, {"asset_key": label, "outcome": outcome})
