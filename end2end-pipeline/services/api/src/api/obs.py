"""OpenTelemetry setup for the API.

Uses the FastAPI auto-instrumentation, which populates per-request spans +
`http.server.request.duration` histograms. Enabled only when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set, so tests that exercise `main:app`
directly don't need a Collector.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from typing import Any

import structlog

_initialized = False


def _endpoint() -> str | None:
    return os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")


def _service_name() -> str:
    return os.environ.get("OTEL_SERVICE_NAME", "api")


def instrument_app(app: Any) -> None:
    global _initialized
    endpoint = _endpoint()
    if not endpoint or _initialized:
        return

    from opentelemetry import metrics, trace
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
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

    # Records `http.server.request.duration` automatically.
    FastAPIInstrumentor.instrument_app(app)

    _install_log_correlation()
    _initialized = True


def _install_log_correlation() -> None:
    from opentelemetry import trace

    def _add_trace_context(
        _logger: Any, _method: str, event: MutableMapping[str, Any]
    ) -> MutableMapping[str, Any]:
        span = trace.get_current_span()
        ctx = span.get_span_context() if span else None
        if ctx and ctx.is_valid:
            event["trace_id"] = format(ctx.trace_id, "032x")
            event["span_id"] = format(ctx.span_id, "016x")
        return event

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _add_trace_context,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
    )
