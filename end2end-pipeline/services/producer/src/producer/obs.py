"""OpenTelemetry setup for the producer.

Idempotent and env-driven. If `OTEL_EXPORTER_OTLP_ENDPOINT` is unset we
install no-op providers so unit tests and bare `python -m producer` runs
work without a Collector. When the Collector is reachable the producer
emits:

* Traces — one span per `produce()` call, linked to messages via the
  standard W3C trace-context headers injected as Kafka message headers.
* Metrics — `producer_events_sent_total` (Counter), `producer_delivery_failed_total`
  (Counter).
* Logs — structlog output is augmented with `trace_id` / `span_id` so
  Loki entries link back to the originating span.
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
    return os.environ.get("OTEL_SERVICE_NAME", "producer")


def setup() -> dict[str, Any]:
    """Install OTel providers + return the metric instruments the app uses.

    Returns a dict with `events_sent` and `delivery_failed` Counters. If OTel
    is disabled they're no-op counters with a compatible `.add()` signature.
    """
    global _initialized
    endpoint = _endpoint()
    if not endpoint or _initialized:
        # Still return instruments so call sites don't need to branch.
        meter = _get_meter()
        _initialized = True
        return {
            "events_sent": meter.create_counter(
                "producer.events.sent", description="Events successfully enqueued"
            ),
            "delivery_failed": meter.create_counter(
                "producer.delivery.failed", description="Kafka delivery failures"
            ),
        }

    # Lazy imports: only pay the cost when observability is turned on.
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

    _install_log_correlation()
    _initialized = True

    meter = metrics.get_meter(_service_name())
    return {
        "events_sent": meter.create_counter(
            "producer.events.sent", description="Events successfully enqueued"
        ),
        "delivery_failed": meter.create_counter(
            "producer.delivery.failed", description="Kafka delivery failures"
        ),
    }


def _get_meter() -> Any:
    """Return a meter whether OTel is configured or not."""
    try:
        from opentelemetry import metrics

        return metrics.get_meter(_service_name())
    except Exception:

        class _NoOpCounter:
            def add(self, *_args: Any, **_kwargs: Any) -> None:
                return None

        class _NoOpMeter:
            def create_counter(self, *_args: Any, **_kwargs: Any) -> _NoOpCounter:
                return _NoOpCounter()

        return _NoOpMeter()


def _install_log_correlation() -> None:
    """Inject `trace_id` / `span_id` from the active span into structlog."""
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

    # Only mutate the config if structlog has already been initialized.
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _add_trace_context,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
    )
