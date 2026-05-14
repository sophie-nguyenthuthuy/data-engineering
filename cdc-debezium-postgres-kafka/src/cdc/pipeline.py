"""End-to-end CDC event pipeline.

Composes :class:`DLQRouter` → list[:class:`Transform`] over a stream of
raw payloads. Returns a :class:`PipelineResult` summarising counts of
clean, dlq, and transformed events.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cdc.dlq.router import DLQDecision, DLQRouter

if TYPE_CHECKING:
    from collections.abc import Iterable

    from cdc.events.envelope import DebeziumEnvelope
    from cdc.transforms.base import Transform


@dataclass
class PipelineResult:
    """Aggregate output of one :meth:`Pipeline.run` invocation."""

    clean: list[DebeziumEnvelope] = field(default_factory=list)
    dlq: list[DLQDecision] = field(default_factory=list)
    transform_failures: list[tuple[DebeziumEnvelope, str]] = field(default_factory=list)

    def total(self) -> int:
        return len(self.clean) + len(self.dlq) + len(self.transform_failures)

    def success_rate(self) -> float:
        t = self.total()
        return len(self.clean) / t if t else 1.0


@dataclass
class Pipeline:
    """Compose a DLQ router with a chain of transforms."""

    router: DLQRouter = field(default_factory=DLQRouter)
    transforms: list[Transform] = field(default_factory=list)

    def run(self, payloads: Iterable[bytes | str | dict[str, Any]]) -> PipelineResult:
        result = PipelineResult()
        for payload in payloads:
            decision = self.router.route(payload)
            if isinstance(decision, DLQDecision):
                result.dlq.append(decision)
                continue
            envelope: DebeziumEnvelope = decision
            try:
                for t in self.transforms:
                    envelope = t.apply(envelope)
            except Exception as exc:
                result.transform_failures.append((envelope, f"{type(exc).__name__}: {exc}"))
                continue
            result.clean.append(envelope)
        return result


__all__ = ["Pipeline", "PipelineResult"]
