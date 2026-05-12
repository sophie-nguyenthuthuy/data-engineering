"""Physical operators per engine + cross-engine conversions + cost knobs."""

from __future__ import annotations

from ppc.engines.base import ENGINE_PROFILES, EngineOp, EngineProfile
from ppc.engines.conversions import ConversionRegistry, default_conversion_registry
from ppc.engines.physical_ops import (
    PhysicalAggregate,
    PhysicalConversion,
    PhysicalFilter,
    PhysicalHashJoin,
    PhysicalScan,
)

__all__ = [
    "ENGINE_PROFILES",
    "ConversionRegistry",
    "EngineOp",
    "EngineProfile",
    "PhysicalAggregate",
    "PhysicalConversion",
    "PhysicalFilter",
    "PhysicalHashJoin",
    "PhysicalScan",
    "default_conversion_registry",
]
