"""Physical operators per engine + cross-engine conversions + cost knobs."""

from __future__ import annotations

from ppc.engines.base import EngineOp, EngineProfile, ENGINE_PROFILES
from ppc.engines.conversions import ConversionRegistry, default_conversion_registry
from ppc.engines.physical_ops import (
    PhysicalAggregate,
    PhysicalConversion,
    PhysicalFilter,
    PhysicalHashJoin,
    PhysicalScan,
)

__all__ = [
    "EngineOp",
    "EngineProfile",
    "ENGINE_PROFILES",
    "ConversionRegistry",
    "default_conversion_registry",
    "PhysicalScan",
    "PhysicalFilter",
    "PhysicalAggregate",
    "PhysicalHashJoin",
    "PhysicalConversion",
]
