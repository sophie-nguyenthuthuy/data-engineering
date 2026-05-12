"""Intermediate representation: types, expressions, logical & physical plans."""

from __future__ import annotations

from ppc.ir.expr import (
    BinaryOp,
    ColumnRef,
    Expr,
    Literal,
    UnaryOp,
    column,
    lit,
)
from ppc.ir.logical import (
    LogicalAggregate,
    LogicalFilter,
    LogicalJoin,
    LogicalNode,
    LogicalScan,
)
from ppc.ir.physical import PhysicalNode, PhysicalPlan
from ppc.ir.schema import Column, Schema, Stats
from ppc.ir.types import (
    BOOLEAN,
    DOUBLE,
    INT32,
    INT64,
    STRING,
    TIMESTAMP,
    DataType,
)

__all__ = [
    # types
    "DataType",
    "INT32",
    "INT64",
    "DOUBLE",
    "STRING",
    "BOOLEAN",
    "TIMESTAMP",
    # schema
    "Column",
    "Schema",
    "Stats",
    # expressions
    "Expr",
    "ColumnRef",
    "Literal",
    "BinaryOp",
    "UnaryOp",
    "column",
    "lit",
    # logical
    "LogicalNode",
    "LogicalScan",
    "LogicalFilter",
    "LogicalAggregate",
    "LogicalJoin",
    # physical
    "PhysicalNode",
    "PhysicalPlan",
]
