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
    "BOOLEAN",
    "DOUBLE",
    "INT32",
    "INT64",
    "STRING",
    "TIMESTAMP",
    "BinaryOp",
    # schema
    "Column",
    "ColumnRef",
    # types
    "DataType",
    # expressions
    "Expr",
    "Literal",
    "LogicalAggregate",
    "LogicalFilter",
    "LogicalJoin",
    # logical
    "LogicalNode",
    "LogicalScan",
    # physical
    "PhysicalNode",
    "PhysicalPlan",
    "Schema",
    "Stats",
    "UnaryOp",
    "column",
    "lit",
]
