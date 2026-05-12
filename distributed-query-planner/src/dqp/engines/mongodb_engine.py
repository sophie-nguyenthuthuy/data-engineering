"""MongoDB engine: translates predicates to aggregation pipeline $match stages."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

from dqp.engines.base import EngineBase, EngineCapability, PushdownResult
from dqp.predicate import (
    AndPredicate,
    BetweenPredicate,
    ColumnRef,
    ComparisonOp,
    ComparisonPredicate,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    NotPredicate,
    OrPredicate,
    Predicate,
    columns_referenced,
)

# Map DQP ComparisonOp → MongoDB operator string
_OP_MAP: Dict[ComparisonOp, str] = {
    ComparisonOp.EQ: "$eq",
    ComparisonOp.NEQ: "$ne",
    ComparisonOp.LT: "$lt",
    ComparisonOp.LTE: "$lte",
    ComparisonOp.GT: "$gt",
    ComparisonOp.GTE: "$gte",
}


def converted_like_to_regex(pattern: str) -> str:
    """Convert a SQL LIKE pattern to a MongoDB-compatible regex string.

    % → .* (any sequence of characters)
    _ → .  (any single character)
    All other regex metacharacters are escaped.
    """
    result = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "%":
            result.append(".*")
        elif ch == "_":
            result.append(".")
        elif ch == "\\":
            # Escape sequence: take next character literally
            if i + 1 < len(pattern):
                result.append(re.escape(pattern[i + 1]))
                i += 1
        else:
            result.append(re.escape(ch))
        i += 1
    # Anchor to full string so semantics match SQL LIKE
    return "^" + "".join(result) + "$"


class MongoDBEngine(EngineBase):
    """Translates DQP predicates to MongoDB aggregation pipeline filters."""

    def __init__(self, db: Any = None) -> None:
        """*db* is a pymongo Database object; may be None for plan-only usage."""
        self._db = db

    @property
    def name(self) -> str:
        return "mongodb"

    @property
    def capabilities(self) -> Set[EngineCapability]:
        return {
            EngineCapability.COMPARISON,
            EngineCapability.IN,
            EngineCapability.BETWEEN,
            EngineCapability.LIKE,
            EngineCapability.IS_NULL,
            EngineCapability.AND,
            EngineCapability.OR,
            EngineCapability.NOT,
        }

    def can_push(self, pred: Predicate) -> bool:
        """MongoDB cannot push cross-table predicates (cross-collection $match)."""
        refs = columns_referenced(pred)
        tables = {r.table for r in refs if r.table is not None}
        if len(tables) > 1:
            return False
        return super().can_push(pred)

    def translate_predicate(self, pred: Predicate) -> Dict[str, Any]:
        """Translate a predicate to a MongoDB query document."""

        if isinstance(pred, ComparisonPredicate):
            col = pred.column.column
            val = pred.value.value
            if pred.op == ComparisonOp.EQ:
                # Use shorthand {field: value} for equality
                return {col: val}
            return {col: {_OP_MAP[pred.op]: val}}

        if isinstance(pred, InPredicate):
            col = pred.column.column
            vals = [lit.value for lit in pred.values]
            op = "$nin" if pred.negated else "$in"
            return {col: {op: vals}}

        if isinstance(pred, BetweenPredicate):
            col = pred.column.column
            lo = pred.low.value
            hi = pred.high.value
            if pred.negated:
                # NOT BETWEEN lo AND hi → col < lo OR col > hi
                return {"$or": [{col: {"$lt": lo}}, {col: {"$gt": hi}}]}
            return {col: {"$gte": lo, "$lte": hi}}

        if isinstance(pred, LikePredicate):
            col = pred.column.column
            regex = converted_like_to_regex(pred.pattern)
            if pred.negated:
                return {col: {"$not": re.compile(regex)}}
            return {col: {"$regex": regex}}

        if isinstance(pred, IsNullPredicate):
            col = pred.column.column
            if pred.negated:
                return {col: {"$ne": None}}
            return {col: None}

        if isinstance(pred, AndPredicate):
            clauses = [self.translate_predicate(p) for p in pred.predicates]
            return {"$and": clauses}

        if isinstance(pred, OrPredicate):
            clauses = [self.translate_predicate(p) for p in pred.predicates]
            return {"$or": clauses}

        if isinstance(pred, NotPredicate):
            inner = self.translate_predicate(pred.predicate)
            return {"$nor": [inner]}

        raise ValueError(f"Unsupported predicate type: {type(pred).__name__}")

    def build_aggregation_pipeline(
        self,
        match_stage: Dict[str, Any],
        project_stage: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Build a MongoDB aggregation pipeline from a match filter and optional projection."""
        pipeline: List[Dict[str, Any]] = [{"$match": match_stage}]
        if project_stage:
            pipeline.append({"$project": project_stage})
        return pipeline

    def execute_scan(
        self, table_name: str, pushed_result: PushdownResult, columns: List[str]
    ) -> Any:
        """Execute a find/aggregate on the MongoDB collection."""
        if self._db is None:
            raise RuntimeError("MongoDBEngine requires a db connection to execute scans")
        try:
            import pymongo  # noqa: F401
        except ImportError as exc:
            raise ImportError("pymongo is required; install with: pip install pymongo") from exc

        collection = self._db[table_name]
        match_filter = pushed_result.native_filter or {}
        projection = {col: 1 for col in columns} if columns else None
        if projection:
            projection["_id"] = 0

        pipeline = self.build_aggregation_pipeline(match_filter, projection)
        return list(collection.aggregate(pipeline))
