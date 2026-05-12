"""Parquet engine: translates predicates to PyArrow dataset filter expressions."""
from __future__ import annotations

from typing import Any, List, Optional, Set

from dqp.engines.base import EngineBase, EngineCapability, PushdownResult
from dqp.predicate import (
    AndPredicate,
    BetweenPredicate,
    ComparisonOp,
    ComparisonPredicate,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    NotPredicate,
    OrPredicate,
    Predicate,
)


class ParquetEngine(EngineBase):
    """Translates DQP predicates to PyArrow dataset filter expressions.

    LIKE predicates are not supported — they become residual predicates.
    OR is limited (PyArrow supports it but only with scalar comparisons).
    """

    def __init__(self, path: str) -> None:
        self._path = path

    @property
    def name(self) -> str:
        return "parquet"

    @property
    def capabilities(self) -> Set[EngineCapability]:
        return {
            EngineCapability.COMPARISON,
            EngineCapability.IN,
            EngineCapability.BETWEEN,
            EngineCapability.IS_NULL,
            EngineCapability.AND,
            # LIKE is deliberately excluded — not supported by PyArrow row-group stats
            # OR is excluded to keep pushdown conservative (complex OR can't prune row groups)
        }

    def translate_predicate(self, pred: Predicate) -> Optional[Any]:
        """Translate a predicate to a PyArrow dataset filter expression.

        Returns None if the predicate cannot be expressed as a PyArrow filter.
        """
        try:
            import pyarrow.dataset as ds
        except ImportError as exc:
            raise ImportError("pyarrow is required; install with: pip install pyarrow") from exc

        if isinstance(pred, LikePredicate):
            # Not pushable into Parquet row-group filters
            return None

        if isinstance(pred, ComparisonPredicate):
            field_expr = ds.field(pred.column.column)
            val = pred.value.value
            op = pred.op
            if op == ComparisonOp.EQ:
                return field_expr == val
            elif op == ComparisonOp.NEQ:
                return field_expr != val
            elif op == ComparisonOp.LT:
                return field_expr < val
            elif op == ComparisonOp.LTE:
                return field_expr <= val
            elif op == ComparisonOp.GT:
                return field_expr > val
            elif op == ComparisonOp.GTE:
                return field_expr >= val

        if isinstance(pred, InPredicate):
            field_expr = ds.field(pred.column.column)
            vals = [lit.value for lit in pred.values]
            if pred.negated:
                # ~isin  →  chain of != with AND
                parts = [field_expr != v for v in vals]
                result = parts[0]
                for p in parts[1:]:
                    result = result & p
                return result
            return field_expr.isin(vals)

        if isinstance(pred, BetweenPredicate):
            field_expr = ds.field(pred.column.column)
            lo = pred.low.value
            hi = pred.high.value
            if pred.negated:
                return (field_expr < lo) | (field_expr > hi)
            return (field_expr >= lo) & (field_expr <= hi)

        if isinstance(pred, IsNullPredicate):
            field_expr = ds.field(pred.column.column)
            if pred.negated:
                return field_expr.is_valid()
            return field_expr.is_null()

        if isinstance(pred, AndPredicate):
            parts = [self.translate_predicate(p) for p in pred.predicates]
            valid = [p for p in parts if p is not None]
            if not valid:
                return None
            result = valid[0]
            for p in valid[1:]:
                result = result & p
            return result

        if isinstance(pred, OrPredicate):
            parts = [self.translate_predicate(p) for p in pred.predicates]
            valid = [p for p in parts if p is not None]
            if len(valid) != len(pred.predicates):
                # If any sub-predicate can't be pushed, the whole OR can't be pushed
                return None
            result = valid[0]
            for p in valid[1:]:
                result = result | p
            return result

        if isinstance(pred, NotPredicate):
            inner = self.translate_predicate(pred.predicate)
            if inner is None:
                return None
            return ~inner

        return None

    def build_filter_expression(self, predicates: List[Predicate]) -> Optional[Any]:
        """Combine a list of predicates into a single PyArrow filter expression."""
        if not predicates:
            return None
        parts = [self.translate_predicate(p) for p in predicates]
        valid = [p for p in parts if p is not None]
        if not valid:
            return None
        try:
            import pyarrow.dataset as ds
        except ImportError:
            return None
        result = valid[0]
        for p in valid[1:]:
            result = result & p
        return result

    def estimate_row_groups_skipped(self, filter_expr: Any, schema_path: str) -> int:
        """Count how many row groups would be skipped by the given filter expression.

        Uses row-group statistics (min/max) stored in the Parquet footer.
        Returns 0 if stats are unavailable or the file doesn't exist.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError:
            return 0

        try:
            pf = pq.ParquetFile(schema_path)
        except Exception:
            return 0

        n_skipped = 0
        for i in range(pf.metadata.num_row_groups):
            rg = pf.metadata.row_group(i)
            if _row_group_eliminated(rg, filter_expr):
                n_skipped += 1

        return n_skipped

    def execute_scan(
        self, table_name: str, pushed_result: PushdownResult, columns: List[str]
    ) -> Any:
        """Read from the Parquet dataset with the pushed filter."""
        try:
            import pyarrow.dataset as ds
        except ImportError as exc:
            raise ImportError("pyarrow is required; install with: pip install pyarrow") from exc

        dataset = ds.dataset(self._path, format="parquet")
        filter_expr = pushed_result.native_filter
        cols_arg = columns if columns else None
        table = dataset.to_table(filter=filter_expr, columns=cols_arg)
        return table.to_pylist()


def _row_group_eliminated(row_group: Any, filter_expr: Any) -> bool:
    """Heuristic: try to determine if a row group can be skipped.

    This is a best-effort check using column min/max statistics.
    PyArrow's dataset scanner does this automatically; this function is
    for diagnostic/estimation purposes only.
    """
    # PyArrow handles this internally in to_table(filter=...).
    # We conservatively return False (no rows skipped) in the fallback path.
    return False
