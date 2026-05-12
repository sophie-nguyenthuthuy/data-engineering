"""Detect expensive SQL anti-patterns across query history."""

from __future__ import annotations

import logging
from collections import defaultdict

from ..models import ExpensivePattern, Platform, QueryRecord, Severity
from ..sql_parser import detect_expensive_patterns

logger = logging.getLogger(__name__)

# Pattern metadata: (display name, description, fix, savings_pct, severity)
_PATTERN_META: dict[str, dict] = {
    "select_star": {
        "name": "SELECT * (full column scan)",
        "description": (
            "Queries using SELECT * retrieve every column, preventing column-level pruning "
            "in columnar stores. This inflates bytes scanned and cost."
        ),
        "fix": (
            "Replace SELECT * with an explicit column list. "
            "In BigQuery this directly reduces billed bytes. "
            "In Snowflake it reduces micro-partition reads."
        ),
        "savings_pct": 25,
        "severity": Severity.MEDIUM,
    },
    "cross_join": {
        "name": "CROSS JOIN (Cartesian product)",
        "description": (
            "A CROSS JOIN multiplies row counts of both tables. Even on moderate tables "
            "this can explode to billions of rows, causing massive scans and OOM."
        ),
        "fix": (
            "Replace with an INNER JOIN or LEFT JOIN on an explicit ON condition. "
            "If a broadcast/replicate is intended, use a subquery with LIMIT."
        ),
        "savings_pct": 60,
        "severity": Severity.HIGH,
    },
    "non_sargable_filter": {
        "name": "Non-sargable filter (function on filter column)",
        "description": (
            "Wrapping a column in a function inside WHERE (e.g. WHERE UPPER(col)='X') "
            "prevents partition pruning and index use, forcing a full scan."
        ),
        "fix": (
            "Move the transformation to the right-hand side: WHERE col = LOWER('X'). "
            "For date truncation use BETWEEN or >= / < instead of DATE(ts) = '2024-01-01'."
        ),
        "savings_pct": 30,
        "severity": Severity.HIGH,
    },
    "scalar_subquery_in_select": {
        "name": "Correlated scalar subquery in SELECT list",
        "description": (
            "A subquery in the SELECT list that references outer columns is re-executed "
            "once per output row, turning an O(n) query into O(n²)."
        ),
        "fix": (
            "Rewrite as a JOIN or a window function. "
            "Use a CTE to pre-aggregate the subquery result and join it in."
        ),
        "savings_pct": 40,
        "severity": Severity.HIGH,
    },
    "unnecessary_distinct": {
        "name": "DISTINCT without GROUP BY / ORDER BY",
        "description": (
            "DISTINCT forces a full sort/hash-dedup pass. When the upstream data is "
            "already unique (e.g. joined on a PK), this extra pass wastes compute."
        ),
        "fix": (
            "Verify whether duplicates can actually exist. If not, remove DISTINCT. "
            "If dedup is needed, use GROUP BY with explicit aggregations instead."
        ),
        "savings_pct": 10,
        "severity": Severity.LOW,
    },
    "order_without_limit": {
        "name": "ORDER BY without LIMIT",
        "description": (
            "Sorting millions of rows and discarding the order immediately wastes compute. "
            "This pattern is common in exploratory queries that were never cleaned up."
        ),
        "fix": (
            "Add a LIMIT clause if only the top-N rows are needed, "
            "or remove ORDER BY entirely when consuming the full result set."
        ),
        "savings_pct": 8,
        "severity": Severity.LOW,
    },
}

_MIN_QUERY_COUNT = 3  # flag a pattern only if it appears this many times


class PatternDetector:
    """Scan query records and flag expensive SQL anti-patterns."""

    def __init__(self, min_query_count: int = _MIN_QUERY_COUNT) -> None:
        self.min_query_count = min_query_count

    def detect(self, records: list[QueryRecord]) -> list[ExpensivePattern]:
        # Accumulate cost + examples per (platform, pattern)
        buckets: dict[tuple[Platform, str], dict] = defaultdict(
            lambda: {"cost": 0.0, "count": 0, "examples": []}
        )

        for rec in records:
            found = detect_expensive_patterns(rec.query_text)
            for pat in found:
                key = (rec.platform, pat)
                buckets[key]["cost"] += rec.cost_usd
                buckets[key]["count"] += 1
                if len(buckets[key]["examples"]) < 3:
                    snippet = rec.query_text[:400].strip().replace("\n", " ")
                    buckets[key]["examples"].append(snippet)

        results: list[ExpensivePattern] = []
        for (platform, pat_id), data in buckets.items():
            if data["count"] < self.min_query_count:
                continue
            meta = _PATTERN_META.get(pat_id)
            if not meta:
                continue
            results.append(
                ExpensivePattern(
                    pattern_name=meta["name"],
                    platform=platform,
                    severity=meta["severity"],
                    description=meta["description"],
                    query_count=data["count"],
                    total_cost_usd=data["cost"],
                    estimated_savings_pct=meta["savings_pct"],
                    example_queries=data["examples"],
                    fix_suggestion=meta["fix"],
                )
            )

        # Sort by estimated dollar savings descending
        results.sort(key=lambda p: p.estimated_savings_usd, reverse=True)
        return results
