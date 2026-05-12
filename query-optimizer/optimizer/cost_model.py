"""
Cost model for physical operators.

Costs are measured in abstract "work units":
  I/O cost  = pages read/written   (weight: 1.0)
  CPU cost  = rows processed        (weight: CPU_FACTOR)
  Memory    = spill penalty when build side exceeds buffer pool

Reference: Selinger et al. (System R), Graefe (Volcano/Cascades).

Operator costs
--------------
SeqScan         : pages(table)
HashJoin        : build side I/O + probe side I/O + output
MergeJoin       : sort both sides (if not already sorted) + merge pass
NestedLoopJoin  : outer rows * inner pages  (good only for tiny outer)
"""
from __future__ import annotations
from dataclasses import dataclass

# Tuning constants
PAGE_SIZE_BYTES = 8_192          # 8 KB pages
CPU_FACTOR = 0.001               # CPU work per row vs. one I/O
BUFFER_POOL_PAGES = 1_024        # pages available for hash/sort (8 MB)
SORT_COST_FACTOR = 2.0           # merge-sort ≈ 2 * n log n passes


@dataclass
class CostEstimate:
    io_cost: float = 0.0
    cpu_cost: float = 0.0

    @property
    def total(self) -> float:
        return self.io_cost + self.cpu_cost

    def __add__(self, other: "CostEstimate") -> "CostEstimate":
        return CostEstimate(self.io_cost + other.io_cost, self.cpu_cost + other.cpu_cost)

    def __repr__(self) -> str:
        return f"Cost(io={self.io_cost:,.0f}, cpu={self.cpu_cost:,.2f}, total={self.total:,.2f})"


class CostModel:
    def __init__(self, avg_row_bytes: dict[str, int] | None = None) -> None:
        # per-table average row sizes; fallback to 100 bytes
        self._row_bytes: dict[str, int] = avg_row_bytes or {}

    def _pages(self, rows: float, table: str = "") -> float:
        row_bytes = self._row_bytes.get(table, 100)
        return max(1.0, rows * row_bytes / PAGE_SIZE_BYTES)

    # ------------------------------------------------------------------
    # Leaf operators
    # ------------------------------------------------------------------

    def seq_scan(self, table: str, rows: float) -> CostEstimate:
        pages = self._pages(rows, table)
        return CostEstimate(io_cost=pages, cpu_cost=rows * CPU_FACTOR)

    # ------------------------------------------------------------------
    # Join operators
    # ------------------------------------------------------------------

    def hash_join(
        self,
        build_rows: float,
        probe_rows: float,
        output_rows: float,
        build_table: str = "",
        probe_table: str = "",
    ) -> CostEstimate:
        """
        Classic hash join:
          Phase 1 (build): read build side, build hash table.
          Phase 2 (probe): read probe side, probe hash table.
          If build side > buffer pool → Grace hash join with partition spill.
        """
        build_pages = self._pages(build_rows, build_table)
        probe_pages = self._pages(probe_rows, probe_table)

        io = build_pages + probe_pages

        # Spill penalty: when build side overflows buffer pool we do 3 passes
        if build_pages > BUFFER_POOL_PAGES:
            spill_factor = build_pages / BUFFER_POOL_PAGES
            io += 2 * (build_pages + probe_pages) * spill_factor * 0.1  # partial spill

        cpu = (build_rows + probe_rows + output_rows) * CPU_FACTOR
        return CostEstimate(io_cost=io, cpu_cost=cpu)

    def merge_join(
        self,
        left_rows: float,
        right_rows: float,
        output_rows: float,
        left_sorted: bool = False,
        right_sorted: bool = False,
        left_table: str = "",
        right_table: str = "",
    ) -> CostEstimate:
        """
        Sort-merge join.
        If input is already sorted on the join key, skip the sort phase.
        Sort cost: external merge sort ≈ 2 * N * log2(N/B) passes.
        """
        import math

        left_pages = self._pages(left_rows, left_table)
        right_pages = self._pages(right_rows, right_table)

        def sort_cost(pages: float) -> float:
            if pages <= 1:
                return 0.0
            passes = math.ceil(math.log2(max(pages / BUFFER_POOL_PAGES, 1.0) + 1))
            return SORT_COST_FACTOR * pages * max(1, passes)

        io = (0.0 if left_sorted else sort_cost(left_pages)) + \
             (0.0 if right_sorted else sort_cost(right_pages)) + \
             left_pages + right_pages  # merge pass

        cpu = (left_rows + right_rows + output_rows) * CPU_FACTOR
        return CostEstimate(io_cost=io, cpu_cost=cpu)

    def nested_loop_join(
        self,
        outer_rows: float,
        inner_rows: float,
        output_rows: float,
        outer_table: str = "",
        inner_table: str = "",
    ) -> CostEstimate:
        """
        Block nested loop join.
        Cost: scan outer once + for each outer block scan inner.
        With a buffer pool the inner is re-read outer_pages / B times.
        """
        outer_pages = self._pages(outer_rows, outer_table)
        inner_pages = self._pages(inner_rows, inner_table)
        blocks = max(1.0, outer_pages / BUFFER_POOL_PAGES)
        io = outer_pages + blocks * inner_pages
        cpu = (outer_rows * inner_rows + output_rows) * CPU_FACTOR
        return CostEstimate(io_cost=io, cpu_cost=cpu)
