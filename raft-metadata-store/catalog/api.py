"""
Catalog API — thin layer over the distributed KV store.

Key schema:
  datasets/{name}                   → Dataset JSON
  tables/{dataset}/{table}          → Table JSON
  lineage/{source}/{target}/{job}   → DataLineage JSON
  tags/by-tag/{tag}/{full_name}     → "" (index for tag search)
"""

import json
import logging
from typing import Any, Dict, List, Optional

from .models import Column, DataLineage, Dataset, Table

logger = logging.getLogger(__name__)


class CatalogAPI:
    """
    High-level catalog operations backed by a distributed KV store.
    All writes are serialized through Raft; reads served from local state.
    """

    def __init__(self, raft_node, kv_store) -> None:
        self._raft = raft_node
        self._kv = kv_store

    # ── Dataset CRUD ──────────────────────────────────────────────────────

    async def create_dataset(self, dataset: Dataset) -> Dataset:
        key = f"datasets/{dataset.name}"
        await self._put(key, dataset.to_dict())
        return dataset

    async def get_dataset(self, name: str) -> Optional[Dataset]:
        vv = await self._kv.get(f"datasets/{name}")
        if vv is None:
            return None
        return Dataset.from_dict(vv.value)

    async def list_datasets(self) -> List[Dataset]:
        pairs = await self._kv.list_prefix("datasets/")
        return [Dataset.from_dict(v.value) for _, v in pairs]

    async def update_dataset(self, dataset: Dataset) -> Dataset:
        import time
        dataset.updated_at = time.time()
        key = f"datasets/{dataset.name}"
        vv = await self._kv.get(key)
        ver = vv.version if vv else None
        await self._put(key, dataset.to_dict(), version=ver)
        return dataset

    async def delete_dataset(self, name: str) -> None:
        # Delete dataset and all its tables
        tables = await self.list_tables(name)
        for t in tables:
            await self.delete_table(name, t.name)
        await self._delete(f"datasets/{name}")

    # ── Table CRUD ────────────────────────────────────────────────────────

    async def create_table(self, table: Table) -> Table:
        key = f"tables/{table.dataset_name}/{table.name}"
        await self._put(key, table.to_dict())
        for tag in table.tags:
            await self._put(f"tags/by-tag/{tag}/{table.full_name}", "")
        return table

    async def get_table(self, dataset: str, name: str) -> Optional[Table]:
        vv = await self._kv.get(f"tables/{dataset}/{name}")
        if vv is None:
            return None
        return Table.from_dict(vv.value)

    async def list_tables(self, dataset: str) -> List[Table]:
        pairs = await self._kv.list_prefix(f"tables/{dataset}/")
        return [Table.from_dict(v.value) for _, v in pairs]

    async def update_table(self, table: Table) -> Table:
        import time
        table.updated_at = time.time()
        key = f"tables/{table.dataset_name}/{table.name}"
        vv = await self._kv.get(key)
        ver = vv.version if vv else None
        await self._put(key, table.to_dict(), version=ver)
        return table

    async def delete_table(self, dataset: str, name: str) -> None:
        table = await self.get_table(dataset, name)
        if table:
            for tag in table.tags:
                await self._delete(f"tags/by-tag/{tag}/{table.full_name}")
        await self._delete(f"tables/{dataset}/{name}")

    async def add_column(self, dataset: str, table_name: str, col: Column) -> Table:
        table = await self.get_table(dataset, table_name)
        if table is None:
            raise KeyError(f"table {dataset}.{table_name} not found")
        table.columns.append(col)
        return await self.update_table(table)

    # ── Lineage ───────────────────────────────────────────────────────────

    async def add_lineage(self, lineage: DataLineage) -> DataLineage:
        key = f"lineage/{lineage.source}/{lineage.target}/{lineage.job}"
        await self._put(key, lineage.to_dict())
        return lineage

    async def get_lineage_upstream(self, table_full_name: str) -> List[DataLineage]:
        """All edges where this table is the target."""
        pairs = await self._kv.list_glob(f"lineage/*/{table_full_name}/*")
        return [DataLineage.from_dict(v.value) for _, v in pairs]

    async def get_lineage_downstream(self, table_full_name: str) -> List[DataLineage]:
        """All edges where this table is the source."""
        pairs = await self._kv.list_prefix(f"lineage/{table_full_name}/")
        return [DataLineage.from_dict(v.value) for _, v in pairs]

    async def get_lineage_impact(self, table_full_name: str, depth: int = 5) -> List[str]:
        """BFS downstream impact analysis."""
        visited = set()
        queue = [table_full_name]
        result = []
        for _ in range(depth):
            if not queue:
                break
            next_queue = []
            for node in queue:
                if node in visited:
                    continue
                visited.add(node)
                downstream = await self.get_lineage_downstream(node)
                for edge in downstream:
                    if edge.target not in visited:
                        result.append(edge.target)
                        next_queue.append(edge.target)
            queue = next_queue
        return result

    # ── Tag search ────────────────────────────────────────────────────────

    async def find_by_tag(self, tag: str) -> List[str]:
        pairs = await self._kv.list_prefix(f"tags/by-tag/{tag}/")
        prefix = f"tags/by-tag/{tag}/"
        return [k[len(prefix):] for k, _ in pairs]

    # ── Private helpers ───────────────────────────────────────────────────

    async def _put(
        self, key: str, value: Any, version: Optional[int] = None
    ) -> None:
        cmd: Dict[str, Any] = {"op": "put", "key": key, "value": value}
        if version is not None:
            cmd["version"] = version
        await self._raft.submit(cmd)

    async def _delete(self, key: str) -> None:
        await self._raft.submit({"op": "delete", "key": key})
