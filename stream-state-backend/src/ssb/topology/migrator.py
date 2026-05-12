"""Async topology migration tasks."""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from ..backend.base import StorageBackend
    from .descriptor import OperatorDescriptor, TopologyDescriptor

logger = logging.getLogger(__name__)


class MigrationStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class MigrationTask:
    """
    Represents one topology-version migration.

    Attributes
    ----------
    old_topology:
        The topology before migration.
    new_topology:
        The target topology.
    status:
        Current ``MigrationStatus``.
    progress:
        ``(migrated_keys, total_keys)`` — updated during migration.
    error:
        Exception if migration failed; ``None`` otherwise.
    """

    def __init__(
        self,
        old_topology: "TopologyDescriptor",
        new_topology: "TopologyDescriptor",
        backend: "StorageBackend",
        key_mapper: Callable[[bytes], bytes] | None = None,
    ) -> None:
        self.old_topology = old_topology
        self.new_topology = new_topology
        self._backend = backend
        self._key_mapper = key_mapper or (lambda k: k)
        self.status: MigrationStatus = MigrationStatus.PENDING
        self.progress: tuple[int, int] = (0, 0)
        self.error: Exception | None = None
        self._task: asyncio.Task | None = None
        self.started_at: float | None = None
        self.completed_at: float | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, loop: asyncio.AbstractEventLoop | None = None) -> asyncio.Task:
        """Schedule the migration on *loop* (or the running loop)."""
        if self._task is not None:
            return self._task
        target_loop = loop or asyncio.get_event_loop()
        self._task = target_loop.create_task(self._run())
        return self._task

    async def wait(self) -> None:
        """Await until migration completes (or fails)."""
        if self._task is not None:
            await self._task

    def to_dict(self) -> dict:
        return {
            "old_version": self.old_topology.version,
            "new_version": self.new_topology.version,
            "status": self.status.value,
            "progress": {"migrated": self.progress[0], "total": self.progress[1]},
            "error": str(self.error) if self.error else None,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }

    # ------------------------------------------------------------------
    # Migration logic
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        self.status = MigrationStatus.RUNNING
        self.started_at = time.time()
        try:
            await asyncio.get_event_loop().run_in_executor(None, self._sync_migrate)
            self.status = MigrationStatus.COMPLETED
        except Exception as exc:
            self.status = MigrationStatus.FAILED
            self.error = exc
            logger.exception(
                "Migration v%d→v%d failed",
                self.old_topology.version,
                self.new_topology.version,
            )
        finally:
            self.completed_at = time.time()

    def _sync_migrate(self) -> None:
        """
        Blocking migration called in a thread-pool executor.

        For each changed/added operator:
        1. Collect all CF names from the old topology.
        2. Copy each key from old CF(s) to new CF(s) via the key mapper.
        3. After all copies succeed, drop the old CFs.
        """
        added, removed, changed = self.old_topology.diff(self.new_topology)
        logger.info(
            "Migration v%d→v%d: added=%s removed=%s changed=%s",
            self.old_topology.version,
            self.new_topology.version,
            added,
            removed,
            changed,
        )

        # Gather all (old_cf, new_cf) pairs to migrate
        pairs: list[tuple[str, str]] = []

        for op_id in changed:
            old_op = self.old_topology.operators[op_id]
            new_op = self.new_topology.operators[op_id]
            # Migrate state names that exist in both; copy new ones as empty
            for sname in old_op.state_names:
                old_cf = _cf_name(op_id, sname)
                new_cf = _cf_name(op_id, sname)
                if old_cf != new_cf:
                    pairs.append((old_cf, new_cf))

        # Ensure new CFs exist
        for _, new_cf in pairs:
            self._backend.create_cf(new_cf)

        # For added operators, ensure their CFs are created
        for op_id in added:
            new_op = self.new_topology.operators[op_id]
            for sname in new_op.state_names:
                self._backend.create_cf(_cf_name(op_id, sname))

        # Count total keys for progress tracking
        total = 0
        for old_cf, _ in pairs:
            for _ in self._backend.scan(old_cf):
                total += 1
        self.progress = (0, total)

        migrated = 0
        old_cfs_to_drop: list[str] = []

        for old_cf, new_cf in pairs:
            batch: list[tuple[str, bytes, bytes | None]] = []
            for raw_k, raw_v in self._backend.scan(old_cf):
                new_k = self._key_mapper(raw_k)
                batch.append((new_cf, new_k, raw_v))
                migrated += 1
                self.progress = (migrated, total)

                # Flush in chunks to avoid huge batches
                if len(batch) >= 500:
                    self._backend.write_batch(batch)
                    batch = []

            if batch:
                self._backend.write_batch(batch)

            if old_cf != new_cf:
                old_cfs_to_drop.append(old_cf)

        # Remove operators that no longer exist
        for op_id in removed:
            old_op = self.old_topology.operators[op_id]
            for sname in old_op.state_names:
                old_cfs_to_drop.append(_cf_name(op_id, sname))

        # Drop old CFs only after migration completes successfully
        for cf in old_cfs_to_drop:
            try:
                self._backend.drop_cf(cf)
            except Exception:
                logger.warning("Could not drop CF '%s'", cf, exc_info=True)

        self.progress = (migrated, total)
        logger.info(
            "Migration v%d→v%d complete: %d keys migrated",
            self.old_topology.version,
            self.new_topology.version,
            migrated,
        )


def _cf_name(operator_id: str, state_name: str) -> str:
    """Canonical column-family name for an (operator_id, state_name) pair."""
    return f"{operator_id}::{state_name}"


class TopologyMigrator:
    """
    Queues and runs topology migrations serially.

    Migrations are submitted via :meth:`submit` and run one at a time
    so that concurrent updates don't interleave.
    """

    def __init__(self, backend: "StorageBackend") -> None:
        self._backend = backend
        self._queue: asyncio.Queue[MigrationTask] = asyncio.Queue()
        self._history: list[MigrationTask] = []
        self._active: MigrationTask | None = None
        self._worker_task: asyncio.Task | None = None

    def start(self) -> None:
        """Start the serial migration worker."""
        loop = asyncio.get_event_loop()
        self._worker_task = loop.create_task(self._worker())

    async def stop(self) -> None:
        """Stop the worker after draining the queue."""
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

    async def submit(
        self,
        old_topology: "TopologyDescriptor",
        new_topology: "TopologyDescriptor",
        key_mapper: Callable[[bytes], bytes] | None = None,
    ) -> MigrationTask:
        """Enqueue a migration and return the ``MigrationTask``."""
        task = MigrationTask(old_topology, new_topology, self._backend, key_mapper)
        self._history.append(task)
        await self._queue.put(task)
        return task

    @property
    def active(self) -> MigrationTask | None:
        return self._active

    @property
    def history(self) -> list[MigrationTask]:
        return list(self._history)

    async def _worker(self) -> None:
        while True:
            task = await self._queue.get()
            self._active = task
            try:
                await task._run()
            except Exception:
                logger.exception("Unhandled error in migration worker")
            finally:
                self._active = None
                self._queue.task_done()
