"""StateBackendManager — main entry point for stream-state-backend."""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, Callable

import msgpack

from .backend.base import StorageBackend
from .backend.memory_backend import MemoryBackend
from .state.descriptor import StateDescriptor, TTLConfig
from .state.handle import (
    AggregatingStateHandle,
    ListStateHandle,
    MapStateHandle,
    ReducingStateHandle,
    ValueStateHandle,
)
from .topology.descriptor import OperatorDescriptor, TopologyDescriptor
from .topology.migrator import MigrationTask, TopologyMigrator, _cf_name
from .ttl.compactor import TTLCompactor

logger = logging.getLogger(__name__)

_META_CF = "__meta__"
_META_TOPOLOGY_KEY = b"__topology__"
_META_CF_REGISTRY_KEY = b"__cf_registry__"


class StateContext:
    """
    A scoped view of state for a single ``(operator_id, record_key)``
    pair.

    Operator code calls ``get_*_state()`` methods to obtain handles.
    State descriptors are registered in the backend and TTL compactor
    transparently.
    """

    def __init__(
        self,
        manager: "StateBackendManager",
        operator_id: str,
        record_key: Any,
    ) -> None:
        self._manager = manager
        self._operator_id = operator_id
        self._record_key = record_key

    # ------------------------------------------------------------------
    # State handle factories
    # ------------------------------------------------------------------

    def get_value_state(
        self,
        name: str,
        default: Any = None,
        ttl: TTLConfig | None = None,
    ) -> ValueStateHandle:
        desc = StateDescriptor(
            name=name, state_type="value", default=default, ttl=ttl
        )
        cf = self._manager._ensure_state(self._operator_id, desc)
        return ValueStateHandle(self._manager.backend, cf, self._record_key, desc)

    def get_list_state(
        self,
        name: str,
        ttl: TTLConfig | None = None,
    ) -> ListStateHandle:
        desc = StateDescriptor(name=name, state_type="list", ttl=ttl)
        cf = self._manager._ensure_state(self._operator_id, desc)
        return ListStateHandle(self._manager.backend, cf, self._record_key, desc)

    def get_map_state(
        self,
        name: str,
        ttl: TTLConfig | None = None,
    ) -> MapStateHandle:
        desc = StateDescriptor(name=name, state_type="map", ttl=ttl)
        cf = self._manager._ensure_state(self._operator_id, desc)
        return MapStateHandle(self._manager.backend, cf, self._record_key, desc)

    def get_reducing_state(
        self,
        name: str,
        reduce_fn: Callable[[Any, Any], Any],
        ttl: TTLConfig | None = None,
    ) -> ReducingStateHandle:
        desc = StateDescriptor(
            name=name, state_type="reducing", reduce_fn=reduce_fn, ttl=ttl
        )
        cf = self._manager._ensure_state(self._operator_id, desc)
        return ReducingStateHandle(self._manager.backend, cf, self._record_key, desc)

    def get_aggregating_state(
        self,
        name: str,
        add_fn: Callable[[Any, Any], Any],
        get_fn: Callable[[Any], Any],
        initial_acc: Any = None,
        ttl: TTLConfig | None = None,
    ) -> AggregatingStateHandle:
        desc = StateDescriptor(
            name=name,
            state_type="aggregating",
            add_fn=add_fn,
            get_fn=get_fn,
            initial_acc=initial_acc,
            ttl=ttl,
        )
        cf = self._manager._ensure_state(self._operator_id, desc)
        return AggregatingStateHandle(
            self._manager.backend, cf, self._record_key, desc
        )


class StateBackendManager:
    """
    Central coordinator for all stream operator state.

    Usage
    -----
    ::

        mgr = StateBackendManager("/tmp/mydb", backend="rocksdb")
        mgr.start()

        ctx = mgr.get_state_context("word_count", "hello")
        cnt = ctx.get_value_state("count", default=0)
        cnt.set(cnt.get() + 1)

        task = await mgr.update_topology(new_topo)
        await task.wait()

        mgr.stop()
    """

    def __init__(
        self,
        db_path: str = "/tmp/ssb_default",
        backend: str = "memory",
        compaction_interval_s: float = 5.0,
        api_host: str = "127.0.0.1",
        api_port: int = 8765,
    ) -> None:
        self._db_path = db_path
        self._backend_type = backend
        self._compaction_interval_s = compaction_interval_s
        self._api_host = api_host
        self._api_port = api_port

        self._backend: StorageBackend = self._create_backend()
        self._compactor: TTLCompactor = TTLCompactor(
            self._backend, compaction_interval_s
        )
        self._topology: TopologyDescriptor | None = None
        self._migrator: TopologyMigrator | None = None

        # operator_id → set of state_names registered
        self._registered_states: dict[str, set[str]] = {}
        self._lock = threading.RLock()

        # FastAPI app / uvicorn server (started on request)
        self._api_app = None
        self._api_server = None
        self._api_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, run_api: bool = False) -> None:
        """
        Open the backend, start the compactor, optionally start API server.

        Parameters
        ----------
        run_api:
            If ``True``, start the FastAPI server in a background thread.
        """
        self._backend.open()
        self._backend.create_cf(_META_CF)
        self._compactor.start()
        self._restore_topology()

        if run_api:
            self._start_api()

        logger.info("StateBackendManager started (backend=%s)", self._backend_type)

    def stop(self) -> None:
        """Stop the compactor, API server, and close the backend."""
        self._compactor.stop()
        self._stop_api()
        self._backend.close()
        logger.info("StateBackendManager stopped")

    # ------------------------------------------------------------------
    # State context
    # ------------------------------------------------------------------

    def get_state_context(self, operator_id: str, record_key: Any) -> StateContext:
        """Return a ``StateContext`` scoped to *(operator_id, record_key)*."""
        return StateContext(self, operator_id, record_key)

    # ------------------------------------------------------------------
    # Topology management
    # ------------------------------------------------------------------

    async def update_topology(
        self,
        new_topo: TopologyDescriptor,
        key_mapper: Callable[[bytes], bytes] | None = None,
    ) -> MigrationTask:
        """
        Update the current topology to *new_topo*.

        Returns a ``MigrationTask`` that can be awaited.  The migration
        runs asynchronously; old CFs are dropped only after it succeeds.

        Raises ``ValueError`` if *new_topo.version* is not strictly
        greater than the current version.
        """
        with self._lock:
            old_topo = self._topology or TopologyDescriptor(version=0, operators={})
            if new_topo.version <= old_topo.version:
                raise ValueError(
                    f"New topology version {new_topo.version} must be greater than "
                    f"current version {old_topo.version}"
                )

            # Ensure CFs exist for all operators in the new topology
            for op_id, op in new_topo.operators.items():
                for sname in op.state_names:
                    self._backend.create_cf(_cf_name(op_id, sname))

            # Initialise migrator lazily
            if self._migrator is None:
                self._migrator = TopologyMigrator(self._backend)
                self._migrator.start()

            task = await self._migrator.submit(old_topo, new_topo, key_mapper)

            # Update topology pointer eagerly; the task runs in background
            self._topology = new_topo
            self._persist_topology(new_topo)

        return task

    def set_topology(self, topo: TopologyDescriptor) -> None:
        """
        Synchronously set the topology (no migration).

        Useful for initial setup or testing.
        """
        with self._lock:
            for op_id, op in topo.operators.items():
                for sname in op.state_names:
                    self._backend.create_cf(_cf_name(op_id, sname))
            self._topology = topo
            self._persist_topology(topo)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def backend(self) -> StorageBackend:
        return self._backend

    @property
    def current_topology(self) -> TopologyDescriptor | None:
        return self._topology

    @property
    def migrator(self) -> TopologyMigrator | None:
        return self._migrator

    @property
    def compactor(self) -> TTLCompactor:
        return self._compactor

    # ------------------------------------------------------------------
    # Internal: state registration
    # ------------------------------------------------------------------

    def _ensure_state(self, operator_id: str, desc: StateDescriptor) -> str:
        """
        Ensure the column family for *(operator_id, desc.name)* exists and
        the state is registered in the compactor.

        Returns the CF name.
        """
        cf = _cf_name(operator_id, desc.name)
        with self._lock:
            needs_create = cf not in (self._backend.list_cfs())
            needs_register = (
                operator_id not in self._registered_states
                or desc.name not in self._registered_states.get(operator_id, set())
            )

        if needs_create:
            self._backend.create_cf(cf)

        if needs_register:
            with self._lock:
                self._registered_states.setdefault(operator_id, set()).add(desc.name)
            self._compactor.register_cf(cf, desc.ttl)
            self._update_topology_with_state(operator_id, desc.name)

        elif desc.ttl is not None:
            # Update TTL config in compactor even if already registered
            self._compactor.register_cf(cf, desc.ttl)

        return cf

    def _update_topology_with_state(
        self, operator_id: str, state_name: str
    ) -> None:
        """Add *state_name* to *operator_id* in the current topology."""
        with self._lock:
            if self._topology is None:
                op = OperatorDescriptor(
                    operator_id=operator_id, state_names=[state_name]
                )
                self._topology = TopologyDescriptor(
                    version=1, operators={operator_id: op}
                )
            else:
                op = self._topology.operators.get(operator_id)
                if op is None:
                    op = OperatorDescriptor(
                        operator_id=operator_id, state_names=[state_name]
                    )
                    self._topology.operators[operator_id] = op
                elif state_name not in op.state_names:
                    op.state_names.append(state_name)
            self._persist_topology(self._topology)

    # ------------------------------------------------------------------
    # Internal: topology persistence
    # ------------------------------------------------------------------

    def _persist_topology(self, topo: TopologyDescriptor) -> None:
        """Write topology to the __meta__ CF."""
        payload = msgpack.packb(topo.to_dict(), use_bin_type=True)
        self._backend.put(_META_CF, _META_TOPOLOGY_KEY, payload)

    def _restore_topology(self) -> None:
        """Reload topology from the __meta__ CF on startup."""
        raw = self._backend.get(_META_CF, _META_TOPOLOGY_KEY)
        if raw is not None:
            try:
                data = msgpack.unpackb(raw, raw=False)
                self._topology = TopologyDescriptor.from_dict(data)
                logger.info(
                    "Restored topology version %d", self._topology.version
                )
            except Exception:
                logger.exception("Failed to restore topology; starting fresh")

    # ------------------------------------------------------------------
    # Internal: backend factory
    # ------------------------------------------------------------------

    def _create_backend(self) -> StorageBackend:
        if self._backend_type == "rocksdb":
            try:
                from .backend.rocksdb_backend import RocksDBBackend

                return RocksDBBackend(self._db_path)
            except ImportError:
                logger.warning(
                    "python-rocksdb not available; falling back to MemoryBackend"
                )
                return MemoryBackend()
        return MemoryBackend()

    # ------------------------------------------------------------------
    # Internal: API server
    # ------------------------------------------------------------------

    def _start_api(self) -> None:
        from .api.server import create_app

        self._api_app = create_app(self)
        try:
            import uvicorn

            config = uvicorn.Config(
                self._api_app,
                host=self._api_host,
                port=self._api_port,
                log_level="warning",
            )
            server = uvicorn.Server(config)
            self._api_server = server

            def _run_server() -> None:
                asyncio.run(server.serve())

            self._api_thread = threading.Thread(
                target=_run_server,
                name="ssb-api",
                daemon=True,
            )
            self._api_thread.start()
            logger.info(
                "API server started on http://%s:%d", self._api_host, self._api_port
            )
        except ImportError:
            logger.warning("uvicorn not installed; API server not started")

    def _stop_api(self) -> None:
        if self._api_server is not None:
            self._api_server.should_exit = True
        if self._api_thread is not None:
            self._api_thread.join(timeout=5)
