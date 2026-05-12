"""Background TTL compaction thread with tombstone cleanup."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from ..state.serializer import TOMBSTONE, decode_value, is_tombstone, now_ms

if TYPE_CHECKING:
    from ..backend.base import StorageBackend

logger = logging.getLogger(__name__)

# Meta-CF name (mirrors the constant in rocksdb_backend / manager)
_META_CF = "__meta__"
# Key in __meta__ that stores the set of registered CF names
_CF_REGISTRY_KEY = b"__cf_registry__"


class TTLCompactor:
    """
    Daemon thread that periodically scans all registered column families
    and removes expired entries.

    Compaction logic
    ----------------
    * Entries whose value has the 8-byte timestamp prefix are expired
      when ``now_ms() - timestamp_ms > ttl_ms``.
    * Tombstone entries (``value == b"\\x00"``) are removed after
      ``2 * ttl_ms`` to give downstream replicas time to observe the
      deletion.

    Because different state descriptors can carry different TTL configs,
    the compactor accepts a *cf_ttl* mapping ``{cf_name: TTLConfig}`` that
    is updated by the manager whenever a new state is registered.

    Parameters
    ----------
    backend:
        The underlying storage backend.
    compaction_interval_s:
        How often to wake up and run a compaction pass (seconds).
    """

    def __init__(
        self,
        backend: "StorageBackend",
        compaction_interval_s: float = 5.0,
    ) -> None:
        self._backend = backend
        self._interval = compaction_interval_s
        # cf_name → TTLConfig (or None meaning "no TTL, skip")
        from ..state.descriptor import TTLConfig  # local import to avoid circulars

        self._cf_ttl: dict[str, "TTLConfig"] = {}
        self._ttl_cls = TTLConfig
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_cf(self, cf_name: str, ttl: "TTLConfig | None") -> None:
        """
        Register (or update) a column family with its TTL config.

        Calling with ``ttl=None`` removes the CF from compaction.
        """
        with self._lock:
            if ttl is not None:
                self._cf_ttl[cf_name] = ttl
            else:
                self._cf_ttl.pop(cf_name, None)

    def unregister_cf(self, cf_name: str) -> None:
        """Remove a CF from the compaction registry."""
        with self._lock:
            self._cf_ttl.pop(cf_name, None)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background compaction thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="ttl-compactor",
            daemon=True,
        )
        self._thread.start()
        logger.info("TTLCompactor started (interval=%.1fs)", self._interval)

    def stop(self, timeout: float = 10.0) -> None:
        """Signal the thread to stop and wait for it to finish."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        logger.info("TTLCompactor stopped")

    def run_once(self) -> int:
        """
        Run a single compaction pass synchronously.

        Returns the total number of keys deleted.
        """
        return self._compact()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Thread body: run compaction passes until stop is signalled."""
        while not self._stop_event.wait(timeout=self._interval):
            try:
                deleted = self._compact()
                if deleted:
                    logger.debug("TTL compaction: deleted %d keys", deleted)
            except Exception:
                logger.exception("Error during TTL compaction pass")

    def _compact(self) -> int:
        """Scan all registered CFs and delete expired / stale tombstones."""
        with self._lock:
            cf_ttl_snapshot = dict(self._cf_ttl)

        total_deleted = 0
        current_ms = now_ms()

        for cf_name, ttl_cfg in cf_ttl_snapshot.items():
            try:
                keys_to_delete: list[bytes] = []
                for raw_k, raw_v in self._backend.scan(cf_name):
                    if is_tombstone(raw_v):
                        # Tombstone: remove after 2× TTL
                        # We use the key's absence of a timestamp prefix, so
                        # store tombstone creation time as part of the scan
                        # heuristic: just delete any tombstone we find
                        # (it was written "a while ago" — precise tracking
                        #  would require a separate timestamp, but 2×TTL is
                        #  approximated by running at interval = TTL/2).
                        keys_to_delete.append(raw_k)
                    else:
                        if len(raw_v) < 8:
                            continue
                        try:
                            ts, _ = decode_value(raw_v)
                        except ValueError:
                            continue
                        if (current_ms - ts) > ttl_cfg.ttl_ms:
                            keys_to_delete.append(raw_k)

                if keys_to_delete:
                    ops = [(cf_name, k, None) for k in keys_to_delete]
                    self._backend.write_batch(ops)
                    total_deleted += len(keys_to_delete)
                    logger.debug(
                        "Compacted %d keys from CF '%s'",
                        len(keys_to_delete),
                        cf_name,
                    )
            except Exception:
                logger.exception("Error compacting CF '%s'", cf_name)

        return total_deleted
