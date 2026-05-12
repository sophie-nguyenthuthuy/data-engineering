"""
State handle implementations.

Each handle wraps a ``StorageBackend`` and a column-family name, providing
the typed API that operator code uses.  The column-family name is derived
from ``(operator_id, state_name)`` by the ``StateContext``.

Value encoding
--------------
All values are stored as:

    8-byte big-endian timestamp (ms)  |  msgpack payload

MapState uses per-entry RocksDB keys of the form:

    encode_key(record_key) + b"\\xff" + encode_key(map_key)

so the backend's prefix-scan can retrieve all entries for a given record
key without deserializing a potentially huge blob.
"""

from __future__ import annotations

from typing import Any, Callable, Generic, Iterable, Iterator, TypeVar

from ..backend.base import StorageBackend
from .descriptor import StateDescriptor, TTLConfig
from .serializer import (
    TOMBSTONE,
    decode_key,
    decode_map_suffix,
    decode_value,
    encode_key,
    encode_map_suffix,
    encode_value,
    is_tombstone,
    now_ms,
)

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
IN = TypeVar("IN")
ACC = TypeVar("ACC")
OUT = TypeVar("OUT")


class _BaseStateHandle:
    """Shared plumbing for all state handle types."""

    def __init__(
        self,
        backend: StorageBackend,
        cf: str,
        record_key: Any,
        descriptor: StateDescriptor,
    ) -> None:
        self._backend = backend
        self._cf = cf
        self._record_key_bytes = encode_key(record_key)
        self._descriptor = descriptor

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ttl(self) -> TTLConfig | None:
        return self._descriptor.ttl

    def _is_expired(self, timestamp_ms: int) -> bool:
        ttl = self._ttl()
        if ttl is None:
            return False
        return (now_ms() - timestamp_ms) > ttl.ttl_ms

    def _refresh_if_needed(self, raw: bytes) -> bytes:
        """Re-write with updated timestamp when *update_on_read* is set."""
        ttl = self._ttl()
        if ttl is None or not ttl.update_on_read:
            return raw
        ts, value = decode_value(raw)
        new_raw = encode_value(value)
        self._backend.put(self._cf, self._record_key_bytes, new_raw)
        return new_raw


class ValueStateHandle(_BaseStateHandle, Generic[T]):
    """
    Stores a single value per record key.

    ``get()`` returns the default from the descriptor when no value
    exists or the entry has expired.
    """

    def get(self) -> T | None:
        raw = self._backend.get(self._cf, self._record_key_bytes)
        if raw is None or is_tombstone(raw):
            return self._descriptor.default
        ts, value = decode_value(raw)
        if self._is_expired(ts):
            return self._descriptor.default
        self._refresh_if_needed(raw)
        return value  # type: ignore[return-value]

    def set(self, value: T) -> None:
        raw = encode_value(value)
        self._backend.put(self._cf, self._record_key_bytes, raw)

    def clear(self) -> None:
        self._backend.put(self._cf, self._record_key_bytes, TOMBSTONE)


class ListStateHandle(_BaseStateHandle, Generic[T]):
    """
    Stores a list of values per record key (serialized as a single blob).
    """

    def get(self) -> list[T]:
        raw = self._backend.get(self._cf, self._record_key_bytes)
        if raw is None or is_tombstone(raw):
            return []
        ts, value = decode_value(raw)
        if self._is_expired(ts):
            return []
        self._refresh_if_needed(raw)
        return value  # type: ignore[return-value]

    def add(self, value: T) -> None:
        existing = self.get()
        existing.append(value)
        self._backend.put(self._cf, self._record_key_bytes, encode_value(existing))

    def update(self, values: list[T]) -> None:
        self._backend.put(self._cf, self._record_key_bytes, encode_value(list(values)))

    def clear(self) -> None:
        self._backend.put(self._cf, self._record_key_bytes, TOMBSTONE)


class MapStateHandle(_BaseStateHandle, Generic[K, V]):
    """
    Stores a map per record key using per-entry RocksDB keys.

    Each map entry is stored as a separate backend key:

        record_key_bytes + ``\\xff`` + encode_key(map_key)

    This allows efficient prefix scans over large maps without loading
    the entire map into memory.
    """

    def _entry_key(self, map_key: K) -> bytes:
        return self._record_key_bytes + encode_map_suffix(map_key)

    def _prefix(self) -> bytes:
        return self._record_key_bytes + b"\xff"

    def get(self, key: K) -> V | None:
        raw = self._backend.get(self._cf, self._entry_key(key))
        if raw is None or is_tombstone(raw):
            return None
        ts, value = decode_value(raw)
        if self._is_expired(ts):
            return None
        if self._ttl() and self._ttl().update_on_read:  # type: ignore[union-attr]
            self._backend.put(self._cf, self._entry_key(key), encode_value(value))
        return value  # type: ignore[return-value]

    def put(self, key: K, value: V) -> None:
        self._backend.put(self._cf, self._entry_key(key), encode_value(value))

    def remove(self, key: K) -> None:
        self._backend.put(self._cf, self._entry_key(key), TOMBSTONE)

    def contains(self, key: K) -> bool:
        return self.get(key) is not None

    def keys(self) -> Iterable[K]:
        return (k for k, _ in self.items())

    def values(self) -> Iterable[V]:
        return (v for _, v in self.items())

    def items(self) -> Iterable[tuple[K, V]]:
        prefix = self._prefix()
        for raw_k, raw_v in self._backend.scan(self._cf, prefix=prefix):
            if is_tombstone(raw_v):
                continue
            try:
                ts, value = decode_value(raw_v)
            except ValueError:
                continue
            if self._is_expired(ts):
                continue
            suffix = raw_k[len(self._record_key_bytes):]
            map_key = decode_map_suffix(suffix)
            yield map_key, value

    def clear(self) -> None:
        prefix = self._prefix()
        ops: list[tuple[str, bytes, bytes | None]] = []
        for raw_k, _ in self._backend.scan(self._cf, prefix=prefix):
            ops.append((self._cf, raw_k, TOMBSTONE))
        if ops:
            self._backend.write_batch(ops)


class ReducingStateHandle(_BaseStateHandle, Generic[T]):
    """
    Stores a single aggregated value that is updated via a reduce function.

    ``add(v)`` calls ``reduce_fn(existing, v)`` and stores the result.
    """

    def __init__(
        self,
        backend: StorageBackend,
        cf: str,
        record_key: Any,
        descriptor: StateDescriptor,
    ) -> None:
        super().__init__(backend, cf, record_key, descriptor)
        if descriptor.reduce_fn is None:
            raise ValueError("ReducingStateHandle requires descriptor.reduce_fn")
        self._reduce_fn: Callable[[T, T], T] = descriptor.reduce_fn

    def get(self) -> T | None:
        raw = self._backend.get(self._cf, self._record_key_bytes)
        if raw is None or is_tombstone(raw):
            return None
        ts, value = decode_value(raw)
        if self._is_expired(ts):
            return None
        self._refresh_if_needed(raw)
        return value  # type: ignore[return-value]

    def add(self, value: T) -> None:
        existing = self.get()
        if existing is None:
            new_value = value
        else:
            new_value = self._reduce_fn(existing, value)
        self._backend.put(self._cf, self._record_key_bytes, encode_value(new_value))

    def clear(self) -> None:
        self._backend.put(self._cf, self._record_key_bytes, TOMBSTONE)


class AggregatingStateHandle(_BaseStateHandle, Generic[IN, ACC, OUT]):
    """
    Stores an accumulator that is updated via ``add_fn`` and read via ``get_fn``.

    ``add(v)`` → ``acc = add_fn(acc, v)``
    ``get()``  → ``get_fn(acc)``
    """

    def __init__(
        self,
        backend: StorageBackend,
        cf: str,
        record_key: Any,
        descriptor: StateDescriptor,
    ) -> None:
        super().__init__(backend, cf, record_key, descriptor)
        if descriptor.add_fn is None or descriptor.get_fn is None:
            raise ValueError(
                "AggregatingStateHandle requires descriptor.add_fn and descriptor.get_fn"
            )
        self._add_fn: Callable[[ACC, IN], ACC] = descriptor.add_fn
        self._get_fn: Callable[[ACC], OUT] = descriptor.get_fn
        self._initial_acc: ACC = descriptor.initial_acc

    def _load_acc(self) -> ACC:
        raw = self._backend.get(self._cf, self._record_key_bytes)
        if raw is None or is_tombstone(raw):
            return self._initial_acc
        ts, acc = decode_value(raw)
        if self._is_expired(ts):
            return self._initial_acc
        self._refresh_if_needed(raw)
        return acc  # type: ignore[return-value]

    def add(self, value: IN) -> None:
        acc = self._load_acc()
        new_acc = self._add_fn(acc, value)
        self._backend.put(self._cf, self._record_key_bytes, encode_value(new_acc))

    def get(self) -> OUT:
        return self._get_fn(self._load_acc())

    def clear(self) -> None:
        self._backend.put(self._cf, self._record_key_bytes, TOMBSTONE)
