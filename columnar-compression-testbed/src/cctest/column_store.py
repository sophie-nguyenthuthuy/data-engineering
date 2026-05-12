"""Simple in-memory columnar store with adaptive codec selection.

Usage
-----
    store = ColumnStore()
    store.insert({"price": prices, "symbol": symbols, "ts": timestamps})
    df = store.retrieve()  # returns dict[str, np.ndarray]

On every ``insert``, the SchemaEvolutionTracker checks whether the incoming
columns represent a schema change; affected columns have their codec choices
re-evaluated automatically.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np

from .codecs import Codec, EncodedColumn
from .schema import SchemaEvolutionTracker
from .selector import EncodingSelector, SelectorConfig

logger = logging.getLogger(__name__)


class ColumnStore:
    def __init__(
        self,
        selector: Optional[EncodingSelector] = None,
        config: Optional[SelectorConfig] = None,
    ) -> None:
        self._selector = selector or EncodingSelector(config=config)
        self._tracker = SchemaEvolutionTracker()
        self._store: dict[str, list[EncodedColumn]] = {}
        self._codec_used: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def insert(self, columns: dict[str, np.ndarray]) -> None:
        """Compress and store *columns*.  Schema changes trigger re-evaluation."""
        diff = self._tracker.observe(columns, selector=self._selector)
        if diff.has_changes:
            logger.info("Schema diff:\n%s", diff)

        for name, arr in columns.items():
            codec = self._selector.select(name, arr)
            encoded = codec.encode(arr)
            self._store.setdefault(name, []).append(encoded)
            self._codec_used[name] = codec.name

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def retrieve(self, columns: Optional[list[str]] = None) -> dict[str, np.ndarray]:
        """Decompress and concatenate all stored chunks for each column."""
        names = columns if columns is not None else list(self._store.keys())
        result: dict[str, np.ndarray] = {}
        for name in names:
            chunks = self._store.get(name, [])
            if not chunks:
                continue
            codec = self._selector.select.__self__  # noqa: not ideal – look up via name
            decoded_chunks = []
            for chunk in chunks:
                from .codecs import ALL_CODECS
                codec_obj = next((c for c in ALL_CODECS if c.name == chunk.codec_name), None)
                if codec_obj is None:
                    raise KeyError(f"Unknown codec {chunk.codec_name!r} in stored chunk")
                decoded_chunks.append(codec_obj.decode(chunk))
            result[name] = np.concatenate(decoded_chunks)
        return result

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def codec_summary(self) -> dict[str, str]:
        return dict(self._codec_used)

    def storage_bytes(self) -> dict[str, int]:
        return {
            name: sum(c.total_bytes() for c in chunks)
            for name, chunks in self._store.items()
        }

    def original_bytes(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for name, chunks in self._store.items():
            total = sum(c.original_len * _dtype_itemsize(c.original_dtype) for c in chunks)
            result[name] = total
        return result

    def compression_summary(self) -> dict[str, dict]:
        sb = self.storage_bytes()
        ob = self.original_bytes()
        summary = {}
        for name in sb:
            ratio = ob[name] / sb[name] if sb[name] else 0
            summary[name] = {
                "codec": self._codec_used.get(name, "?"),
                "original_bytes": ob[name],
                "compressed_bytes": sb[name],
                "ratio": round(ratio, 3),
            }
        return summary

    def schema(self) -> dict[str, str]:
        return self._tracker.current


def _dtype_itemsize(dtype_str: str) -> int:
    try:
        return np.dtype(dtype_str).itemsize
    except Exception:
        return 8
