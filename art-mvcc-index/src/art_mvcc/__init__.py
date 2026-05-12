"""Adaptive Radix Tree + MVCC.

Top-level facade:
    from art_mvcc import ART, MVCCArt, Snapshot
"""

from __future__ import annotations

__version__ = "0.1.0"

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from art_mvcc.art.tree import ART
    from art_mvcc.mvcc.store import MVCCArt, Snapshot

_LAZY: dict[str, str] = {
    "ART": "art_mvcc.art.tree",
    "MVCCArt": "art_mvcc.mvcc.store",
    "Snapshot": "art_mvcc.mvcc.store",
}


def __getattr__(name: str) -> Any:
    mod = _LAZY.get(name)
    if mod is None:
        raise AttributeError(f"module 'art_mvcc' has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(mod), name)


__all__ = ["ART", "MVCCArt", "Snapshot"]
