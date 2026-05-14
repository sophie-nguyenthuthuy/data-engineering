"""Consumer-group offset store.

Pure in-memory + JSONL-persisted offsets, keyed by ``(group, topic)``.
The semantics mirror Kafka's ``__consumer_offsets``: a consumer
commits the *next* offset it intends to read, so resuming reads the
record at the committed offset.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class OffsetStore:
    """Persistent (group, topic) → next-offset map."""

    path: Path | None = None
    _state: dict[tuple[str, str], int] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def __post_init__(self) -> None:
        if self.path is not None:
            if not isinstance(self.path, Path):
                self.path = Path(self.path)
            self._hydrate()

    # ---------------------------------------------------------------- API

    def commit(self, *, group: str, topic: str, next_offset: int) -> None:
        if not group or not topic:
            raise ValueError("group and topic must be non-empty")
        if next_offset < 0:
            raise ValueError("next_offset must be ≥ 0")
        with self._lock:
            self._state[(group, topic)] = next_offset
            self._flush()

    def get(self, *, group: str, topic: str, default: int = 0) -> int:
        if not group or not topic:
            raise ValueError("group and topic must be non-empty")
        with self._lock:
            return self._state.get((group, topic), default)

    def all(self) -> dict[tuple[str, str], int]:
        with self._lock:
            return dict(self._state)

    # ----------------------------------------------------------- persist

    def _flush(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Snapshot the entire state on every commit — small + simple +
        # crash-consistent because we write to a sibling .tmp and replace.
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for (group, topic), offset in sorted(self._state.items()):
                fh.write(json.dumps({"group": group, "topic": topic, "offset": offset}))
                fh.write("\n")
        tmp.replace(self.path)

    def _hydrate(self) -> None:
        assert self.path is not None
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                self._state[(obj["group"], obj["topic"])] = int(obj["offset"])


__all__ = ["OffsetStore"]
