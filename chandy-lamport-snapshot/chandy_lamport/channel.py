"""Thread-safe FIFO channel with Chandy-Lamport recording support."""
from __future__ import annotations

import threading
from queue import Empty, Queue
from typing import List, Optional

from .message import DataMessage, Marker


class Channel:
    """
    A directed FIFO channel between two nodes.

    Recording mode captures every DataMessage received on this channel
    between two points in time (start_recording / stop_recording).
    These captured messages represent the in-transit state of the channel
    at the moment of a distributed snapshot.
    """

    def __init__(self, src: str, dst: str) -> None:
        self.src = src
        self.dst = dst
        self.name = f"{src}->{dst}"

        self._q: Queue = Queue()
        self._lock = threading.Lock()
        self._recording = False
        self._recorded: List[DataMessage] = []

    # ── transport ──────────────────────────────────────────────────────────

    def send(self, msg: object) -> None:
        self._q.put(msg)

    def receive(self, timeout: float = 0.02) -> Optional[object]:
        try:
            return self._q.get(timeout=timeout)
        except Empty:
            return None

    def drain(self) -> List[object]:
        """Return and discard all queued items (used during recovery reset)."""
        items: List[object] = []
        while True:
            try:
                items.append(self._q.get_nowait())
            except Empty:
                break
        return items

    # ── recording ─────────────────────────────────────────────────────────

    def start_recording(self) -> None:
        with self._lock:
            self._recording = True
            self._recorded = []

    def record_if_needed(self, msg: DataMessage) -> None:
        """Call this immediately after receiving a DataMessage on the channel."""
        with self._lock:
            if self._recording:
                self._recorded.append(msg)

    def stop_recording(self) -> List[DataMessage]:
        """Stop recording and return captured in-transit messages."""
        with self._lock:
            self._recording = False
            captured = list(self._recorded)
            self._recorded = []
            return captured

    @property
    def is_recording(self) -> bool:
        with self._lock:
            return self._recording

    def __repr__(self) -> str:
        return f"Channel({self.name})"
