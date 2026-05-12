from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .clock import HybridLogicalClock, WallClock
from .store import MetadataStore
from .timestamp import HLCTimestamp


@dataclass
class ReplicationMessage:
    source_node: str
    key: str
    value: Any
    send_ts: HLCTimestamp
    send_wall_ms: int


@dataclass
class CausalEvent:
    """Records a (event, cause) pair for anomaly detection."""
    event_id: str
    node_id: str
    ts: HLCTimestamp
    wall_ms_at_event: int
    caused_by: str | None = None   # event_id of the causal predecessor


class Region:
    """
    Simulates a single node in a multi-region pipeline.

    Each region has:
      - Its own HLC (or wall clock for the baseline).
      - A MetadataStore.
      - Configurable clock drift and network latency.
      - An event log for post-hoc anomaly detection.
    """

    def __init__(
        self,
        name: str,
        drift_ms: int = 0,
        network_latency_ms: int = 0,
        use_hlc: bool = True,
    ) -> None:
        self.name = name
        self._network_latency_ms = network_latency_ms
        self._use_hlc = use_hlc

        if use_hlc:
            self._clock: HybridLogicalClock | WallClock = HybridLogicalClock(
                node_id=name, drift_ms=drift_ms
            )
        else:
            self._clock = WallClock(node_id=name, drift_ms=drift_ms)

        self.store = MetadataStore(self._clock)
        self._events: list[CausalEvent] = []
        self._lock = threading.Lock()
        self._event_counter = 0

    # ------------------------------------------------------------------
    # Clock drift control
    # ------------------------------------------------------------------

    @property
    def drift_ms(self) -> int:
        return self._clock.drift_ms

    @drift_ms.setter
    def drift_ms(self, value: int) -> None:
        self._clock.drift_ms = value

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    def write(self, key: str, value: Any, caused_by_event: str | None = None) -> str:
        """Write key=value, return the event ID."""
        ts = self.store.put(key, value)
        event_id = self._new_event_id()
        wall_ms = int(time.time() * 1000)
        with self._lock:
            self._events.append(
                CausalEvent(
                    event_id=event_id,
                    node_id=self.name,
                    ts=ts,
                    wall_ms_at_event=wall_ms,
                    caused_by=caused_by_event,
                )
            )
        return event_id

    def replicate_to(self, target: Region, key: str, caused_by_event: str) -> str:
        """
        Send a replication message from this region to *target*.

        Simulates network latency.  The target records the receive event
        as causally following *caused_by_event*.
        """
        result = self.store.get(key)
        if result is None:
            raise KeyError(key)
        value, send_ts = result

        msg = ReplicationMessage(
            source_node=self.name,
            key=key,
            value=value,
            send_ts=send_ts,
            send_wall_ms=int(time.time() * 1000),
        )

        if self._network_latency_ms:
            time.sleep(self._network_latency_ms / 1000)

        return target._receive_replication(msg, caused_by_event)

    def _receive_replication(self, msg: ReplicationMessage, caused_by: str) -> str:
        ts = self.store.put(msg.key, msg.value, remote_ts=msg.send_ts)
        event_id = self._new_event_id()
        wall_ms = int(time.time() * 1000)
        with self._lock:
            self._events.append(
                CausalEvent(
                    event_id=event_id,
                    node_id=self.name,
                    ts=ts,
                    wall_ms_at_event=wall_ms,
                    caused_by=caused_by,
                )
            )
        return event_id

    def events(self) -> list[CausalEvent]:
        with self._lock:
            return list(self._events)

    def _new_event_id(self) -> str:
        with self._lock:
            self._event_counter += 1
            return f"{self.name}:{self._event_counter}"
