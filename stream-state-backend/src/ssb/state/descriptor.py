"""StateDescriptor and TTLConfig definitions."""

from __future__ import annotations

import dataclasses
from typing import Any, Callable


@dataclasses.dataclass
class TTLConfig:
    """
    Time-to-live configuration attached to a ``StateDescriptor``.

    Attributes
    ----------
    ttl_ms:
        Milliseconds after which a state entry is considered expired.
    update_on_read:
        If ``True`` the timestamp is refreshed every time the entry is
        read, effectively making it an idle-timeout rather than a
        creation-time TTL.
    """

    ttl_ms: int
    update_on_read: bool = False

    def __post_init__(self) -> None:
        if self.ttl_ms <= 0:
            raise ValueError("ttl_ms must be positive")


@dataclasses.dataclass
class StateDescriptor:
    """
    Describes a named piece of operator state.

    Parameters
    ----------
    name:
        Logical name of the state (unique within an operator).
    state_type:
        One of ``"value"``, ``"list"``, ``"map"``, ``"reducing"``,
        ``"aggregating"``.
    default:
        Default value returned by ``ValueState.get()`` when no entry
        exists.  Ignored for collection state types.
    reduce_fn:
        Required for ``reducing`` state.  ``(T, T) → T``.
    add_fn:
        Required for ``aggregating`` state.  ``(ACC, IN) → ACC``.
    get_fn:
        Required for ``aggregating`` state.  ``(ACC) → OUT``.
    initial_acc:
        Initial accumulator value for ``aggregating`` state.
    ttl:
        Optional TTL configuration.
    """

    name: str
    state_type: str = "value"
    default: Any = None
    reduce_fn: Callable[[Any, Any], Any] | None = None
    add_fn: Callable[[Any, Any], Any] | None = None
    get_fn: Callable[[Any], Any] | None = None
    initial_acc: Any = None
    ttl: TTLConfig | None = None

    _VALID_TYPES = frozenset({"value", "list", "map", "reducing", "aggregating"})

    def __post_init__(self) -> None:
        if self.state_type not in self._VALID_TYPES:
            raise ValueError(
                f"Invalid state_type {self.state_type!r}. "
                f"Must be one of {sorted(self._VALID_TYPES)}"
            )
        if self.state_type == "reducing" and self.reduce_fn is None:
            raise ValueError("reduce_fn is required for reducing state")
        if self.state_type == "aggregating" and (
            self.add_fn is None or self.get_fn is None
        ):
            raise ValueError("add_fn and get_fn are required for aggregating state")
