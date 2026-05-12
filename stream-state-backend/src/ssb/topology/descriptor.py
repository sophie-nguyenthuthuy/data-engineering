"""Topology and operator descriptors with version tracking."""

from __future__ import annotations

import dataclasses
from typing import Any


@dataclasses.dataclass
class OperatorDescriptor:
    """
    Describes a single streaming operator.

    Attributes
    ----------
    operator_id:
        Unique string identifier (e.g. ``"word_count"``).
    state_names:
        Ordered list of state names owned by this operator.
    parallelism:
        Number of parallel instances.  Changing parallelism is treated
        as a topology change and triggers key redistribution migration.
    metadata:
        Optional free-form dict for user annotations.
    """

    operator_id: str
    state_names: list[str] = dataclasses.field(default_factory=list)
    parallelism: int = 1
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OperatorDescriptor):
            return NotImplemented
        return (
            self.operator_id == other.operator_id
            and sorted(self.state_names) == sorted(other.state_names)
            and self.parallelism == other.parallelism
        )

    def __hash__(self) -> int:
        return hash((self.operator_id, tuple(sorted(self.state_names)), self.parallelism))

    def has_changed(self, other: "OperatorDescriptor") -> bool:
        """Return True if *other* is a modified version of *self*."""
        return self != other


@dataclasses.dataclass
class TopologyDescriptor:
    """
    Describes the full operator topology at a given version.

    Attributes
    ----------
    version:
        Monotonically increasing integer.  A new topology must have a
        strictly greater version than the current one.
    operators:
        Mapping from ``operator_id`` to ``OperatorDescriptor``.
    """

    version: int
    operators: dict[str, OperatorDescriptor] = dataclasses.field(default_factory=dict)

    def diff(
        self, new: "TopologyDescriptor"
    ) -> tuple[
        list[str],  # added operator_ids
        list[str],  # removed operator_ids
        list[str],  # changed operator_ids
    ]:
        """
        Compare *self* (old) with *new* and return three lists:

        * added   — operator IDs present in *new* but not *self*
        * removed — operator IDs present in *self* but not *new*
        * changed — operator IDs present in both but with different descriptors
        """
        old_ids = set(self.operators)
        new_ids = set(new.operators)

        added = sorted(new_ids - old_ids)
        removed = sorted(old_ids - new_ids)
        changed = sorted(
            oid
            for oid in old_ids & new_ids
            if self.operators[oid].has_changed(new.operators[oid])
        )
        return added, removed, changed

    def to_dict(self) -> dict:
        """Serialize to a JSON-compatible dict."""
        return {
            "version": self.version,
            "operators": {
                oid: {
                    "operator_id": op.operator_id,
                    "state_names": op.state_names,
                    "parallelism": op.parallelism,
                    "metadata": op.metadata,
                }
                for oid, op in self.operators.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TopologyDescriptor":
        """Deserialize from a dict produced by :meth:`to_dict`."""
        operators = {
            oid: OperatorDescriptor(
                operator_id=od["operator_id"],
                state_names=od.get("state_names", []),
                parallelism=od.get("parallelism", 1),
                metadata=od.get("metadata", {}),
            )
            for oid, od in data.get("operators", {}).items()
        }
        return cls(version=data["version"], operators=operators)
