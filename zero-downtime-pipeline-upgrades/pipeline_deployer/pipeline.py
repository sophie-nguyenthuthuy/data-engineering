"""
Abstract base class every pipeline version must implement.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class BasePipeline(ABC):
    """
    Minimal contract for a versioned data pipeline.

    A pipeline is a stateful, record-by-record processor.  Each call to
    ``process`` may read and update internal state (e.g. running aggregates,
    ML model context, lookup caches).

    Implementations must be thread-safe if the shadow runner is used with
    concurrent record dispatch.
    """

    @property
    @abstractmethod
    def version(self) -> str:
        """Human-readable version tag, e.g. ``'v1'`` or ``'v2.3.1'``."""

    @abstractmethod
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single record and return the pipeline's output.

        Args:
            record: Arbitrary key/value input record.

        Returns:
            Arbitrary key/value output record.  The shadow runner will diff
            this against the other version's output.
        """

    def setup(self) -> None:
        """
        Called once before any records are processed.
        Override to open connections, load models, warm caches, etc.
        """

    def teardown(self) -> None:
        """
        Called once after processing is complete.
        Override to flush state, close connections, etc.
        """

    def snapshot_state(self) -> Dict[str, Any]:
        """
        Return a serialisable snapshot of internal state.
        Used by the orchestrator for checkpointing before traffic shifts.
        Override if the pipeline carries meaningful state.
        """
        return {}

    def restore_state(self, snapshot: Dict[str, Any]) -> None:
        """
        Restore internal state from a snapshot produced by ``snapshot_state``.
        """
