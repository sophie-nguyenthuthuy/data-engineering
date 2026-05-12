"""
stream-state-backend (ssb)
~~~~~~~~~~~~~~~~~~~~~~~~~~
Persistent, queryable state backend for stream processors.
"""

from .manager import StateBackendManager
from .state.descriptor import StateDescriptor, TTLConfig
from .topology.descriptor import OperatorDescriptor, TopologyDescriptor

__all__ = [
    "StateBackendManager",
    "StateDescriptor",
    "TTLConfig",
    "OperatorDescriptor",
    "TopologyDescriptor",
]
