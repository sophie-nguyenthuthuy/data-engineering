import time
from typing import Optional

import numpy as np

# 32-byte struct — fits two per cache line.
# Field order: all 8-byte fields first, then two 4-byte fields at the end.
EVENT_DTYPE = np.dtype([
    ("timestamp", np.int64),   # Unix nanoseconds
    ("entity_id", np.int64),   # Entity (user / account / session)
    ("value", np.float64),     # Numeric payload
    ("type_id", np.int32),     # Registered event type
    ("flags", np.uint32),      # Bitmask for categorical attributes
])

assert EVENT_DTYPE.itemsize == 32, "EVENT_DTYPE must be 32 bytes"


def make_event(
    type_id: int,
    entity_id: int,
    value: float = 0.0,
    flags: int = 0,
    timestamp: Optional[int] = None,
) -> np.void:
    ev = np.zeros(1, dtype=EVENT_DTYPE)[0]
    ev["timestamp"] = timestamp if timestamp is not None else time.time_ns()
    ev["type_id"] = type_id
    ev["entity_id"] = entity_id
    ev["value"] = value
    ev["flags"] = flags
    return ev
