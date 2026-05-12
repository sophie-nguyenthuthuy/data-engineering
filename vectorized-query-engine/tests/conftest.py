import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
import pyarrow as pa
from vqe import Engine


@pytest.fixture
def small_engine():
    engine = Engine()
    engine.register_dict("t", {
        "id":    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "name":  ["alice", "bob", "carol", "dave", "eve",
                  "frank", "grace", "heidi", "ivan", "judy"],
        "score": [85.0, 92.0, 78.0, 95.0, 60.0, 88.0, 71.0, 99.0, 55.0, 84.0],
        "dept":  ["eng", "eng", "hr", "eng", "hr", "mkt", "mkt", "eng", "hr", "mkt"],
    })
    engine.register_dict("u", {
        "uid":  [1, 2, 3, 4, 5],
        "tid":  [1, 3, 5, 7, 9],
        "role": ["admin", "user", "user", "admin", "guest"],
    })
    return engine
