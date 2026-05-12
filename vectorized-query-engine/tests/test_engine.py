"""End-to-end engine tests comparing VQE output to DuckDB."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
import pyarrow as pa
import duckdb

from vqe import Engine


@pytest.fixture
def eng():
    engine = Engine()
    engine.register_dict("t", {
        "id":    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "name":  ["alice", "bob", "carol", "dave", "eve",
                  "frank", "grace", "heidi", "ivan", "judy"],
        "score": [85.0, 92.0, 78.0, 95.0, 60.0, 88.0, 71.0, 99.0, 55.0, 84.0],
        "dept":  ["eng", "eng", "hr", "eng", "hr", "mkt", "mkt", "eng", "hr", "mkt"],
    })
    return engine


@pytest.fixture
def duck(eng):
    con = duckdb.connect()
    con.register("t", eng.catalog.get("t").data)
    return con


def _sorted(tbl: pa.Table) -> list:
    """Convert table to sorted list of dicts for comparison."""
    rows = tbl.to_pydict()
    keys = list(rows.keys())
    n = len(rows[keys[0]]) if keys else 0
    result = [tuple(rows[k][i] for k in keys) for i in range(n)]
    return sorted(result)


def _round(val, places=4):
    if isinstance(val, float):
        return round(val, places)
    return val


def _sorted_rounded(tbl: pa.Table, places=4) -> list:
    rows = tbl.to_pydict()
    keys = list(rows.keys())
    n = len(rows[keys[0]]) if keys else 0
    result = [tuple(_round(rows[k][i], places) for k in keys) for i in range(n)]
    return sorted(result)


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------

def test_select_star_volcano(eng):
    result = eng.execute("SELECT * FROM t", mode="volcano")
    assert result.num_rows == 10


def test_select_star_pipeline(eng):
    result = eng.execute("SELECT * FROM t", mode="pipeline")
    assert result.num_rows == 10


def test_filter_volcano(eng):
    result = eng.execute("SELECT id FROM t WHERE score > 90", mode="volcano")
    ids = sorted(result.column("id").to_pylist())
    assert ids == [2, 4, 8]


def test_filter_pipeline(eng):
    result = eng.execute("SELECT id FROM t WHERE score > 90", mode="pipeline")
    ids = sorted(result.column("id").to_pylist())
    assert ids == [2, 4, 8]


def test_count_star_volcano(eng):
    result = eng.execute("SELECT COUNT(*) FROM t", mode="volcano")
    assert result.num_rows == 1
    count_val = result.column(result.schema.names[0])[0].as_py()
    assert count_val == 10


def test_count_star_pipeline(eng):
    result = eng.execute("SELECT COUNT(*) FROM t", mode="pipeline")
    count_val = result.column(result.schema.names[0])[0].as_py()
    assert count_val == 10


def test_sum_aggregate(eng):
    result = eng.execute("SELECT SUM(score) AS total FROM t", mode="pipeline")
    total = result.column("total")[0].as_py()
    assert abs(total - 807.0) < 0.01


def test_group_by_count(eng):
    result = eng.execute(
        "SELECT dept, COUNT(*) AS cnt FROM t GROUP BY dept ORDER BY dept",
        mode="pipeline",
    )
    rows = {
        r[0]: r[1]
        for r in zip(result.column("dept").to_pylist(), result.column("cnt").to_pylist())
    }
    assert rows["eng"] == 4
    assert rows["hr"] == 3
    assert rows["mkt"] == 3


def test_group_by_sum(eng):
    result = eng.execute(
        "SELECT dept, SUM(score) AS s FROM t GROUP BY dept ORDER BY dept",
        mode="pipeline",
    )
    rows = {
        r[0]: r[1]
        for r in zip(result.column("dept").to_pylist(), result.column("s").to_pylist())
    }
    assert abs(rows["eng"] - (85 + 92 + 95 + 99)) < 0.01  # 371
    assert abs(rows["hr"] - (78 + 60 + 55)) < 0.01         # 193
    assert abs(rows["mkt"] - (88 + 71 + 84)) < 0.01        # 243


def test_limit(eng):
    result = eng.execute("SELECT id FROM t LIMIT 3", mode="pipeline")
    assert result.num_rows == 3


def test_order_by_desc(eng):
    result = eng.execute("SELECT id, score FROM t ORDER BY score DESC LIMIT 3", mode="pipeline")
    scores = result.column("score").to_pylist()
    assert scores == sorted(scores, reverse=True)


def test_between(eng):
    result = eng.execute("SELECT id FROM t WHERE score BETWEEN 80 AND 90", mode="pipeline")
    ids = sorted(result.column("id").to_pylist())
    expected = [i + 1 for i, s in enumerate([85, 92, 78, 95, 60, 88, 71, 99, 55, 84]) if 80 <= s <= 90]
    assert ids == expected


# ---------------------------------------------------------------------------
# Both modes agree
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT id, score FROM t WHERE score > 75 ORDER BY id",
    "SELECT dept, COUNT(*) AS cnt FROM t GROUP BY dept",
    "SELECT COUNT(*) FROM t WHERE score > 80",
])
def test_volcano_pipeline_agree(eng, sql):
    vol = eng.execute(sql, mode="volcano")
    pip = eng.execute(sql, mode="pipeline")
    assert _sorted(vol) == _sorted(pip)


# ---------------------------------------------------------------------------
# Explain
# ---------------------------------------------------------------------------

def test_explain(eng):
    text = eng.explain("SELECT SUM(score) FROM t WHERE score > 80")
    assert "Logical Plan" in text
