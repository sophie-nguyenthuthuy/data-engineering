"""Engine adapter tests."""

from __future__ import annotations

import pytest

from pvc.engines.base import EngineError
from pvc.engines.injectable import InjectableEngine
from pvc.engines.sqlite import SQLiteEngine

# ----------------------------------------------------------- SQLite


def test_sqlite_setup_and_query():
    eng = SQLiteEngine()
    eng.setup(
        ddl=["CREATE TABLE t (id INTEGER, v INTEGER)"],
        inserts=[("INSERT INTO t (id, v) VALUES (?, ?)", [(1, 10), (2, 20)])],
    )
    rows = eng.execute("SELECT SUM(v) FROM t")
    assert rows == [(30,)]
    eng.close()


def test_sqlite_setup_twice_rejected():
    eng = SQLiteEngine()
    eng.setup(ddl=[], inserts=[])
    with pytest.raises(EngineError):
        eng.setup(ddl=[], inserts=[])
    eng.close()


def test_sqlite_execute_before_setup_rejected():
    eng = SQLiteEngine()
    with pytest.raises(EngineError):
        eng.execute("SELECT 1")


def test_sqlite_query_error_surfaces_as_engine_error():
    eng = SQLiteEngine()
    eng.setup(ddl=[], inserts=[])
    with pytest.raises(EngineError):
        eng.execute("SELECT * FROM ghost_table")
    eng.close()


def test_sqlite_close_is_idempotent():
    eng = SQLiteEngine()
    eng.setup(ddl=[], inserts=[])
    eng.close()
    eng.close()  # should not raise


# --------------------------------------------------------- Injectable


def test_injectable_routes_setup_inserts_and_queries():
    seen: list[str] = []

    def runner(sql: str):
        seen.append(sql)
        if sql.startswith("SELECT"):
            return [(42,)]
        return []

    eng = InjectableEngine(execute_fn=runner)
    eng.setup(
        ddl=["CREATE TABLE x (id INT)"],
        inserts=[("INSERT INTO x (id) VALUES (?)", [(1,), (2,)])],
    )
    rows = eng.execute("SELECT * FROM x")
    assert rows == [(42,)]
    # 1 DDL + 2 INSERTs + 1 SELECT
    assert len(seen) == 4
    assert seen[0].startswith("CREATE TABLE")
    assert "VALUES (1)" in seen[1]
    assert "VALUES (2)" in seen[2]


def test_injectable_execute_before_setup_rejected():
    eng = InjectableEngine(execute_fn=lambda _s: [])
    with pytest.raises(EngineError):
        eng.execute("SELECT 1")


def test_injectable_separate_ddl_runner_when_provided():
    ddl_seen: list[str] = []
    data_seen: list[str] = []

    def ddl_run(sql: str):
        ddl_seen.append(sql)
        return []

    def data_run(sql: str):
        data_seen.append(sql)
        return [("ok",)]

    eng = InjectableEngine(execute_fn=data_run, ddl_runner=ddl_run)
    eng.setup(ddl=["CREATE TABLE t (id INT)"], inserts=[])
    eng.execute("SELECT 1")
    assert ddl_seen == ["CREATE TABLE t (id INT)"]
    assert data_seen == ["SELECT 1"]


def test_injectable_close_runs_closer():
    closed = {"n": 0}
    eng = InjectableEngine(
        execute_fn=lambda _s: [], closer=lambda: closed.__setitem__("n", closed["n"] + 1)
    )
    eng.close()
    assert closed["n"] == 1
