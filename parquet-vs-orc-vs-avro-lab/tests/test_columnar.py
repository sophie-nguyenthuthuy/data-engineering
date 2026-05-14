"""Column + Schema tests."""

from __future__ import annotations

import pytest

from pova.columnar.column import Column, ColumnType
from pova.columnar.schema import Schema


def test_column_rejects_empty_name():
    with pytest.raises(ValueError):
        Column(name="", type=ColumnType.INT64, values=(1,))


def test_column_rejects_type_mismatch():
    with pytest.raises(TypeError):
        Column(name="c", type=ColumnType.INT64, values=("not int",))


def test_column_int64_rejects_bool():
    with pytest.raises(TypeError):
        Column(name="c", type=ColumnType.INT64, values=(True,))


def test_column_float64_accepts_int():
    col = Column(name="c", type=ColumnType.FLOAT64, values=(1, 2.0))
    assert len(col) == 2


def test_column_allows_null():
    col = Column(name="c", type=ColumnType.STRING, values=("a", None, "b"))
    assert col.null_count() == 1


def test_schema_rejects_empty_fields():
    with pytest.raises(ValueError):
        Schema(fields=())


def test_schema_rejects_duplicate_names():
    with pytest.raises(ValueError):
        Schema(fields=(("a", ColumnType.INT64), ("a", ColumnType.STRING)))


def test_schema_validate_count_mismatch():
    s = Schema(fields=(("a", ColumnType.INT64),))
    with pytest.raises(ValueError):
        s.validate([])


def test_schema_validate_type_mismatch():
    s = Schema(fields=(("a", ColumnType.INT64),))
    with pytest.raises(ValueError):
        s.validate([Column("a", ColumnType.STRING, ("x",))])


def test_schema_validate_unequal_row_counts():
    s = Schema(fields=(("a", ColumnType.INT64), ("b", ColumnType.INT64)))
    with pytest.raises(ValueError):
        s.validate(
            [
                Column("a", ColumnType.INT64, (1, 2, 3)),
                Column("b", ColumnType.INT64, (1, 2)),
            ]
        )


def test_schema_names_property():
    s = Schema(fields=(("a", ColumnType.INT64), ("b", ColumnType.STRING)))
    assert s.names == ("a", "b")
