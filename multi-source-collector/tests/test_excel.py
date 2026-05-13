"""Excel adapter — requires the [excel] extra (openpyxl)."""

from __future__ import annotations

import pytest

from msc.sources.base import SourceError
from msc.sources.excel import ExcelSource

openpyxl = pytest.importorskip("openpyxl")


def _make_xlsx(tmp_path, header, rows, name="data.xlsx"):
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.append(header)
    for r in rows:
        ws.append(r)
    p = tmp_path / name
    wb.save(p)
    return p


@pytest.mark.excel
def test_excel_source_emits_one_record_per_row(tmp_path):
    p = _make_xlsx(tmp_path, ["id", "name"], [[1, "alice"], [2, "bob"]])
    rows = list(ExcelSource(path=p, id_column="id").fetch())
    assert [r.source_id for r in rows] == ["1", "2"]
    assert rows[0].fields["name"] == "alice"


@pytest.mark.excel
def test_excel_source_default_id_is_row_index(tmp_path):
    p = _make_xlsx(tmp_path, ["a"], [[10], [20]])
    rows = list(ExcelSource(path=p, dataset="d").fetch())
    assert [r.source_id for r in rows] == ["row-1", "row-2"]


@pytest.mark.excel
def test_excel_source_raises_on_missing_file(tmp_path):
    src = ExcelSource(path=tmp_path / "ghost.xlsx", dataset="x")
    with pytest.raises(SourceError):
        list(src.fetch())


@pytest.mark.excel
def test_excel_source_raises_on_unknown_id_column(tmp_path):
    p = _make_xlsx(tmp_path, ["a"], [[1]])
    src = ExcelSource(path=p, id_column="b", dataset="x")
    with pytest.raises(SourceError):
        list(src.fetch())
