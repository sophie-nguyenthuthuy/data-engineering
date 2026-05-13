"""Source-adapter tests using local fixtures + fake transports."""

from __future__ import annotations

import json

import pytest

from msc.sources.base import Record, SourceError
from msc.sources.csv_src import CSVSource
from msc.sources.ftp import FTPSource
from msc.sources.gsheet import GoogleSheetSource
from msc.sources.http_api import HTTPAPISource

# ----------------------------------------------------------------- Record


def test_record_rejects_empty_source_id():
    with pytest.raises(ValueError):
        Record(source_id="")


def test_record_holds_fields_dict():
    r = Record(source_id="r1", fields={"a": 1})
    assert r.fields["a"] == 1


# ---------------------------------------------------------------- CSV


def _write_csv(tmp_path, text: str):
    p = tmp_path / "data.csv"
    p.write_text(text, encoding="utf-8")
    return p


def test_csv_source_emits_one_record_per_row(tmp_path):
    p = _write_csv(tmp_path, "id,name\n1,alice\n2,bob\n")
    rows = list(CSVSource(path=p, id_column="id").fetch())
    assert [r.source_id for r in rows] == ["1", "2"]
    assert rows[0].fields == {"id": "1", "name": "alice"}


def test_csv_source_default_id_is_row_number(tmp_path):
    p = _write_csv(tmp_path, "a,b\n10,20\n30,40\n")
    rows = list(CSVSource(path=p, dataset="dataset").fetch())
    assert [r.source_id for r in rows] == ["row-1", "row-2"]


def test_csv_source_default_dataset_is_stem(tmp_path):
    p = _write_csv(tmp_path, "a\n1\n")
    src = CSVSource(path=p)
    assert src.dataset == "data"


def test_csv_source_raises_on_missing_file(tmp_path):
    src = CSVSource(path=tmp_path / "ghost.csv", dataset="x")
    with pytest.raises(SourceError):
        list(src.fetch())


def test_csv_source_raises_on_unknown_id_column(tmp_path):
    p = _write_csv(tmp_path, "a\n1\n")
    src = CSVSource(path=p, id_column="b", dataset="x")
    with pytest.raises(SourceError):
        list(src.fetch())


# ---------------------------------------------------------------- HTTP


def _bytes_fetcher(payload: dict):
    body = json.dumps(payload).encode()

    def _fetch(_url: str, _headers: dict, _timeout: float) -> bytes:
        return body

    return _fetch


def test_http_source_reads_records_at_root():
    src = HTTPAPISource(
        url="https://example.com/x",
        dataset="users",
        fetcher=_bytes_fetcher([{"id": "u1", "name": "a"}, {"id": "u2", "name": "b"}]),
        id_field="id",
    )
    rows = list(src.fetch())
    assert [r.source_id for r in rows] == ["u1", "u2"]


def test_http_source_resolves_nested_path():
    src = HTTPAPISource(
        url="https://example.com/x",
        dataset="users",
        records_path="data.items",
        fetcher=_bytes_fetcher({"data": {"items": [{"id": "u1"}]}}),
        id_field="id",
    )
    assert next(iter(src.fetch())).source_id == "u1"


def test_http_source_raises_on_missing_path():
    src = HTTPAPISource(
        url="https://example.com/x",
        dataset="users",
        records_path="data.nope",
        fetcher=_bytes_fetcher({"data": {"items": []}}),
    )
    with pytest.raises(SourceError):
        list(src.fetch())


def test_http_source_raises_when_value_is_not_a_list():
    src = HTTPAPISource(
        url="https://example.com/x",
        dataset="users",
        fetcher=_bytes_fetcher({"id": 1}),
    )
    with pytest.raises(SourceError):
        list(src.fetch())


def test_http_source_raises_on_non_object_items():
    def _fetch(*_args, **_kw):
        return b"[1, 2, 3]"

    src = HTTPAPISource(url="https://example.com/x", dataset="x", fetcher=_fetch)
    with pytest.raises(SourceError):
        list(src.fetch())


def test_http_source_raises_on_bad_json():
    def _fetch(*_args, **_kw):
        return b"not json"

    src = HTTPAPISource(url="https://example.com/x", dataset="x", fetcher=_fetch)
    with pytest.raises(SourceError):
        list(src.fetch())


def test_http_source_validates_url():
    with pytest.raises(ValueError):
        HTTPAPISource(url="", dataset="x")


def test_http_source_validates_timeout():
    with pytest.raises(ValueError):
        HTTPAPISource(url="https://x", dataset="x", timeout=0)


def test_http_source_falls_back_to_row_index():
    src = HTTPAPISource(
        url="https://example.com/x",
        dataset="x",
        fetcher=_bytes_fetcher([{"name": "a"}, {"name": "b"}]),
    )
    rows = list(src.fetch())
    assert rows[0].source_id == "row-00000001"


# ---------------------------------------------------------------- FTP


class _FakeFTP:
    def __init__(self, payload: bytes):
        self._payload = payload
        self.logins: list[tuple[str, str]] = []

    def login(self, user="anonymous", passwd=""):
        self.logins.append((user, passwd))
        return "230 ok"

    def retrbinary(self, _cmd, callback):
        callback(self._payload)
        return "226 ok"

    def quit(self):
        return "221 bye"


def test_ftp_source_decodes_csv_and_yields_records():
    payload = b"id,name\n1,alice\n2,bob\n"

    def _connect(_host, _port, _timeout):
        return _FakeFTP(payload)

    src = FTPSource(
        host="example.com",
        remote_path="/data.csv",
        dataset="users",
        id_column="id",
        connect=_connect,
    )
    rows = list(src.fetch())
    assert [r.source_id for r in rows] == ["1", "2"]


def test_ftp_source_validates_host_and_path():
    with pytest.raises(ValueError):
        FTPSource(host="", remote_path="/x", dataset="y")
    with pytest.raises(ValueError):
        FTPSource(host="h", remote_path="", dataset="y")


def test_ftp_source_validates_port_range():
    with pytest.raises(ValueError):
        FTPSource(host="h", remote_path="/x", dataset="y", port=0)
    with pytest.raises(ValueError):
        FTPSource(host="h", remote_path="/x", dataset="y", port=70_000)


def test_ftp_source_propagates_connect_error():
    def _connect(*_args, **_kw):
        raise OSError("nope")

    src = FTPSource(host="h", remote_path="/x", dataset="y", connect=_connect)
    with pytest.raises(SourceError):
        list(src.fetch())


# ----------------------------------------------------------- Google Sheet


def test_gsheet_source_yields_csv_records():
    body = b"id,name\n1,alice\n2,bob\n"

    def _fetch(_url):
        return body

    src = GoogleSheetSource(sheet_id="abc123", dataset="users", id_column="id", fetcher=_fetch)
    rows = list(src.fetch())
    assert [r.source_id for r in rows] == ["1", "2"]


def test_gsheet_validates_sheet_id():
    with pytest.raises(ValueError):
        GoogleSheetSource(sheet_id="", dataset="x")
