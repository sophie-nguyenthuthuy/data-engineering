"""Envelope + parser tests."""

from __future__ import annotations

import json

import pytest

from cdc.events.envelope import DebeziumEnvelope, Op, SourceInfo
from cdc.events.parse import ParseError, parse_envelope


def _source(table: str = "orders", ts_ms: int = 1) -> SourceInfo:
    return SourceInfo(db="db1", schema="public", table=table, ts_ms=ts_ms)


def _make(op: Op, *, before=None, after=None, ts_ms=10) -> DebeziumEnvelope:
    return DebeziumEnvelope(op=op, source=_source(), ts_ms=ts_ms, before=before, after=after)


# ---------------------------------------------------------------- Op


def test_op_parse_known_codes():
    assert Op.parse("c") is Op.CREATE
    assert Op.parse("u") is Op.UPDATE
    assert Op.parse("d") is Op.DELETE
    assert Op.parse("r") is Op.READ


def test_op_parse_rejects_unknown():
    with pytest.raises(ValueError):
        Op.parse("x")


# ----------------------------------------------------------- SourceInfo


def test_source_info_rejects_empty_table():
    with pytest.raises(ValueError):
        SourceInfo(db="d", schema="s", table="", ts_ms=1)


def test_source_info_rejects_negative_ts_ms():
    with pytest.raises(ValueError):
        SourceInfo(db="d", schema="s", table="t", ts_ms=-1)


# --------------------------------------------------------- DebeziumEnvelope


def test_create_event_requires_after():
    with pytest.raises(ValueError):
        _make(Op.CREATE)


def test_delete_event_requires_before():
    with pytest.raises(ValueError):
        _make(Op.DELETE)


def test_update_event_requires_both():
    with pytest.raises(ValueError):
        _make(Op.UPDATE, before={"id": 1})
    with pytest.raises(ValueError):
        _make(Op.UPDATE, after={"id": 1})


def test_envelope_rejects_negative_ts_ms():
    with pytest.raises(ValueError):
        _make(Op.CREATE, after={"id": 1}, ts_ms=-1)


def test_primary_key_from_after():
    env = _make(Op.CREATE, after={"id": 7, "x": "v"})
    assert env.primary_key(("id",)) == (7,)


def test_primary_key_uses_before_on_delete():
    env = _make(Op.DELETE, before={"id": 7})
    assert env.primary_key(("id",)) == (7,)


def test_primary_key_rejects_missing_column():
    env = _make(Op.CREATE, after={"id": 7})
    with pytest.raises(ValueError):
        env.primary_key(("missing",))


def test_primary_key_rejects_empty_key_columns():
    env = _make(Op.CREATE, after={"id": 7})
    with pytest.raises(ValueError):
        env.primary_key(())


# -------------------------------------------------------------- parse


def _create_payload() -> dict:
    return {
        "op": "c",
        "ts_ms": 100,
        "before": None,
        "after": {"id": 1, "name": "alice"},
        "source": {"db": "d", "schema": "public", "table": "orders", "ts_ms": 99},
    }


def test_parse_accepts_dict_payload():
    env = parse_envelope(_create_payload())
    assert env.op is Op.CREATE
    assert env.after == {"id": 1, "name": "alice"}


def test_parse_accepts_json_string():
    env = parse_envelope(json.dumps(_create_payload()))
    assert env.op is Op.CREATE


def test_parse_accepts_json_bytes():
    env = parse_envelope(json.dumps(_create_payload()).encode())
    assert env.op is Op.CREATE


def test_parse_rejects_invalid_json():
    with pytest.raises(ParseError):
        parse_envelope(b"not json")


def test_parse_rejects_non_object_root():
    with pytest.raises(ParseError):
        parse_envelope("[]")


def test_parse_rejects_missing_op():
    payload = _create_payload()
    del payload["op"]
    with pytest.raises(ParseError):
        parse_envelope(payload)


def test_parse_rejects_unknown_op():
    payload = _create_payload()
    payload["op"] = "x"
    with pytest.raises(ParseError):
        parse_envelope(payload)


def test_parse_rejects_missing_source():
    payload = _create_payload()
    del payload["source"]
    with pytest.raises(ParseError):
        parse_envelope(payload)


def test_parse_rejects_non_dict_after():
    payload = _create_payload()
    payload["after"] = "not a dict"
    with pytest.raises(ParseError):
        parse_envelope(payload)


def test_parse_propagates_envelope_invariants():
    payload = _create_payload()
    payload["op"] = "d"
    payload["after"] = None
    payload["before"] = None  # delete with no before → invariant violation
    with pytest.raises(ParseError):
        parse_envelope(payload)


def test_parse_extracts_optional_lsn_txid():
    payload = _create_payload()
    payload["source"]["lsn"] = 12345
    payload["source"]["txid"] = 678
    env = parse_envelope(payload)
    assert env.source.lsn == 12345
    assert env.source.txid == 678
