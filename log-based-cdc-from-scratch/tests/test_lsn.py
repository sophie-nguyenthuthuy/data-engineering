"""LSN + BinlogPosition tests."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from lcdc.lsn import LSN, BinlogPosition


def test_lsn_rejects_negative():
    with pytest.raises(ValueError):
        LSN(-1)


def test_lsn_rejects_overflow():
    with pytest.raises(ValueError):
        LSN(1 << 64)


def test_lsn_str_format():
    assert str(LSN(0)) == "0/0"
    assert str(LSN((0xABCDEF << 32) | 0x123456)) == "ABCDEF/123456"


def test_lsn_parse_round_trip():
    s = "0/16B374C8"
    assert str(LSN.parse(s)) == s
    assert LSN.parse("0/16B374C8").value == 0x16B374C8


def test_lsn_parse_rejects_bad_input():
    with pytest.raises(ValueError):
        LSN.parse("no_slash")


def test_lsn_ordering():
    assert LSN(1) < LSN(2)
    assert LSN(5) > LSN(4)


def test_binlog_position_rejects_empty_file():
    with pytest.raises(ValueError):
        BinlogPosition(file="", position=0)


def test_binlog_position_rejects_negative_position():
    with pytest.raises(ValueError):
        BinlogPosition(file="binlog.000001", position=-1)


def test_binlog_position_str():
    assert str(BinlogPosition(file="binlog.000001", position=120)) == "binlog.000001:120"


@settings(max_examples=40, deadline=None)
@given(st.integers(0, (1 << 64) - 1))
def test_property_lsn_str_parse_round_trip(v):
    assert LSN.parse(str(LSN(v))) == LSN(v)
