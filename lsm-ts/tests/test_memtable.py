import pytest
from lsm.memtable import Memtable
from lsm.types import TSKey, TSValue


def _key(metric: str, ts: int) -> TSKey:
    return TSKey.make(metric, {"host": "h1"}, ts)


def _val(v: float) -> TSValue:
    return TSValue(value=v)


def test_put_and_get():
    m = Memtable()
    k = _key("cpu", 1000)
    m.put(k, _val(42.0))
    assert m.get(k) == _val(42.0)


def test_overwrite():
    m = Memtable()
    k = _key("cpu", 1000)
    m.put(k, _val(1.0))
    m.put(k, _val(2.0))
    assert m.get(k) == _val(2.0)


def test_delete_returns_none():
    m = Memtable()
    k = _key("mem", 2000)
    m.put(k, _val(100.0))
    m.delete(k)
    assert m.get(k) is None


def test_range_scan_ordered():
    m = Memtable()
    for ts in [3000, 1000, 2000]:
        m.put(_key("temp", ts), _val(float(ts)))
    start = _key("temp", 1000)
    end   = _key("temp", 3000)
    pairs = m.range_scan(start, end)
    keys_ts = [TSKey.decode(k).timestamp_ns for k, _ in pairs]
    assert keys_ts == sorted(keys_ts)
    assert 3000 not in keys_ts  # end is exclusive


def test_is_full():
    m = Memtable(size_limit_bytes=100)
    assert not m.is_full
    for i in range(20):
        m.put(_key("x", i), _val(float(i)))
    assert m.is_full


def test_size_tracking():
    m = Memtable()
    assert m.size_bytes == 0
    m.put(_key("a", 1), _val(1.0))
    assert m.size_bytes > 0
