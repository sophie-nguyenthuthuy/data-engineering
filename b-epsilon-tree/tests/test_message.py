"""Message + Op."""

from __future__ import annotations

from beps.tree.message import Message, Op


def test_put_message_repr():
    m = Message(op=Op.PUT, key=b"k", value=42, seq=1)
    assert "Put" in repr(m)
    assert "42" in repr(m)


def test_del_message_repr():
    m = Message(op=Op.DEL, key=b"k", seq=2)
    assert "Del" in repr(m)


def test_message_value_optional_for_del():
    m = Message(op=Op.DEL, key=b"k", seq=2)
    assert m.value is None
