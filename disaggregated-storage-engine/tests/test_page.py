"""Page abstractions."""

from __future__ import annotations

import pytest

from disagg.core.page import PAGE_SIZE, Page, PageId, blank_page


def test_blank_page_correct_size():
    p = blank_page(PageId(0, 1))
    assert len(p.data) == PAGE_SIZE
    assert p.version == 0
    assert p.data == b"\x00" * PAGE_SIZE


def test_page_update_bumps_version():
    p = blank_page(PageId(0, 1))
    p.update(b"X" * PAGE_SIZE)
    assert p.version == 1
    p.update(b"Y" * PAGE_SIZE)
    assert p.version == 2


def test_page_update_rejects_wrong_size():
    p = blank_page(PageId(0, 1))
    with pytest.raises(ValueError):
        p.update(b"too short")


def test_page_construct_rejects_wrong_size():
    with pytest.raises(ValueError):
        Page(page_id=PageId(0, 1), data=b"too short")


def test_page_clone_is_deep_independent_of_version():
    p = blank_page(PageId(0, 1))
    p.update(b"X" * PAGE_SIZE)
    c = p.clone()
    assert c.version == p.version
    assert c.data == p.data
    # Mutate the original; clone unaffected
    p.update(b"Z" * PAGE_SIZE)
    assert c.version == 1
    assert c.data == b"X" * PAGE_SIZE


def test_page_id_is_hashable_and_repr():
    a = PageId(0, 5)
    b = PageId(0, 5)
    c = PageId(1, 5)
    s = {a, b, c}
    assert len(s) == 2
    assert repr(a) == "p(0:5)"
