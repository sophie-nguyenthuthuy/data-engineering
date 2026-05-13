"""Paginator unit tests."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlsplit

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aph.paginators.base import PageRequest
from aph.paginators.cursor import CursorPaginator
from aph.paginators.link_header import LinkHeaderPaginator, parse_link_header
from aph.paginators.offset import OffsetPaginator
from aph.paginators.token import TokenPaginator
from aph.transport import Response


def _query(url: str) -> dict[str, str]:
    return dict(parse_qsl(urlsplit(url).query))


# -------------------------------------------------------------- Offset


def test_offset_first_sets_offset_zero_and_limit():
    p = OffsetPaginator(limit=20, records_path="data")
    req = p.first("https://api/x")
    q = _query(req.url)
    assert q["offset"] == "0"
    assert q["limit"] == "20"


def test_offset_next_advances_offset_when_page_full():
    p = OffsetPaginator(limit=2, records_path="data")
    req = p.first("https://api/x")
    resp = Response(status=200, body={"data": [{"id": 1}, {"id": 2}]})
    nxt = p.next(req, resp)
    assert nxt is not None
    assert _query(nxt.url)["offset"] == "2"


def test_offset_next_returns_none_on_partial_page():
    p = OffsetPaginator(limit=10, records_path="data")
    req = p.first("https://api/x")
    resp = Response(status=200, body={"data": [{"id": 1}]})
    assert p.next(req, resp) is None


def test_offset_records_from_root_list():
    p = OffsetPaginator()
    resp = Response(status=200, body=[{"id": 1}, {"id": 2}])
    assert p.records(resp) == [{"id": 1}, {"id": 2}]


def test_offset_rejects_zero_limit():
    with pytest.raises(ValueError):
        OffsetPaginator(limit=0)


def test_offset_rejects_empty_param_names():
    with pytest.raises(ValueError):
        OffsetPaginator(offset_param="")


# -------------------------------------------------------------- Cursor


def test_cursor_first_removes_cursor_param():
    p = CursorPaginator()
    req = p.first("https://api/x?cursor=stale")
    assert "cursor" not in _query(req.url)


def test_cursor_next_sets_cursor_query():
    p = CursorPaginator(cursor_param="cursor", cursor_path="next_cursor", records_path="data")
    req = PageRequest(url="https://api/x")
    resp = Response(status=200, body={"data": [], "next_cursor": "abc"})
    nxt = p.next(req, resp)
    assert nxt is not None
    assert _query(nxt.url)["cursor"] == "abc"


def test_cursor_next_none_when_cursor_missing_or_empty():
    p = CursorPaginator()
    req = PageRequest(url="https://api/x")
    assert p.next(req, Response(status=200, body={"data": []})) is None
    assert p.next(req, Response(status=200, body={"next_cursor": ""})) is None


def test_cursor_breaks_loop_on_echoed_cursor():
    p = CursorPaginator()
    req = PageRequest(url="https://api/x?cursor=same")
    resp = Response(status=200, body={"next_cursor": "same"})
    assert p.next(req, resp) is None


def test_cursor_records_from_nested_path():
    p = CursorPaginator(records_path="results.items")
    resp = Response(status=200, body={"results": {"items": [{"id": 1}]}})
    assert p.records(resp) == [{"id": 1}]


def test_cursor_validates_args():
    with pytest.raises(ValueError):
        CursorPaginator(cursor_param="")
    with pytest.raises(ValueError):
        CursorPaginator(cursor_path="")


# --------------------------------------------------------------- Token


def test_token_first_strips_token_param():
    p = TokenPaginator()
    req = p.first("https://api/x?pageToken=stale")
    assert "pageToken" not in _query(req.url)


def test_token_next_uses_next_token_path():
    p = TokenPaginator(next_token_path="nextPageToken", records_path="items")
    req = PageRequest(url="https://api/x")
    resp = Response(status=200, body={"items": [], "nextPageToken": "tok"})
    nxt = p.next(req, resp)
    assert nxt is not None
    assert _query(nxt.url)["pageToken"] == "tok"


def test_token_next_none_when_missing():
    p = TokenPaginator()
    req = PageRequest(url="https://api/x")
    assert p.next(req, Response(status=200, body={"items": []})) is None


def test_token_records_returns_empty_when_path_missing():
    p = TokenPaginator(records_path="items")
    assert p.records(Response(status=200, body={})) == []


def test_token_validates_args():
    with pytest.raises(ValueError):
        TokenPaginator(token_param="")
    with pytest.raises(ValueError):
        TokenPaginator(next_token_path="")


# ----------------------------------------------------------- LinkHeader


def test_parse_link_header_basic_pair():
    link = '<https://api/x?page=2>; rel="next", <https://api/x?page=5>; rel="last"'
    parsed = parse_link_header(link)
    assert parsed["next"] == "https://api/x?page=2"
    assert parsed["last"] == "https://api/x?page=5"


def test_parse_link_header_unknown_format_returns_empty():
    assert parse_link_header("garbage") == {}


def test_parse_link_header_ignores_entry_without_rel():
    parsed = parse_link_header('<https://api/x>; type="x"')
    assert parsed == {}


def test_link_header_paginator_follows_rel_next():
    p = LinkHeaderPaginator(records_path="items")
    req = PageRequest(url="https://api/x?page=1")
    resp = Response(
        status=200,
        body={"items": [{"id": 1}]},
        headers={"Link": '<https://api/x?page=2>; rel="next"'},
    )
    nxt = p.next(req, resp)
    assert nxt is not None
    assert nxt.url == "https://api/x?page=2"


def test_link_header_paginator_terminates_when_no_next():
    p = LinkHeaderPaginator(records_path="items")
    req = PageRequest(url="https://api/x?page=last")
    resp = Response(
        status=200,
        body={"items": []},
        headers={"Link": '<https://api/x?page=last>; rel="prev"'},
    )
    assert p.next(req, resp) is None


def test_link_header_paginator_returns_none_when_next_equals_self():
    p = LinkHeaderPaginator(records_path="items")
    req = PageRequest(url="https://api/x?page=1")
    resp = Response(
        status=200,
        body={"items": []},
        headers={"Link": '<https://api/x?page=1>; rel="next"'},
    )
    assert p.next(req, resp) is None


def test_link_header_records_at_root_when_no_path():
    p = LinkHeaderPaginator()
    resp = Response(status=200, body=[{"id": 1}, {"id": 2}], headers={"Link": ""})
    assert p.records(resp) == [{"id": 1}, {"id": 2}]


# ------------------------------------------------------ Hypothesis property


@settings(max_examples=30, deadline=None)
@given(
    pages=st.lists(st.lists(st.integers(0, 1000), min_size=0, max_size=10), min_size=1, max_size=10)
)
def test_property_offset_iterates_every_record(pages):
    """For *any* partitioning of a record list into pages of size ≤ limit,
    the offset paginator iterates the same records back in order."""
    limit = max(1, max(len(p) for p in pages))
    # Pad every page except the last to `limit`, so the paginator only stops on
    # a deliberately-short last page.
    padded = [page + ["pad"] * (limit - len(page)) for page in pages[:-1]] + [pages[-1]]

    from urllib.parse import parse_qsl, urlsplit

    from aph.client import PaginatedClient
    from aph.paginators.offset import OffsetPaginator

    flat = [r for page in padded for r in page]

    def transport(url: str, _headers: object) -> Response:
        q = dict(parse_qsl(urlsplit(url).query))
        offset = int(q["offset"])
        chunk = flat[offset : offset + limit]
        return Response(status=200, body={"data": chunk}, url=url)

    client = PaginatedClient(
        transport=transport,
        paginator=OffsetPaginator(limit=limit, records_path="data"),
    )
    records = client.fetch_all("https://api/x")
    # The padded fixture sees offset progression terminate when the last page
    # is shorter than `limit` — we must have seen every record up to that point.
    expected = [r for page in padded for r in page]
    if len(padded[-1]) < limit:
        assert records == expected
