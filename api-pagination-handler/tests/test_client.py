"""PaginatedClient integration tests."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlsplit

import pytest

from aph.client import PaginatedClient
from aph.paginators.cursor import CursorPaginator
from aph.paginators.link_header import LinkHeaderPaginator
from aph.paginators.offset import OffsetPaginator
from aph.paginators.token import TokenPaginator
from aph.retry import RetryPolicy
from aph.transport import Response


def _offset_transport(records, page_size):
    def transport(url, _headers):
        q = dict(parse_qsl(urlsplit(url).query))
        offset = int(q.get("offset", "0"))
        chunk = records[offset : offset + page_size]
        return Response(status=200, body={"data": chunk}, url=url)

    return transport


def _cursor_transport(records, page_size):
    def transport(url, _headers):
        q = dict(parse_qsl(urlsplit(url).query))
        cursor = q.get("cursor")
        start = int(cursor) if cursor else 0
        chunk = records[start : start + page_size]
        next_start = start + len(chunk)
        return Response(
            status=200,
            body={
                "data": chunk,
                "next_cursor": str(next_start) if next_start < len(records) else None,
            },
            url=url,
        )

    return transport


def test_client_iterates_offset_pagination():
    records = [{"id": i} for i in range(10)]
    client = PaginatedClient(
        transport=_offset_transport(records, page_size=3),
        paginator=OffsetPaginator(limit=3, records_path="data"),
    )
    assert client.fetch_all("https://api/x") == records


def test_client_iterates_cursor_pagination():
    records = [{"id": i} for i in range(7)]
    client = PaginatedClient(
        transport=_cursor_transport(records, page_size=2),
        paginator=CursorPaginator(records_path="data"),
    )
    assert client.fetch_all("https://api/x") == records


def test_client_iterates_token_pagination():
    pages = [
        {"items": [{"id": 0}, {"id": 1}], "nextPageToken": "p2"},
        {"items": [{"id": 2}, {"id": 3}], "nextPageToken": "p3"},
        {"items": [{"id": 4}]},
    ]

    def transport(url, _headers):
        q = dict(parse_qsl(urlsplit(url).query))
        tok = q.get("pageToken")
        idx = {None: 0, "p2": 1, "p3": 2}[tok]
        return Response(status=200, body=pages[idx], url=url)

    client = PaginatedClient(transport=transport, paginator=TokenPaginator())
    out = client.fetch_all("https://api/x")
    assert [r["id"] for r in out] == [0, 1, 2, 3, 4]


def test_client_iterates_link_header_pagination():
    pages = {
        "https://api/x?page=1": (
            {"items": [{"id": 1}]},
            '<https://api/x?page=2>; rel="next"',
        ),
        "https://api/x?page=2": (
            {"items": [{"id": 2}]},
            '<https://api/x?page=1>; rel="prev"',
        ),
    }

    def transport(url, _headers):
        body, link = pages[url]
        return Response(status=200, body=body, headers={"Link": link}, url=url)

    client = PaginatedClient(
        transport=transport,
        paginator=LinkHeaderPaginator(records_path="items"),
    )
    assert client.fetch_all("https://api/x?page=1") == [{"id": 1}, {"id": 2}]


def test_client_retries_transient_503():
    state = {"n": 0}

    def transport(url, _headers):
        state["n"] += 1
        if state["n"] < 3:
            return Response(status=503, body={})
        return Response(status=200, body={"data": [{"id": 1}]}, url=url)

    client = PaginatedClient(
        transport=transport,
        paginator=OffsetPaginator(limit=10, records_path="data"),
        retry=RetryPolicy(max_attempts=5, base=0.001, jitter=False),
    )
    assert client.fetch_all("https://api/x") == [{"id": 1}]
    assert state["n"] == 3


def test_client_raises_on_non_retryable_400():
    def transport(_url, _headers):
        return Response(status=400, body={"error": "bad"})

    client = PaginatedClient(
        transport=transport,
        paginator=OffsetPaginator(limit=5, records_path="data"),
    )
    with pytest.raises(RuntimeError):
        client.fetch_all("https://api/x")


def test_client_respects_max_pages_circuit_breaker():
    """A paginator that never returns ``None`` is stopped by max_pages."""

    def transport(_url, _headers):
        # Always returns 1 record + a fresh cursor — would loop forever.
        return Response(
            status=200,
            body={"data": [{"id": 1}], "next_cursor": "rotating"},
            headers={},
        )

    # Build a cursor paginator that *doesn't* echo-detect (rotates cursor).
    state = {"n": 0}

    class _Cursor(CursorPaginator):
        def next(self, prev, resp):
            state["n"] += 1
            return super().next(
                prev,
                Response(
                    status=200,
                    body={"next_cursor": f"c{state['n']}"},
                    headers=resp.headers,
                ),
            )

    client = PaginatedClient(
        transport=transport,
        paginator=_Cursor(records_path="data"),
        max_pages=5,
    )
    out = client.fetch_all("https://api/x")
    assert len(out) == 5  # exactly max_pages records emitted before stop


def test_client_rejects_empty_base_url():
    client = PaginatedClient(
        transport=lambda u, h: Response(status=200, body={"data": []}),
        paginator=OffsetPaginator(limit=10, records_path="data"),
    )
    with pytest.raises(ValueError):
        list(client.iter_pages(""))


def test_client_rejects_zero_max_pages():
    with pytest.raises(ValueError):
        PaginatedClient(
            transport=lambda u, h: Response(status=200, body={"data": []}),
            paginator=OffsetPaginator(limit=10, records_path="data"),
            max_pages=0,
        )
