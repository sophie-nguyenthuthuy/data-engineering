"""HTTP API source adapter.

Uses :mod:`urllib.request` from the stdlib so the package keeps zero
runtime dependencies. The caller supplies:

  * ``url`` — the endpoint to ``GET``.
  * ``records_path`` — JSON path (dot-separated) where the array of
    records lives in the response (``"data.items"`` → ``payload["data"]["items"]``).
  * ``id_field`` — which key inside each record becomes
    ``Record.source_id``. If omitted, a zero-padded row index is used.
  * ``fetcher`` — optional callable returning bytes for the URL; the
    default is a thin wrapper around ``urllib`` that lets tests inject
    fixtures without touching the network.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from msc.sources.base import Record, Source, SourceError

Fetcher = Callable[[str, dict[str, str], float], bytes]


def _default_fetcher(url: str, headers: dict[str, str], timeout: float) -> bytes:
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            data: bytes = resp.read()
            return data
    except URLError as exc:
        raise SourceError(f"HTTP fetch failed for {url}: {exc}") from exc


@dataclass
class HTTPAPISource(Source):
    """JSON-over-HTTP source.

    ``records_path = ""`` means the response body is itself the list of
    records.
    """

    url: str
    dataset: str
    records_path: str = ""
    id_field: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float = 30.0
    fetcher: Fetcher | None = None
    kind: str = "http_api"

    def __post_init__(self) -> None:
        if not self.url:
            raise ValueError("url must be non-empty")
        if self.timeout <= 0:
            raise ValueError("timeout must be > 0")
        super().__post_init__()

    def fetch(self) -> Iterator[Record]:
        fetch_fn: Fetcher = self.fetcher or _default_fetcher
        body = fetch_fn(self.url, dict(self.headers), self.timeout)
        try:
            payload = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise SourceError(f"could not decode JSON from {self.url}: {exc}") from exc

        records = _resolve_path(payload, self.records_path)
        if not isinstance(records, list):
            raise SourceError(
                f"expected list at path {self.records_path!r} from {self.url}, got "
                f"{type(records).__name__}"
            )
        for i, item in enumerate(records):
            if not isinstance(item, dict):
                raise SourceError(
                    f"records must be JSON objects, got {type(item).__name__} at index {i}"
                )
            source_id = (
                str(item[self.id_field])
                if self.id_field is not None and self.id_field in item
                else f"row-{i + 1:08d}"
            )
            yield Record(source_id=source_id, fields=dict(item))


def _resolve_path(payload: Any, path: str) -> Any:
    if not path:
        return payload
    cur: Any = payload
    for segment in path.split("."):
        if isinstance(cur, dict) and segment in cur:
            cur = cur[segment]
        else:
            raise SourceError(f"path segment {segment!r} not found in payload")
    return cur


__all__ = ["Fetcher", "HTTPAPISource"]
