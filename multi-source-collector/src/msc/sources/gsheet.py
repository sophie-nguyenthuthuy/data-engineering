"""Google Sheets source adapter.

We hit the *public* GViz endpoint
``https://docs.google.com/spreadsheets/d/<id>/gviz/tq?tqx=out:csv&sheet=<sheet>``
because it is keyless for any sheet marked "anyone with the link can
view". The response is CSV, so we delegate parsing to the same logic
:class:`CSVSource` uses.

For private sheets the project's ``[gsheet]`` extra adds ``requests``;
the caller can pass an authenticated session via ``fetcher`` and we'll
use it instead of the default keyless GET.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from urllib.error import URLError
from urllib.request import Request, urlopen

from msc.sources.base import Record, Source, SourceError

Fetcher = Callable[[str], bytes]


def _default_fetcher(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "multi-source-collector"})
    try:
        with urlopen(req, timeout=30.0) as resp:
            data: bytes = resp.read()
            return data
    except URLError as exc:
        raise SourceError(f"could not GET Google Sheet: {exc}") from exc


def _build_url(sheet_id: str, sheet_name: str | None) -> str:
    base = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv"
    if sheet_name:
        return base + f"&sheet={sheet_name}"
    return base


@dataclass
class GoogleSheetSource(Source):
    """Public Google Sheet → CSV → :class:`Record` iterator."""

    sheet_id: str
    dataset: str
    sheet_name: str | None = None
    id_column: str | None = None
    fetcher: Fetcher | None = field(default=None)
    kind: str = "gsheet"

    def __post_init__(self) -> None:
        if not self.sheet_id:
            raise ValueError("sheet_id must be non-empty")
        super().__post_init__()

    def fetch(self) -> Iterator[Record]:
        url = _build_url(self.sheet_id, self.sheet_name)
        body = (self.fetcher or _default_fetcher)(url)
        text = body.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames is None:
            return
        if self.id_column and self.id_column not in reader.fieldnames:
            raise SourceError(
                f"id_column {self.id_column!r} not in sheet header {reader.fieldnames}"
            )
        for i, row in enumerate(reader, start=1):
            source_id = str(row[self.id_column]) if self.id_column is not None else f"row-{i}"
            yield Record(source_id=source_id, fields=dict(row))


__all__ = ["Fetcher", "GoogleSheetSource"]
