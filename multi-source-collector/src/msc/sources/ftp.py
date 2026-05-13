"""FTP source adapter.

Pulls one or more files from a (potentially anonymous) FTP server,
decoding each as text and feeding the bytes into the CSV adapter so we
get free CSV parsing semantics. The connection is fully injectable so
tests can drive the source against an in-memory fake ``FTP``-like
object — no real network access required.
"""

from __future__ import annotations

import contextlib
import csv
import io
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from msc.sources.base import Record, Source, SourceError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


class FTPLike(Protocol):
    """Minimal subset of :class:`ftplib.FTP` we depend on."""

    def login(self, user: str = ..., passwd: str = ...) -> str: ...
    def retrbinary(self, cmd: str, callback: Callable[[bytes], None]) -> str: ...
    def quit(self) -> str: ...


def _default_connect(host: str, port: int, timeout: float) -> FTPLike:
    from ftplib import FTP

    ftp = FTP(timeout=timeout)
    ftp.connect(host=host, port=port)
    return ftp


@dataclass
class FTPSource(Source):
    """FTP source pulling a CSV file (anonymous by default)."""

    host: str
    remote_path: str
    dataset: str
    port: int = 21
    user: str = "anonymous"
    password: str = ""
    timeout: float = 30.0
    encoding: str = "utf-8"
    delimiter: str = ","
    id_column: str | None = None
    connect: Callable[[str, int, float], FTPLike] = field(default=_default_connect)
    kind: str = "ftp"

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host must be non-empty")
        if not self.remote_path:
            raise ValueError("remote_path must be non-empty")
        if self.port <= 0 or self.port > 65_535:
            raise ValueError("port must be in (0, 65535]")
        if self.timeout <= 0:
            raise ValueError("timeout must be > 0")
        super().__post_init__()

    def fetch(self) -> Iterator[Record]:
        buf = io.BytesIO()
        try:
            ftp = self.connect(self.host, self.port, self.timeout)
        except OSError as exc:
            raise SourceError(f"could not connect to ftp://{self.host}:{self.port}: {exc}") from exc
        try:
            ftp.login(self.user, self.password)

            def _sink(chunk: bytes) -> None:
                buf.write(chunk)

            ftp.retrbinary(f"RETR {self.remote_path}", _sink)
        finally:
            with contextlib.suppress(Exception):
                ftp.quit()
        text = buf.getvalue().decode(self.encoding)
        reader = csv.DictReader(io.StringIO(text), delimiter=self.delimiter)
        if reader.fieldnames is None:
            return
        if self.id_column and self.id_column not in reader.fieldnames:
            raise SourceError(f"id_column {self.id_column!r} not in CSV header {reader.fieldnames}")
        for i, row in enumerate(reader, start=1):
            source_id = str(row[self.id_column]) if self.id_column is not None else f"row-{i}"
            yield Record(source_id=source_id, fields=dict(row))


__all__ = ["FTPLike", "FTPSource"]
