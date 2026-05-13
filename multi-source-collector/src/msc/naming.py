"""Naming convention for staged data.

A staged record is keyed by a four-part path:

    <source>/<dataset>/<partition>/<run_id>.<ext>

where:

  * ``source``   — origin system slug (``http_api``, ``csv``, ``ftp``,
    ``gsheet``, ``excel`` or a tenant-specific alias).
  * ``dataset``  — logical table/object name within that source.
  * ``partition`` — physical partition, ``YYYY/MM/DD`` from the run's
    ingestion timestamp (UTC) by default.
  * ``run_id``   — opaque idempotency key (default: timestamp + sha-256
    of source+dataset; callers can override).

Strings are normalised (lowercased, non-`[a-z0-9_]` replaced with ``_``)
so the same dataset always lands on the same path regardless of how it
was spelt upstream.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from dataclasses import dataclass

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slug(value: str) -> str:
    if not value:
        raise ValueError("value must be non-empty")
    s = _SLUG_RE.sub("_", value.strip().lower())
    s = s.strip("_")
    if not s:
        raise ValueError(f"value {value!r} normalises to empty")
    return s


@dataclass(frozen=True, slots=True)
class StagedKey:
    """The four-part identity of a record landed in the staging zone."""

    source: str
    dataset: str
    partition: str  # YYYY/MM/DD
    run_id: str
    ext: str = "jsonl"

    def __post_init__(self) -> None:
        # Validate partition shape: digits + slashes, exactly two slashes.
        parts = self.partition.split("/")
        if len(parts) != 3 or not all(p.isdigit() for p in parts):
            raise ValueError(f"partition must be YYYY/MM/DD (got {self.partition!r})")
        if not self.run_id:
            raise ValueError("run_id must be non-empty")
        if not self.ext or "/" in self.ext:
            raise ValueError("ext must be a non-empty slug without '/'")

    def path(self) -> str:
        """Return the canonical relative path under the staging root."""
        return f"{self.source}/{self.dataset}/{self.partition}/{self.run_id}.{self.ext}"

    @classmethod
    def parse(cls, path: str) -> StagedKey:
        """Inverse of :meth:`path`. Raises ``ValueError`` on malformed input."""
        parts = path.split("/")
        if len(parts) != 6:
            raise ValueError(f"malformed staged path {path!r}")
        source, dataset, year, month, day, leaf = parts
        if "." not in leaf:
            raise ValueError(f"staged leaf missing extension: {leaf!r}")
        run_id, ext = leaf.rsplit(".", 1)
        return cls(
            source=source,
            dataset=dataset,
            partition=f"{year}/{month}/{day}",
            run_id=run_id,
            ext=ext,
        )


@dataclass(frozen=True, slots=True)
class NamingConvention:
    """Pure-function generator for :class:`StagedKey` instances."""

    default_ext: str = "jsonl"

    def make(
        self,
        *,
        source: str,
        dataset: str,
        when: dt.datetime | None = None,
        run_id: str | None = None,
        ext: str | None = None,
    ) -> StagedKey:
        """Build a normalised :class:`StagedKey` for the given metadata."""
        when = when or dt.datetime.now(tz=dt.timezone.utc)
        if when.tzinfo is None:
            raise ValueError("when must be timezone-aware")
        partition = when.strftime("%Y/%m/%d")
        s = _slug(source)
        d = _slug(dataset)
        if run_id is None:
            run_id = _default_run_id(s, d, when)
        if not run_id:
            raise ValueError("run_id must be non-empty")
        return StagedKey(
            source=s,
            dataset=d,
            partition=partition,
            run_id=run_id,
            ext=ext or self.default_ext,
        )


def _default_run_id(source: str, dataset: str, when: dt.datetime) -> str:
    """Run-id derived from (source, dataset, timestamp). Deterministic."""
    ts = when.strftime("%Y%m%dT%H%M%S")
    digest = hashlib.sha256(f"{source}|{dataset}|{when.isoformat()}".encode()).hexdigest()[:8]
    return f"{ts}-{digest}"


__all__ = ["NamingConvention", "StagedKey"]
