"""PII-masking transform.

Each named column gets its value replaced with a fixed ``mask_value``
(default ``"****"``) in both ``before`` and ``after``. Optional regex
masking lets the same transform redact substrings (e.g. an email
embedded in a free-text comment) without naming every column upstream.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cdc.transforms.base import Transform

if TYPE_CHECKING:
    import re
    from collections.abc import Iterable

    from cdc.events.envelope import DebeziumEnvelope


@dataclass
class MaskPII(Transform):
    """Mask listed columns + (optionally) regex-redact substrings."""

    columns: tuple[str, ...] = ()
    regex_columns: tuple[str, ...] = ()
    pattern: re.Pattern[str] | None = None
    mask_value: str = "****"
    name: str = "mask_pii"
    _columns_set: frozenset[str] = field(init=False, repr=False)
    _regex_set: frozenset[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.columns and not self.regex_columns:
            raise ValueError("MaskPII needs at least one column to mask")
        if self.regex_columns and self.pattern is None:
            raise ValueError("regex_columns requires pattern")
        object.__setattr__(self, "_columns_set", frozenset(self.columns))
        object.__setattr__(self, "_regex_set", frozenset(self.regex_columns))

    def apply(self, envelope: DebeziumEnvelope) -> DebeziumEnvelope:
        from cdc.events.envelope import DebeziumEnvelope as _Env

        return _Env(
            op=envelope.op,
            source=envelope.source,
            ts_ms=envelope.ts_ms,
            before=self._mask(envelope.before),
            after=self._mask(envelope.after),
            extra=envelope.extra,
        )

    # ---------------------------------------------------------------- helpers

    def _mask(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        out = dict(row)
        for col in self._iter_intersection(out, self._columns_set):
            out[col] = self.mask_value
        if self.pattern is not None:
            for col in self._iter_intersection(out, self._regex_set):
                v = out[col]
                if isinstance(v, str):
                    out[col] = self.pattern.sub(self.mask_value, v)
        return out

    @staticmethod
    def _iter_intersection(row: dict[str, Any], target: frozenset[str]) -> Iterable[str]:
        for col in row:
            if col in target:
                yield col


__all__ = ["MaskPII"]
