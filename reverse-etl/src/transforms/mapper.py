from typing import Any
from ..models import FieldMapping
from ..logger import get_logger

logger = get_logger(__name__)

_TRANSFORMS: dict[str, Any] = {
    "upper": str.upper,
    "lower": str.lower,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "strip": str.strip,
}


class FieldMapper:
    def __init__(self, mappings: list[FieldMapping]) -> None:
        self._mappings = mappings

    def apply(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not self._mappings:
            return records

        result = []
        for record in records:
            mapped: dict[str, Any] = {}
            for m in self._mappings:
                value = record.get(m.source)
                if m.transform and value is not None:
                    fn = _TRANSFORMS.get(m.transform)
                    if fn:
                        try:
                            value = fn(value)
                        except Exception as e:
                            logger.warning(f"FieldMapper: transform '{m.transform}' failed on {value!r}: {e}")
                mapped[m.destination] = value
            # carry over unmapped fields
            for k, v in record.items():
                if k not in {m.source for m in self._mappings}:
                    mapped.setdefault(k, v)
            result.append(mapped)
        return result
