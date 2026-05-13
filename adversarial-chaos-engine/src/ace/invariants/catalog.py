"""Invariant DSL with **scoped** registration.

Each :class:`Catalog` owns its own registry, so tests can run in
parallel without bumping into each other through a global dict. This
was the single biggest correctness issue in the original prototype —
the module-level registry leaked between test cases.

Functions register through ``catalog.invariant(...)`` as a decorator;
the catalog records ``(fn, [InvariantSpec, ...])`` keyed by ``fn.__name__``.
There is one *module-level convenience* :func:`invariant` that targets
a shared "default" catalog, but the runner can be pointed at any
catalog explicitly.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias

Row: TypeAlias = dict[str, Any]
Frame: TypeAlias = list[Row]
CheckFn: TypeAlias = Callable[[Frame, Frame], bool]
Pipeline: TypeAlias = Callable[[Frame], Frame]


@dataclass(frozen=True, slots=True)
class InvariantSpec:
    """A single named invariant: ``check(input, output) → bool``."""

    name: str
    check: CheckFn
    description: str
    columns: tuple[str, ...] = ()  # columns referenced by the invariant

    def applies_to_column(self, col: str) -> bool:
        return col in self.columns


@dataclass
class Catalog:
    """Per-test invariant registry."""

    _entries: dict[str, tuple[Pipeline, list[InvariantSpec]]] = field(default_factory=dict)

    # ---------------------------------------------------------------- API

    def invariant(self, **kwargs: Any) -> Callable[[Pipeline], Pipeline]:
        """Decorator factory mirroring the module-level :func:`invariant`."""

        def wrap(fn: Pipeline) -> Pipeline:
            from ace.invariants.checks import build_specs_from_kwargs

            specs = build_specs_from_kwargs(kwargs)
            self._entries[fn.__name__] = (fn, list(specs))

            @functools.wraps(fn)
            def wrapped(*args: Any, **kw: Any) -> Frame:
                return fn(*args, **kw)

            return wrapped

        return wrap

    def register(self, fn: Pipeline, specs: Sequence[InvariantSpec]) -> None:
        """Imperative registration without a decorator."""
        self._entries[fn.__name__] = (fn, list(specs))

    def names(self) -> list[str]:
        return list(self._entries)

    def specs_for(self, fn_name: str) -> list[InvariantSpec]:
        entry = self._entries.get(fn_name)
        return list(entry[1]) if entry else []

    def function(self, fn_name: str) -> Pipeline | None:
        entry = self._entries.get(fn_name)
        return entry[0] if entry else None

    def entries(self) -> list[tuple[str, Pipeline, list[InvariantSpec]]]:
        return [(name, fn, list(specs)) for name, (fn, specs) in self._entries.items()]

    def referenced_columns(self) -> set[str]:
        out: set[str] = set()
        for _, _, specs in self.entries():
            for spec in specs:
                out.update(spec.columns)
        return out

    def clear(self) -> None:
        self._entries.clear()


# --------------------------------------------------------------- convenience

_default_catalog = Catalog()


def invariant(**kwargs: Any) -> Callable[[Pipeline], Pipeline]:
    """Decorator that registers on the module-level default catalog."""
    return _default_catalog.invariant(**kwargs)


def default_catalog() -> Catalog:
    return _default_catalog


__all__ = [
    "Catalog",
    "CheckFn",
    "Frame",
    "InvariantSpec",
    "Pipeline",
    "Row",
    "default_catalog",
    "invariant",
]
