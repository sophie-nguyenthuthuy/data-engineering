"""api-pagination-handler — generic paginator + retry framework."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from aph.client import PaginatedClient
    from aph.paginators.base import PageRequest, Paginator
    from aph.paginators.cursor import CursorPaginator
    from aph.paginators.link_header import LinkHeaderPaginator
    from aph.paginators.offset import OffsetPaginator
    from aph.paginators.token import TokenPaginator
    from aph.retry import RetryError, RetryPolicy
    from aph.transport import Response


_LAZY: dict[str, tuple[str, str]] = {
    "PageRequest": ("aph.paginators.base", "PageRequest"),
    "Paginator": ("aph.paginators.base", "Paginator"),
    "OffsetPaginator": ("aph.paginators.offset", "OffsetPaginator"),
    "CursorPaginator": ("aph.paginators.cursor", "CursorPaginator"),
    "TokenPaginator": ("aph.paginators.token", "TokenPaginator"),
    "LinkHeaderPaginator": ("aph.paginators.link_header", "LinkHeaderPaginator"),
    "RetryPolicy": ("aph.retry", "RetryPolicy"),
    "RetryError": ("aph.retry", "RetryError"),
    "Response": ("aph.transport", "Response"),
    "PaginatedClient": ("aph.client", "PaginatedClient"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CursorPaginator",
    "LinkHeaderPaginator",
    "OffsetPaginator",
    "PageRequest",
    "PaginatedClient",
    "Paginator",
    "Response",
    "RetryError",
    "RetryPolicy",
    "TokenPaginator",
    "__version__",
]
