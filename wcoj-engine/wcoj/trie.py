"""TrieIterator: implicit trie over a lexicographically sorted numpy array.

Each column in the array is one level of the trie.  The iterator supports the
four operations required by Leapfrog Triejoin:

    open()   – descend one level (restrict to subtrie of current key)
    up()     – ascend one level
    next()   – advance to the next distinct key at current depth
    seek(x)  – advance to the first key >= x at current depth

All operations are O(log n) in the number of rows thanks to binary search on
numpy views (no copies for basic slice+column indexing).
"""
from __future__ import annotations

from typing import Any, List, Tuple

import numpy as np


class TrieIterator:
    """Iterator over an implicit trie defined by a sorted 2-D numpy array."""

    __slots__ = ("data", "n_rows", "n_cols", "_depth", "_stack")

    def __init__(self, data: np.ndarray) -> None:
        """
        Args:
            data: 2-D int64 array of shape (n_tuples, n_cols), sorted
                  lexicographically.  Each column = one trie level.
        """
        if data.ndim == 1:
            data = data.reshape(-1, 1)
        self.data: np.ndarray = data
        self.n_rows, self.n_cols = data.shape
        self._depth: int = -1  # -1 = not yet opened
        # Each entry: (range_start, range_end, key_pos)
        #   range_start/end  – valid row-index range at this depth
        #   key_pos          – row index of the first row with current key
        self._stack: List[Tuple[int, int, int]] = []

    # ------------------------------------------------------------------ #
    #  Navigation                                                          #
    # ------------------------------------------------------------------ #

    def open(self) -> None:
        """Descend one level, narrowing to the subtrie of the current key."""
        if self._depth == -1:
            # Root: the full array is the initial range.
            self._stack.append((0, self.n_rows, 0))
        else:
            _, end, key_pos = self._stack[-1]
            col = self._depth
            curr_key = int(self.data[key_pos, col])
            # Rows sharing this key form a contiguous block starting at key_pos.
            block_end = key_pos + int(
                np.searchsorted(self.data[key_pos:end, col], curr_key, side="right")
            )
            self._stack.append((key_pos, block_end, key_pos))
        self._depth += 1

    def up(self) -> None:
        """Ascend one level, restoring the parent range."""
        self._stack.pop()
        self._depth -= 1

    def next(self) -> None:
        """Advance to the next distinct key at the current depth."""
        start, end, key_pos = self._stack[-1]
        col = self._depth
        curr_key = self.data[key_pos, col]
        new_pos = key_pos + int(
            np.searchsorted(self.data[key_pos:end, col], curr_key, side="right")
        )
        self._stack[-1] = (start, end, new_pos)

    def seek(self, x: Any) -> None:
        """Advance to the first key >= x at the current depth."""
        start, end, key_pos = self._stack[-1]
        col = self._depth
        new_pos = key_pos + int(
            np.searchsorted(self.data[key_pos:end, col], x, side="left")
        )
        self._stack[-1] = (start, end, new_pos)

    # ------------------------------------------------------------------ #
    #  Accessors                                                           #
    # ------------------------------------------------------------------ #

    def at_end(self) -> bool:
        """True when no more keys remain at this depth."""
        if not self._stack:
            return True
        _, end, key_pos = self._stack[-1]
        return key_pos >= end

    def key(self) -> int:
        """Return the current key (must not be at_end)."""
        _, _, key_pos = self._stack[-1]
        return int(self.data[key_pos, self._depth])

    def depth(self) -> int:
        """Current depth (-1 before first open())."""
        return self._depth

    def reset(self) -> None:
        """Reset to initial state (depth -1)."""
        self._depth = -1
        self._stack.clear()

    def __repr__(self) -> str:
        s = "at_end" if (self._depth >= 0 and self.at_end()) else (
            f"key={self.key()}" if self._depth >= 0 else "unopened"
        )
        return f"TrieIterator(depth={self._depth}, {s}, rows={self.n_rows})"
