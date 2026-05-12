"""FSST – Fast Static Symbol Table compression for string columns.

Algorithm overview
------------------
1. Sample the input strings and collect all byte n-grams (length 2-8).
2. Rank candidates by gain = (len − 1) × frequency (bytes saved per use).
3. Adaptively cap the symbol table at min(254, n_strings // 4, budget) to
   avoid overhead dominating on small datasets.
4. Code 254 is the escape byte; a literal byte follows it.
5. Encode left-to-right with longest-match-first.
6. Each encoded string is prefixed with its 2-byte length (max 64 KiB/string).
"""
from __future__ import annotations

import struct
from collections import defaultdict
from typing import Sequence

import numpy as np

from .base import Codec, EncodedColumn

_ESCAPE = 254
_MAX_SYMBOL_LEN = 8
_MAX_SYMBOLS = 254


def _build_symbol_table(strings: Sequence[bytes], max_symbols: int = _MAX_SYMBOLS) -> list[bytes]:
    freq: dict[bytes, int] = defaultdict(int)
    for s in strings:
        n = len(s)
        for length in range(2, min(_MAX_SYMBOL_LEN, n) + 1):
            for i in range(n - length + 1):
                freq[s[i : i + length]] += 1

    candidates = sorted(freq.items(), key=lambda kv: (len(kv[0]) - 1) * kv[1], reverse=True)
    return [sym for sym, _ in candidates[:max_symbols]]


def _encode_string(s: bytes, lookup: dict[bytes, int]) -> bytearray:
    out = bytearray()
    i = 0
    n = len(s)
    while i < n:
        matched = False
        for length in range(min(_MAX_SYMBOL_LEN, n - i), 1, -1):
            sub = s[i : i + length]
            code = lookup.get(sub)
            if code is not None:
                out.append(code)
                i += length
                matched = True
                break
        if not matched:
            out.append(_ESCAPE)
            out.append(s[i])
            i += 1
    return out


def _decode_string(data: bytes, symbols: list[bytes]) -> bytes:
    out = bytearray()
    i = 0
    while i < len(data):
        code = data[i]
        i += 1
        if code == _ESCAPE:
            out.append(data[i])
            i += 1
        else:
            out.extend(symbols[code])
    return bytes(out)


class FSSTCodec(Codec):
    name = "fsst"

    def __init__(self, sample_fraction: float = 0.1) -> None:
        self.sample_fraction = sample_fraction

    def supports_dtype(self, dtype: np.dtype) -> bool:
        return dtype.kind in ("U", "O", "S")

    def _adaptive_max_symbols(self, n_strings: int, total_raw_bytes: int) -> int:
        # Each symbol entry costs ~5 bytes overhead; cap so table < 15% of raw data.
        budget = max(8, total_raw_bytes // (5 * 7))  # ~15% headroom
        return min(_MAX_SYMBOLS, n_strings // 4 + 1, budget)

    def encode(self, data: np.ndarray) -> EncodedColumn:
        strings = [s.encode() if isinstance(s, str) else bytes(s) for s in data]

        if not strings:
            return EncodedColumn(
                codec_name=self.name,
                data=b"\x00",  # n_syms=0
                metadata={"n": 0, "sym_bytes": 1},
                original_dtype=str(data.dtype),
                original_len=0,
            )

        total_raw = sum(len(s) for s in strings)
        max_sym = self._adaptive_max_symbols(len(strings), total_raw)

        n_sample = max(1, int(len(strings) * self.sample_fraction))
        rng = np.random.default_rng(42)
        idx = rng.choice(len(strings), size=min(n_sample, len(strings)), replace=False)
        sample = [strings[i] for i in idx]

        symbols = _build_symbol_table(sample, max_symbols=max_sym)
        lookup: dict[bytes, int] = {sym: code for code, sym in enumerate(symbols)}

        encoded_strings = [_encode_string(s, lookup) for s in strings]

        # Symbol table: 1-byte count, then each symbol as 1-byte len + bytes
        sym_buf = bytearray()
        sym_buf.append(len(symbols))
        for sym in symbols:
            sym_buf.append(len(sym))
            sym_buf.extend(sym)

        # Payload: sym_table | (2-byte-len + encoded_bytes) per string
        blob = bytearray()
        for es in encoded_strings:
            blob += struct.pack("<H", len(es))
            blob += es

        payload = bytes(sym_buf) + bytes(blob)

        return EncodedColumn(
            codec_name=self.name,
            data=payload,
            metadata={"n": len(data), "sym_bytes": len(sym_buf)},
            original_dtype=str(data.dtype),
            original_len=len(data),
        )

    def decode(self, encoded: EncodedColumn) -> np.ndarray:
        raw = encoded.data
        n = encoded.metadata["n"]
        sym_bytes = encoded.metadata["sym_bytes"]

        # Parse symbol table
        pos = 0
        n_syms = raw[pos]; pos += 1
        symbols: list[bytes] = []
        for _ in range(n_syms):
            sym_len = raw[pos]; pos += 1
            symbols.append(raw[pos : pos + sym_len])
            pos += sym_len

        pos = sym_bytes  # jump past symbol table

        strings = []
        for _ in range(n):
            (enc_len,) = struct.unpack_from("<H", raw, pos); pos += 2
            chunk = raw[pos : pos + enc_len]; pos += enc_len
            strings.append(_decode_string(chunk, symbols).decode())

        return np.array(strings, dtype=object)
