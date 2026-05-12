# Columnar Compression Research Testbed

A pure-Python research testbed that implements three production-grade columnar encoding strategies side-by-side, with an adaptive per-column selector and schema-evolution tracking.

## Algorithms

| Codec | Best for | Mechanism |
|-------|----------|-----------|
| **FSST** | String / categorical columns | Static symbol table (n-grams 2–8 bytes), greedy gain selection, 1-byte codes |
| **ALP** | Decimal floating-point | Finds optimal exponent e; encodes as `round(v × 10^e)` + frame-of-reference bitpacking; exceptions stored separately |
| **Gorilla Float** | Time-series floats | XOR-delta with leading/trailing zero suppression; reuses prior bit-block header when XOR fits inside it |
| **Gorilla Delta** | Timestamps / monotone ints | Delta-of-delta with variable-length codes (1–68 bits per value) |

## Key features

- **Per-column encoding selector** — samples up to 8 192 rows, benchmarks every applicable codec, and commits to the best compression ratio. Results are cached keyed on `(column_name, dtype)`.
- **Schema evolution** — `SchemaEvolutionTracker` detects added, removed, or type-changed columns across batches and evicts stale codec selections so the next write re-evaluates automatically.
- **Column store** — `ColumnStore` ties everything together: insert dict-of-arrays → compressed storage → lossless retrieval.

## Compression results (50 000-row market-data table)

```
ts         gorilla_delta   6.88x   85.5% saving
symbol     fsst            2.66x   62.5% saving
price      alp             4.00x   75.0% saving
volume     alp             4.57x   78.1% saving
```

## Quick start

```bash
pip install -e ".[dev]"
python examples/basic_usage.py
python examples/schema_evolution.py
pytest
```

## Project layout

```
src/cctest/
├── codecs/
│   ├── base.py        # Codec ABC, EncodedColumn, BenchmarkResult
│   ├── _bits.py       # BitWriter / BitReader (Gorilla I/O)
│   ├── fsst.py        # FSST string compression
│   ├── alp.py         # ALP floating-point compression
│   └── gorilla.py     # Gorilla float + delta codecs
├── selector.py        # Per-column adaptive encoding selector
├── schema.py          # Schema diff & evolution tracker
├── benchmark.py       # Column / table benchmark harness
└── column_store.py    # In-memory column store
```

## References

- Boncz et al., *"FSST: Fast Random Access String Compression"*, VLDB 2020
- Afroozeh et al., *"ALP: Adaptive Lossless floating-Point Compression"*, SIGMOD 2023
- Pelkonen et al., *"Gorilla: A Fast, Scalable, In-Memory Time Series Database"*, VLDB 2015
