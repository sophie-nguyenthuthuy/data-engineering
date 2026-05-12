"""Demonstrates per-column encoding selection and benchmarking."""
import numpy as np

import cctest


def make_table(n: int = 50_000) -> dict[str, np.ndarray]:
    rng = np.random.default_rng(0)
    symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "META"]
    return {
        "ts": np.cumsum(np.concatenate([[1_700_000_000_000], rng.integers(950, 1050, n - 1)])).astype(np.int64),
        "symbol": np.array([symbols[i] for i in rng.integers(0, len(symbols), n)], dtype=object),
        "price": np.round(rng.uniform(100.0, 500.0, n) * 100) / 100,
        "volume": np.round(rng.uniform(1.0, 1000.0, n) * 10) / 10,
    }


def main() -> None:
    print("=" * 60)
    print("Columnar Compression Research Testbed – Basic Usage")
    print("=" * 60)

    table = make_table()

    # --- Per-column adaptive selection ---
    print("\n[1] Adaptive selector: choosing best codec per column")
    selector = cctest.EncodingSelector(config=cctest.SelectorConfig(sample_size=4096))
    for col_name, col_data in table.items():
        codec = selector.select(col_name, col_data)
        print(f"  {col_name:<10} (dtype={col_data.dtype!s:<10}) → {codec.name}")

    # --- Full benchmark comparison ---
    print("\n[2] Full codec benchmark per column")
    for col_name, col_data in table.items():
        results = cctest.run_column_benchmark(col_data, label=col_name)
        cctest.print_benchmark(results, label=f"{col_name} ({col_data.dtype})")

    # --- ColumnStore round-trip ---
    print("\n[3] ColumnStore: insert → retrieve")
    store = cctest.ColumnStore()
    store.insert(table)
    retrieved = store.retrieve()

    for name in table:
        orig = table[name]
        retr = retrieved[name]
        if orig.dtype.kind in ("U", "O"):
            ok = list(orig) == list(retr)
        else:
            ok = np.allclose(orig, retr, equal_nan=True)
        print(f"  {name:<10} round-trip {'OK' if ok else 'FAIL'}")

    print("\n[4] Compression summary")
    summary = store.compression_summary()
    for col, info in summary.items():
        print(
            f"  {col:<10} codec={info['codec']:<20} "
            f"ratio={info['ratio']:.2f}x  "
            f"({info['original_bytes']:,} → {info['compressed_bytes']:,} bytes)"
        )


if __name__ == "__main__":
    main()
