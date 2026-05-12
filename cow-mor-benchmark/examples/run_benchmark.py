"""Quick example: run a single benchmark and print the recommendation."""

from cow_mor_bench.benchmark.runner import run_benchmark
from cow_mor_bench.benchmark.metrics import compare
from cow_mor_bench.workload.classifier import classify_trace
from cow_mor_bench.workload.patterns import PROFILES
from cow_mor_bench.recommender.engine import recommend

profile = PROFILES["oltp"]
result = run_benchmark(profile, schema_name="orders", table_size=10_000, n_ops=40)

print("=== Metric Comparison ===")
for row in compare(result):
    print(f"  {row.metric:<30} CoW={row.cow_value:<12} MoR={row.mor_value:<12}  winner={row.winner}")

classification = classify_trace(result.cow_trace)
rec = recommend(result, classification, table_name="orders_oltp")

print()
print(rec.summary())
