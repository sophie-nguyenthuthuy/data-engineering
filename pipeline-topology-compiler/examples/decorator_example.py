"""
Decorator DSL example — the same pipeline as batch_pipeline.yaml,
declared in pure Python using the @pipeline / @source / @transform / @sink decorators.
"""
from pipeline_topology.dsl.decorator_dsl import (
    extract_spec,
    field,
    pipeline,
    sink,
    source,
    transform,
)
from pipeline_topology.dsl.types import Schema, SLA
from pipeline_topology import select_target, optimize, get_target, check_equivalence


@pipeline(
    name="ecommerce_daily_stats_py",
    sla=SLA(max_latency="6h", dataset_size="80gb"),
    description="Daily order statistics — decorator-DSL version",
)
class EcommercePipeline:
    @source(
        schema=Schema([
            field("order_id",    "string",    nullable=False),
            field("customer_id", "string",    nullable=False),
            field("amount",      "double",    nullable=False),
            field("status",      "string",    nullable=True),
            field("created_at",  "timestamp", nullable=False),
        ]),
        location="/data/raw/orders",
        format="parquet",
    )
    def orders(self): ...

    @source(
        schema=Schema([
            field("customer_id", "string", nullable=False),
            field("segment",     "string", nullable=True),
            field("region",      "string", nullable=True),
        ]),
        location="/data/raw/customers",
        format="parquet",
    )
    def customers(self): ...

    @transform(inputs=["orders"])
    def completed_orders(self, orders):
        return orders.filter("status == 'completed'")

    @transform(inputs=["completed_orders", "customers"])
    def orders_with_customer(self, completed_orders, customers):
        return completed_orders.join(
            customers,
            on="completed_orders.customer_id == customers.customer_id",
            how="inner",
        )

    @transform(inputs=["orders_with_customer"])
    def daily_segment_stats(self, orders_with_customer):
        return (
            orders_with_customer
            .groupby(["segment", "region"])
            .agg(
                total_orders=("order_id", "count"),
                total_revenue=("amount", "sum"),
                avg_order_value=("amount", "avg"),
                max_order_value=("amount", "max"),
            )
        )

    @transform(inputs=["daily_segment_stats"])
    def final_report(self, daily_segment_stats):
        return daily_segment_stats.select(
            "segment", "region", "total_orders", "total_revenue", "avg_order_value"
        )

    @sink(input="final_report", location="/data/output/customer_segment_daily_stats", format="parquet")
    def customer_segment_daily_stats(self, final_report): ...


if __name__ == "__main__":
    spec = extract_spec(EcommercePipeline)
    print("=== Pipeline spec ===")
    print(spec)
    print()

    reason = select_target(spec)
    print(f"=== Target selection ===")
    print(f"Target: {reason.target.value.upper()}")
    print(f"Reason: {reason.reason}")
    print()

    optimized = optimize(spec)
    generator = get_target(reason.target)
    artifact = generator.generate(optimized)

    print("=== Generated code ===")
    for fname, code in artifact.files.items():
        print(f"--- {fname} ---")
        print(code[:1500])
        if len(code) > 1500:
            print(f"  ... ({len(code) - 1500} more chars)")
        print()

    print("=== Equivalence check ===")
    report = check_equivalence(spec, artifact)
    print(report)
