# Pipeline Topology Compiler

A DSL for declaring data pipelines as typed DAGs that **compiles** to Spark, Flink, or dbt based on declared latency SLAs and dataset sizes — and **verifies** that the compiled output is logically equivalent to the spec.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   DSL (input)                           │
│  YAML definition            Python decorator DSL        │
│  pipeline: ...              @pipeline(sla=SLA(...))     │
│    sla: {latency: 5m}       class MyPipeline:           │
│    sources: [...]             @source(schema=...)       │
│    transforms: [...]          @transform(inputs=[...])  │
│    sink: {...}                @sink(input=...)          │
└───────────────────┬─────────────────────────────────────┘
                    │ parse / introspect
                    ▼
┌─────────────────────────────────────────────────────────┐
│            Intermediate Representation (IR)             │
│  PipelineSpec                                           │
│    nodes: dict[str, TransformNode]                      │
│    sla: SLA(max_latency, dataset_size)                  │
│    → topological_order(), infer_schemas(), validate()   │
└──────────┬───────────────────┬──────────────────────────┘
           │ select_target()   │ optimize()
           │                   │  - filter push-down
           │                   │  - merge consecutive SELECTs
           │                   │  - eliminate identity SELECTs
           ▼                   ▼
┌─────────────────────────────────────────────────────────┐
│                  Target Selection                       │
│  latency < 5min  → Flink (streaming)                   │
│  latency < 60min │ size > 100GB → Spark                 │
│  otherwise       → dbt (batch / data warehouse)        │
└──────────────────────────┬──────────────────────────────┘
                           │ generate()
                           ▼
┌─────────────────────────────────────────────────────────┐
│                Code Generators                          │
│  SparkTarget  → PySpark DataFrame API  (*_spark.py)     │
│  FlinkTarget  → PyFlink Table API SQL  (*_flink.py)     │
│  DbtTarget    → SQL CTEs + schema.yml + dbt_project.yml │
└──────────────────────────┬──────────────────────────────┘
                           │ CompiledArtifact
                           ▼
┌─────────────────────────────────────────────────────────┐
│           Proof-of-Equivalence Checker                  │
│  1. Node-set completeness — all spec nodes compiled     │
│  2. Sink schema equivalence — field names & types match │
│  3. Semantic equivalence — per-node predicate / group-  │
│     by / aggregation / join parameters match            │
│  4. DAG topology — edge connectivity preserved          │
│                                                         │
│  Normalizer (before comparison):                        │
│    - inline identity SELECTs                           │
│    - sort AND clauses in predicates                     │
│    - sort aggregations by output name                   │
│    - sort SELECT column lists                           │
└─────────────────────────────────────────────────────────┘
```

---

## Quick start

```bash
pip install -e ".[dev]"
```

### Compile a YAML pipeline

```bash
ptc compile examples/batch_pipeline.yaml
# → selects dbt, writes compiled/ecommerce_daily_stats/dbt/

ptc compile examples/streaming_pipeline.yaml
# → selects flink, writes compiled/realtime_fraud_detection/flink/

ptc compile examples/batch_pipeline.yaml --target spark --verify
# → compiles to Spark and runs the equivalence checker
```

### Inspect target selection

```bash
ptc inspect examples/batch_pipeline.yaml
```

```
Pipeline: ecommerce_daily_stats
  SLA:  SLA(latency=6h, size=80gb)
  Nodes (7):
    [source      ] orders  → Schema(order_id: string!, ...)
    [source      ] customers
    [filter      ] completed_orders
    [join        ] orders_with_customer
    [aggregate   ] daily_segment_stats
    [select      ] final_report
    [sink        ] customer_segment_daily_stats

  Recommended target: DBT
  Reason: batch pipeline (latency=21600s, size=80.0GB) maps to SQL/dbt
```

### Python API

```python
from pipeline_topology import parse_yaml, select_target, optimize, get_target, check_equivalence

spec   = parse_yaml("examples/batch_pipeline.yaml")
reason = select_target(spec)          # SelectionReason(target=dbt, ...)
opt    = optimize(spec)               # filter push-down + select merging
gen    = get_target(reason.target)    # DbtTarget / SparkTarget / FlinkTarget
art    = gen.generate(opt)            # CompiledArtifact with file dict

art.write_to("output/")

report = check_equivalence(spec, art)
assert report.is_equivalent()
```

### Decorator DSL

```python
from pipeline_topology.dsl import pipeline, source, transform, sink, field, Schema, SLA

@pipeline(name="my_pipeline", sla=SLA(max_latency="30m", dataset_size="200gb"))
class MyPipeline:

    @source(schema=Schema([
        field("user_id", "string", nullable=False),
        field("amount",  "double"),
        field("region",  "string"),
    ]))
    def transactions(self): ...

    @transform(inputs=["transactions"])
    def large_txns(self, transactions):
        return transactions.filter("amount > 1000")

    @transform(inputs=["large_txns"])
    def by_region(self, large_txns):
        return (
            large_txns
            .groupby(["region"])
            .agg(total=("amount", "sum"), cnt=("amount", "count"))
        )

    @sink(input="by_region")
    def regional_large_txns(self, by_region): ...
```

---

## SLA → Target decision matrix

| `max_latency`   | `dataset_size` | Target  | Reason                              |
|-----------------|---------------|---------|-------------------------------------|
| < 5 min         | any           | Flink   | streaming / near-realtime           |
| 5 min – 60 min  | any           | Spark   | micro-batch or large-scale batch    |
| > 60 min        | > 100 GB      | Spark   | data volume exceeds SQL practicality|
| > 60 min        | ≤ 100 GB      | dbt     | warehouse-scale batch SQL           |

---

## Running tests

```bash
pytest
pytest --cov=pipeline_topology --cov-report=term-missing
```

---

## Project layout

```
pipeline_topology/
├── dsl/
│   ├── types.py          # FieldType, Schema, SLA, TransformType, Aggregation
│   ├── ir.py             # TransformNode, PipelineSpec
│   ├── yaml_parser.py    # YAML → PipelineSpec
│   └── decorator_dsl.py  # @pipeline/@source/@transform/@sink
├── compiler/
│   ├── selector.py       # SLA-based target selection
│   └── optimizer.py      # filter push-down, select merging
├── targets/
│   ├── spark_target.py   # PySpark DataFrame API codegen
│   ├── flink_target.py   # PyFlink Table API codegen
│   └── dbt_target.py     # dbt SQL + YAML config codegen
└── checker/
    ├── normalizer.py     # algebraic normalization passes
    └── equivalence.py    # proof-of-equivalence checker
```
