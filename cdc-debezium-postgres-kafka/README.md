# cdc-debezium-postgres-kafka

A type-safe Python toolkit for the **Postgres → Debezium → Kafka** CDC
path. Parser for the Debezium event envelope, stateless transforms
(flatten, PII masking, column rename), a DLQ router that classifies
malformed events, and an Avro schema generator compatible with
Confluent Schema Registry.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

The Debezium event envelope is well-specified but the consumer side is
nearly always re-implemented from scratch — with predictable bugs around
`op` discrimination, before/after invariants, and what to do with a
malformed payload. This package gives you one toolkit covering the
parser, the transform chain, the DLQ classifier, and the schema
generator, all `mypy --strict` clean.

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies** — stdlib only.

## CLI

```bash
cdcctl info
cat event.json | cdcctl parse
cat event.json | cdcctl validate
cdcctl schemagen \
  --namespace cdc.public --name Orders \
  --column id:int --column name:text:nullable
```

## Library

```python
from cdc.dlq.router         import DLQDecision, DLQRouter
from cdc.events.envelope    import Op
from cdc.events.parse       import parse_envelope
from cdc.pipeline           import Pipeline
from cdc.schema.avro        import generate_avro_schema
from cdc.transforms.flatten import FlattenAfter
from cdc.transforms.mask_pii import MaskPII
from cdc.transforms.rename  import RenameColumns

# 1. Parse one Debezium event.
env = parse_envelope(raw_bytes_from_kafka)

# 2. Run a transform chain.
pipeline = Pipeline(
    router=DLQRouter(custom_check=lambda e: e.source.table == "orders"),
    transforms=[
        RenameColumns(mapping={"customer_email": "email"}),
        MaskPII(columns=("email", "ssn")),
        FlattenAfter(),
    ],
)
result = pipeline.run(kafka_payload_stream)
print(result.success_rate(), len(result.dlq))

# 3. Generate an Avro schema for the flattened row.
schema = generate_avro_schema(
    namespace="cdc.public",
    name="Orders",
    columns=[("id", "int", False), ("email", "text", True)],
)
```

## Architecture

```
   bytes  ─▶ DLQRouter ──▶ DebeziumEnvelope ──▶ Transform[]  ──▶ clean[]
                │                                    │
                │ DLQDecision(reason, message)      │ transform_failure[]
                ▼                                    ▼
            dlq[]                              quarantine
```

## Components

| Module                    | Role                                                    |
| ------------------------- | ------------------------------------------------------- |
| `cdc.events.envelope`     | `Op`, `SourceInfo`, `DebeziumEnvelope` w/ invariants    |
| `cdc.events.parse`        | JSON → `DebeziumEnvelope`, `ParseError`                 |
| `cdc.transforms.base`     | `Transform` ABC                                         |
| `cdc.transforms.flatten`  | `FlattenAfter` (picks `after`, or `before` for deletes) |
| `cdc.transforms.mask_pii` | `MaskPII` (column-name + regex masking)                 |
| `cdc.transforms.rename`   | `RenameColumns` (no-dup destinations)                   |
| `cdc.dlq.router`          | `DLQRouter` w/ reason taxonomy + counters               |
| `cdc.schema.avro`         | `postgres_to_avro`, `generate_avro_schema`              |
| `cdc.pipeline`            | `Pipeline` + `PipelineResult` (clean / dlq / fail)      |
| `cdc.cli`                 | `cdcctl info | parse | validate | schemagen`            |

## Envelope invariants

`DebeziumEnvelope` rejects any of the following at construction time:

- `op="c"` or `op="r"` with no `after`.
- `op="d"` with no `before`.
- `op="u"` missing either `before` or `after`.
- Negative `ts_ms`.

The parser surfaces these as `ParseError` so the DLQ router can pin
the failure cause.

## DLQ reasons

| Reason          | When                                            |
| --------------- | ----------------------------------------------- |
| `parse_error`   | Payload is not valid Debezium JSON.             |
| `missing_field` | Required envelope field is absent.              |
| `unknown_op`    | `op` is not one of `c`, `u`, `d`, `r`.          |
| `custom`        | Caller-supplied predicate returned False.       |

`DLQRouter.counts()` lets you wire a Prometheus collector with one
gauge per reason.

## Avro schema generation

`postgres_to_avro(pg_type, nullable=)` maps the common Postgres types
to their Avro equivalents:

- `integer/int/int4` → `"int"`
- `bigint/int8` → `"long"`
- `boolean` → `"boolean"`
- `text/varchar/char` → `"string"`
- `uuid` → `{"type": "string", "logicalType": "uuid"}`
- `timestamp[tz]` → `{"type": "long", "logicalType": "timestamp-millis"}`
- `numeric` → `{"type": "bytes", "logicalType": "decimal", ...}`
- unknown → `{"type": "string", "logicalType": "unknown"}` (safe fallback)

`generate_avro_schema(namespace, name, columns)` produces a record
schema; nullable columns are emitted as `["null", T]` unions with
`default: null` (matching Schema Registry's compatibility rules).

## Quality

```bash
make lint        # ruff
make format
make type        # mypy --strict
make test        # 50+ tests
```

- **53 tests**, mypy `--strict` clean over 14 source files.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker build smoke step.
- Multi-stage slim Docker image, non-root `cdc` user.
- **Zero runtime dependencies** — stdlib only.

## License

MIT — see [LICENSE](LICENSE).
