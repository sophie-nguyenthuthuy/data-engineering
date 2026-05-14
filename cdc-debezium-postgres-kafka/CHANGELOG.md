# Changelog

## [0.1.0] — 2026-05-14

### Added
- **Debezium envelope** (`cdc.events.envelope`) — `Op`, `SourceInfo`,
  `DebeziumEnvelope` with per-op before/after invariants enforced at
  construction time + `primary_key(key_columns)` helper.
- **Parser** (`cdc.events.parse`) — JSON → envelope with `ParseError`
  diagnostics; rejects malformed JSON, non-object roots, missing
  required keys, wrong types, and unknown ops.
- **Transforms** (`cdc.transforms.{flatten,mask_pii,rename}`) — stateless
  `Transform` ABC with three builtins; all reject ill-formed config.
- **DLQ router** (`cdc.dlq.router`) — classifies parse failures into
  `parse_error`, `missing_field`, `unknown_op`, or `custom`; counts
  reasons for metric export.
- **Avro schema generator** (`cdc.schema.avro`) — `postgres_to_avro` +
  `generate_avro_schema` emitting Schema-Registry-compatible records;
  unknown Postgres types fall back to `{"type": "string",
  "logicalType": "unknown"}` so the output is always valid Avro.
- **Pipeline** (`cdc.pipeline`) — DLQ router + transform chain over a
  stream of raw payloads; `PipelineResult` exposes `clean`, `dlq`,
  `transform_failures`, `success_rate`.
- **CLI** (`cdcctl info | parse | validate | schemagen`).
- **Docker Compose** stack (`docker-compose.yml`) — full
  Postgres + Debezium Connect + Kafka + Schema Registry stack for
  end-to-end demos.
- **56 tests**, mypy `--strict` clean over 15 source files.
- **Zero runtime dependencies**.

### Notes
- The `DLQRouter` originally bucketed every parse failure as
  `parse_error`. Fixed to inspect the `ParseError` message and route
  unknown-op events to `unknown_op` and missing-key events to
  `missing_field` — matches the documented reason taxonomy and the
  three failing tests.
- ruff TC003 surfaced unnecessary runtime imports of `Iterable`,
  `Sequence`, `re`, and `DebeziumEnvelope` (annotation-only). Moved
  into `TYPE_CHECKING` blocks.
- UP038 flagged `isinstance(payload, (bytes, str))`; replaced with
  Python 3.10+ `isinstance(payload, bytes | str)`.
