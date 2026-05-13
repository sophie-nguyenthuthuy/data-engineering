# Changelog

## [0.1.0] — Initial public release

### Added

- `State` and `StateMachine` mirroring the TLA+ VARIABLES block
- 5 action functions, each = one TLA+ Next disjunct
  (pg_insert, debezium_publish, flink_consume, warehouse_load, reverse_etl)
- Safety invariants: WarehouseSubsetOfPg, RevETLSubsetOfWarehouse,
  KafkaSubsetOfPg, ExactlyOnceInAgg, BoundedLag
- Liveness: `EventualDeliveryWatcher` with `max_steps_to_delivery` threshold
- `Monitor.replay(events)` runtime checker
- Alert sinks: `ListAlertSink`, `ConsoleAlertSink`
- Connectors: synthetic PG WAL / Kafka publish & consume / DW changelog /
  reverse-ETL event streams
- Workload generators: `healthy_stream`, `buggy_stream(bug)` with bugs
  `kafka_lag`, `lost_publish`, `double_publish`
- `spec/pipeline.tla` full TLA+ spec with safety + liveness + fairness
- 36 tests across 7 modules
- CLI: `tlavpctl demo`, `tlavpctl replay`, `tlavpctl info`
- GitHub Actions CI matrix Python 3.10/3.11/3.12

### Limitations

- Synthetic connectors (no real Kafka/PG WAL adapters yet)
- In-process monitor (no distributed deployment)
- Liveness measured in steps, not wall-clock
- Fixed record shape (id, group, value)
