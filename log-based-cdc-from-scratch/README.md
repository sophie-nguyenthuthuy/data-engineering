# log-based-cdc-from-scratch

Protocol-level Change-Data-Capture readers — no Debezium, no driver
dependencies. Two parsers ship side-by-side:

- **MySQL binlog** v4 stream → typed events (`ROTATE`, `QUERY`,
  `TABLE_MAP`, `XID`, `WRITE/UPDATE/DELETE_ROWS_EVENTv2`).
- **Postgres `pgoutput`** logical-replication messages → typed
  messages (`BEGIN`, `COMMIT`, `RELATION`, `INSERT`, `UPDATE`,
  `DELETE`, `TRUNCATE`).

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

Debezium is excellent, but it brings a Kafka Connect runtime + a JVM
+ a non-trivial schema-registry dependency. When you only need to
ship CDC events into a Python service the right answer is often to
parse the wire format directly. This package shows the entire
parser surface in a few hundred lines of typed Python.

## Components

| Module                          | Role                                                             |
| ------------------------------- | ---------------------------------------------------------------- |
| `lcdc.lsn`                      | `LSN` (PG hex/hex format), `BinlogPosition` (file:position)      |
| `lcdc.mysql.header`             | 19-byte event header `(timestamp, type, server_id, size, …)`    |
| `lcdc.mysql.events`             | `Rotate / Query / TableMap / Xid / Rows` event decoders          |
| `lcdc.mysql.reader`             | `BinlogReader` — drains the magic + iterates events              |
| `lcdc.postgres.messages`        | `Begin / Commit / Relation / Insert / Update / Delete / Truncate`|
| `lcdc.postgres.reader`          | `PgOutputReader.decode` + `iter_messages`                        |
| `lcdc.cli`                      | `lcdcctl info | parse-mysql | parse-pg`                         |

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies.**

## CLI

```bash
lcdcctl info
lcdcctl parse-mysql --file mysql-bin.000001
lcdcctl parse-pg --hex 4200000000000012340000000000000000000000002A
```

## Library — MySQL

```python
import io
from lcdc.mysql.reader  import BinlogReader
from lcdc.mysql.events  import RowsEvent, QueryEvent, RowsEventKind

with open("mysql-bin.000001", "rb") as fh:
    for header, event in BinlogReader(stream=fh):
        if isinstance(event, QueryEvent) and event.query in ("BEGIN", "COMMIT"):
            continue
        if isinstance(event, RowsEvent) and event.kind == RowsEventKind.INSERT:
            print(f"INSERT into table_id={event.table_id} cols={event.column_count}")
```

The reader streams events lazily — the entire binlog does not need
to fit in memory. Unknown event types are returned as raw bytes so
nothing is silently dropped.

## Library — Postgres

Hook the decoder up to the standard ``CopyData`` rows from
`pg_logical_emit_message` / `psycopg.replication`:

```python
from lcdc.postgres.reader   import PgOutputReader
from lcdc.postgres.messages import InsertMessage, UpdateMessage, DeleteMessage

decoder = PgOutputReader()
for msg in decoder.iter_messages(my_copydata_payload_stream()):
    if isinstance(msg, InsertMessage):
        cols = [c.value for c in msg.new_tuple.columns]
        ...
```

`TupleColumn` flags each value with its `TupleColumnKind` (`NULL`,
`UNCHANGED` for TOAST chunks omitted by Postgres, `TEXT`, `BINARY`).

## Wire-format coverage

### MySQL binlog header
```
4  timestamp       seconds since epoch (uint32 LE)
1  event_type
4  server_id       (uint32 LE)
4  event_size      includes the 19-byte header
4  log_pos         next-event position
2  flags
```

### Postgres pgoutput message tags
```
B   BEGIN
C   COMMIT
R   RELATION
I   INSERT
U   UPDATE
D   DELETE
T   TRUNCATE
Y   TYPE        (not decoded — emitted as raw)
O   ORIGIN      (not decoded — emitted as raw)
```

## Quality

```bash
make test       # 56 tests
make type       # mypy --strict
make lint
```

- **56 tests**, 0 failing; includes 1 Hypothesis property (LSN
  string ↔ value round-trip) and the binlog header round-trip
  under random fields.
- mypy `--strict` clean over 11 source files; ruff clean.
- Multi-stage slim Docker image, non-root `lcdc` user.
- Python 3.10 / 3.11 / 3.12 CI matrix.
- Zero runtime dependencies.

## License

MIT — see [LICENSE](LICENSE).
