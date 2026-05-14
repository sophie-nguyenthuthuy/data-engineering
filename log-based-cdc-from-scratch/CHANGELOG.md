# Changelog

## [0.1.0] — 2026-05-13

### Added
- `LSN` (Postgres hex/hex) + `BinlogPosition` (MySQL file:position).
- MySQL binlog v4 parser:
  * `EventHeader` (19-byte) with byte-exact encode / decode.
  * Decoders for `ROTATE`, `QUERY`, `XID`, `TABLE_MAP`,
    `WRITE/UPDATE/DELETE_ROWS_EVENTv2`.
  * `BinlogReader` streaming iterator with magic-byte check,
    truncated-payload detection, and raw-bytes fallback for
    unknown events.
- Postgres pgoutput decoder:
  * `BeginMessage`, `CommitMessage`, `RelationMessage`,
    `InsertMessage`, `UpdateMessage`, `DeleteMessage`,
    `TruncateMessage`.
  * `TupleData` with `TupleColumnKind` (NULL / UNCHANGED / TEXT /
    BINARY).
  * `PgOutputReader.decode` + `iter_messages`.
- CLI `lcdcctl info | parse-mysql | parse-pg`.
- 56 tests including 1 Hypothesis property: LSN str↔value round-trip
  + binlog header encode/decode round-trip under random fields.
- mypy `--strict` clean, ruff clean, multi-stage Docker.
- Zero runtime dependencies.

### Notes
- Row-event payloads expose the **raw image bytes**; per-column
  decoding requires the `TableMapEvent` schema that immediately
  precedes them and is intentionally left to the consumer.
- The MySQL length-encoded-int helper handles `< 0xFB`, `0xFC`
  (2-byte), `0xFD` (3-byte), `0xFE` (8-byte) prefixes.
