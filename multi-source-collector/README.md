# multi-source-collector

A multi-source ingestion engine that pulls data from **HTTP APIs**,
**CSV files**, **Excel workbooks**, **FTP servers**, and **Google
Sheets** into a single staging zone — with a strict naming convention
and a manifest-based idempotency guarantee.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

Five different upstream systems will give you five different folder
conventions, five different timestamp formats, and five different
"are we sure we already ingested this?" semantics. This package fixes
the convention at the ingestion boundary so every downstream stage
(transform, quality check, warehouse load) sees the same shape no
matter where the data came from.

## Architecture

```
   ┌───────────────┐   ┌───────────────┐   ┌──────────────┐
   │ HTTPAPISource │   │  CSVSource    │   │ ExcelSource  │   ...
   └──────┬────────┘   └──────┬────────┘   └──────┬───────┘
          │                   │                   │
          └────────── Source.fetch() → Record(...) ┘
                              │
                              ▼
                      ┌───────────────┐
                      │   Runner      │   NamingConvention → StagedKey
                      └──────┬────────┘   Manifest.has(...)  (idempotency)
                             │
                             ▼
                      ┌───────────────┐
                      │  StagingZone  │   atomic JSONL writes,
                      │  (filesystem) │   <source>/<dataset>/YYYY/MM/DD/<run>.jsonl
                      └──────┬────────┘
                             │
                             ▼
                      ┌───────────────┐
                      │   Manifest    │   append-only JSONL, sha256 + row_count
                      └───────────────┘
```

## Install

```bash
pip install -e ".[dev]"           # everything
pip install -e ".[excel]"         # adds openpyxl for Excel support
```

Python 3.10+. **Zero required runtime dependencies** (stdlib only).
Optional extras pull in `openpyxl` and `requests`.

## CLI

```bash
mscctl info
mscctl naming --source "HTTP API" --dataset "My Orders"
mscctl ingest-csv \
  --path samples/orders.csv \
  --dataset orders \
  --id-column id \
  --staging /tmp/staging \
  --manifest /tmp/manifest.jsonl
mscctl list-staging --staging /tmp/staging
mscctl manifest --manifest /tmp/manifest.jsonl --tail 5
```

Example `mscctl naming`:

```
path     = http_api/my_orders/2026/05/13/20260513T120000-d4f8e07a.jsonl
source   = http_api
dataset  = my_orders
run_id   = 20260513T120000-d4f8e07a
```

## Library

```python
from pathlib import Path

from msc.manifest         import Manifest
from msc.naming           import NamingConvention
from msc.runner           import Runner
from msc.sources.csv_src  import CSVSource
from msc.sources.http_api import HTTPAPISource
from msc.staging.zone     import StagingZone

zone = StagingZone(root=Path("/data/staging"))
mf   = Manifest(path=Path("/data/manifest.jsonl"))
run  = Runner(zone=zone, manifest=mf, naming=NamingConvention())

run.ingest(CSVSource(path=Path("orders.csv"), id_column="id"))
run.ingest(HTTPAPISource(
    url="https://api.example.com/v1/customers",
    dataset="customers",
    records_path="data.items",
    id_field="id",
))
```

Re-running the same source on the same UTC day is a **no-op**: the
manifest already records the `(source, dataset, run_id)` triple and
the runner returns `IngestionResult(skipped=True, ...)`.

## Components

| Module                       | Role                                                          |
| ---------------------------- | ------------------------------------------------------------- |
| `msc.naming`                 | `NamingConvention` + `StagedKey` (slug + UTC partition)       |
| `msc.manifest`               | `Manifest` / `ManifestEntry` (append-only JSONL, RLock)       |
| `msc.sources.base`           | `Source` ABC, `Record`, `SourceError`                         |
| `msc.sources.csv_src`        | `CSVSource` (stdlib `csv`)                                    |
| `msc.sources.excel`          | `ExcelSource` (optional `openpyxl`)                           |
| `msc.sources.http_api`       | `HTTPAPISource` (stdlib `urllib`, injectable fetcher)         |
| `msc.sources.ftp`            | `FTPSource` (stdlib `ftplib`, injectable connect)             |
| `msc.sources.gsheet`         | `GoogleSheetSource` (keyless CSV via GViz endpoint)           |
| `msc.staging.zone`           | `StagingZone` — atomic JSONL writer + listing                 |
| `msc.runner`                 | `Runner` — wires sources → staging → manifest                 |
| `msc.cli`                    | `mscctl` entry point                                          |

## Naming convention

```
<source>/<dataset>/<YYYY>/<MM>/<DD>/<run_id>.<ext>
```

- `source` and `dataset` are slugified (`a-z0-9_`, non-alnum → `_`).
- Partition is the **UTC** date of the run.
- `run_id` defaults to `<timestamp>-<sha256[:8]>` so the same
  `(source, dataset, ingestion_moment)` is reproducible byte-for-byte.

## Idempotency

The `Manifest` is the source of truth. Before the runner calls
`StagingZone.write`, it checks `manifest.has(source, dataset, run_id)`;
if the triple exists, the call returns the previous report with
`skipped=True` and zero new bytes. Two parallel runners pointed at the
same manifest path are safe — every public method takes an
`threading.RLock`.

## Source-adapter contract

Every adapter implements:

```python
class Source(ABC):
    kind: str         # naming-convention slug
    dataset: str      # logical table name

    def fetch(self) -> Iterator[Record]: ...
```

`Record` is `(source_id: str, fields: dict[str, Any])`. The runner
preserves `source_id` in the JSONL so downstream joins keep
provenance.

## Quality

```bash
make lint        # ruff
make format      # ruff format
make type        # mypy --strict
make test        # 50 tests
make docker      # production image
```

- **50 tests**, 0 failing; includes 1 Hypothesis property (naming
  always slugifies to `[a-z0-9_]+`).
- `mypy --strict` clean over 14 source files.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker build smoke step.
- Multi-stage slim Docker image, non-root `msc` user.
- **Zero required runtime dependencies** — stdlib only.

## License

MIT — see [LICENSE](LICENSE).
