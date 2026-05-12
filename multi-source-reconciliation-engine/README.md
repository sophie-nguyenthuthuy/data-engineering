# Multi-Source Reconciliation Engine

A financial reconciliation pipeline that ingests the same transaction data from **four independent sources**, runs **multi-key fuzzy matching**, classifies discrepancies by type, and produces a detailed report with confidence scores — all within a **15-minute SLA**.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     4 Data Sources                           │
│  Core Banking │ Reporting System │ Aggregator │ Manual Entries│
└──────────┬───────────┬──────────────┬────────────┬───────────┘
           │           │              │            │
           └───────────┴──────┬───────┴────────────┘
                              ▼
                    ┌─────────────────┐
                    │  Ingestion Layer│  (normalize to common schema)
                    └────────┬────────┘
                             ▼
                    ┌─────────────────┐
                    │ Matching Engine │  (fuzzy multi-key: ref, amount,
                    │                 │   description, date)
                    └────────┬────────┘
                             ▼
                    ┌─────────────────┐
                    │   Classifier    │  (timing / rounding / missing /
                    │                 │   amount_mismatch / multi)
                    └────────┬────────┘
                             ▼
                    ┌─────────────────┐
                    │ Report Generator│  (JSON + HTML with confidence scores)
                    └─────────────────┘
```

---

## Discrepancy Types

| Type | Description | Severity |
|---|---|---|
| `rounding` | Amount diff ≤ $0.05 | LOW |
| `timing` | Same transaction, dates differ ≤ 3 days | LOW – MEDIUM |
| `missing` | Transaction absent from one or more sources | MEDIUM – HIGH |
| `amount_mismatch` | Significant amount difference | MEDIUM – CRITICAL |
| `multi` | Multiple discrepancy types present | varies |

---

## Matching Logic

Each pair of transactions is scored using a weighted composite:

| Key | Weight | Method |
|---|---|---|
| Reference number | 40% | Exact / fuzzy token-set ratio |
| Amount | 35% | Tolerance-based (rounding / pct diff) |
| Description | 15% | RapidFuzz token sort ratio |
| Date | 10% | Day-delta penalty |

A pair is matched when the composite score ≥ **80** (configurable).

---

## Quick Start

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run against sample data

```bash
bash scripts/run_sample.sh
```

Or directly:

```bash
python main.py \
  --core-banking  data/samples/core_banking.csv \
  --reporting     data/samples/reporting_system.csv \
  --aggregator    data/samples/aggregator.csv \
  --manual        data/samples/manual_entries.csv
```

Reports are written to `reports/` in both JSON and HTML formats.

### Run tests

```bash
pytest tests/ -v
```

---

## Configuration

All thresholds are in [`config/settings.yaml`](config/settings.yaml):

```yaml
reconciliation:
  sla_minutes: 15
  rounding_threshold: 0.05     # max $ diff for rounding classification
  timing_day_window: 3         # max days diff for timing classification
  fuzzy_match_threshold: 80    # min composite score (0-100) to form a group

matching:
  weights:
    reference: 0.40
    amount: 0.35
    description: 0.15
    date: 0.10
```

---

## Project Structure

```
multi-source-reconciliation-engine/
├── main.py                   # CLI entry point
├── config/settings.yaml      # All tuneable thresholds
├── src/
│   ├── ingestion/loader.py   # Source normalization
│   ├── matching/engine.py    # Fuzzy multi-key matcher
│   ├── classification/       # Discrepancy classifier
│   ├── reporting/            # JSON + HTML report generator
│   └── pipeline.py           # Orchestrator + SLA tracking
├── data/samples/             # Sample CSV files for all 4 sources
├── tests/test_pipeline.py    # Unit + integration tests
└── scripts/run_sample.sh     # One-command demo
```

---

## Sample Report Output

The HTML report includes:
- KPI cards: total groups, clean %, discrepancy count, high/critical count
- Discrepancy type breakdown table
- Per-group detail table with confidence scores and expandable raw details
