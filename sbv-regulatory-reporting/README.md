# SBV Regulatory Reporting Automation

An automated pipeline that transforms raw bank transaction data into **State Bank of Vietnam (SBV)** regulatory report formats, complete with immutable audit trail and reconciliation checks.

## Reports Generated

| Code | Name | Trigger |
|------|------|---------|
| `BCGD` | Báo cáo Giao dịch Ngày (Daily Transaction Report) | Daily |
| `B01-TCTD` | Báo cáo Số dư & Khối lượng (Balance/Volume Summary) | Monthly |
| `BCGDLN` | Báo cáo Giao dịch Lớn (Large-Value Transactions ≥ 300M VND) | Daily |
| `BCGDNS` | Báo cáo Giao dịch Đáng ngờ (Suspicious Transaction Report) | On-event |

## Architecture

```
raw CSV
   │
   ▼
RawTransactionLoader       ← normalise, validate, warn
   │
   ▼
SBVTransformer             ← build BCGD / B01-TCTD / BCGDLN / BCGDNS
   │
   ├──► ReconciliationEngine  ← 5 checks: row count, VND total,
   │                             tx IDs, per-currency totals,
   │                             large-value coverage
   │
   ├──► ReportWriter          ← Excel (multi-sheet) + CSV
   │
   └──► AuditTrail            ← SHA-256 chained JSONL log
```

### STR Flagging Rules

| Rule | Description |
|------|-------------|
| R1 | Single transaction ≥ 300M VND (SBV Circular 09/2023 threshold) |
| R2 | Account with ≥ 10 transactions in one day |
| R3 | Round-amount VND transactions ≥ 100M (multiples of 100M) |
| R4 | Structuring — amount between 90%–100% of reporting threshold |

### Audit Trail

Every pipeline event writes a tamper-evident entry to `data/output/audit/audit_<run_id>.jsonl`.  
Each entry includes a SHA-256 hash chained from the previous entry — altering any past record breaks the chain, detectable via `sbv-report verify`.

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Generate sample data (500 transactions)
python data/sample/generate_sample.py

# Run the pipeline
python cli.py run data/sample/transactions.csv --date 31/03/2025 --operator OPS001

# Verify audit chain integrity
python cli.py verify data/output/audit/audit_<run_id>.jsonl

# Run tests
pytest
```

## Output Structure

```
data/output/
├── reports/
│   ├── SBV_REPORT_<run_id>_<date>.xlsx     ← all 4 reports, one sheet each
│   ├── BCGD_<run_id>_<date>.csv
│   ├── B01-TCTD_<run_id>_<date>.csv
│   ├── BCGDLN_<run_id>_<date>.csv
│   └── BCGDNS_<run_id>_<date>.csv
└── audit/
    └── audit_<run_id>.jsonl                ← tamper-evident chain log
```

## Configuration

Edit `config/sbv_config.yaml` to set:
- Institution name, code, SWIFT
- Reporting thresholds (large-value, suspicious frequency)
- Date/number formatting
- Audit log retention

## Project Layout

```
sbv-regulatory-reporting/
├── cli.py
├── config/sbv_config.yaml
├── data/sample/
│   ├── generate_sample.py
│   └── transactions.csv
├── src/sbv_reporting/
│   ├── pipeline.py            ← orchestrator
│   ├── audit/trail.py         ← chained SHA-256 audit log
│   ├── reconciliation/checks.py
│   ├── reports/writer.py      ← Excel + CSV output
│   ├── transformers/
│   │   ├── base.py            ← CSV loader + normaliser
│   │   └── sbv_formats.py     ← SBV report builders
│   └── utils/
│       ├── config.py
│       └── validators.py
└── tests/
```

## Regulatory References

- **SBV Circular 09/2023/TT-NHNN** — large-value transaction reporting threshold (300M VND / $10,000 USD)
- **SBV Circular 01/2014/TT-NHNN** — record retention requirements (7 years)
- **Decree 116/2013/NĐ-CP** — AML reporting obligations
