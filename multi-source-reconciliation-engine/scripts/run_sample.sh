#!/usr/bin/env bash
# Run the reconciliation engine against the bundled sample data.
set -euo pipefail

cd "$(dirname "$0")/.."

python main.py \
  --core-banking  data/samples/core_banking.csv \
  --reporting     data/samples/reporting_system.csv \
  --aggregator    data/samples/aggregator.csv \
  --manual        data/samples/manual_entries.csv \
  "$@"
