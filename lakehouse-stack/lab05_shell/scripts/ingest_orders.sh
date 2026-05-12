#!/bin/bash
set -e

BASE_DIR="/Users/Thuy/lakehouse-stack/lab05_shell"
INPUT_FILE="$BASE_DIR/incoming/orders.csv"
OUTPUT_FILE="$BASE_DIR/raw/orders_clean.csv"
LOG_FILE="$BASE_DIR/logs/etl.log"

mkdir -p "$BASE_DIR/raw" "$BASE_DIR/logs"

if [ ! -f "$INPUT_FILE" ]; then
  echo "[ERROR] File not found: $INPUT_FILE" >> "$LOG_FILE"
  exit 1
fi

HEADER=$(head -n 1 "$INPUT_FILE")
if [ "$HEADER" != "order_id,customer_id,order_date,amount,status" ]; then
  echo "[ERROR] Invalid schema" >> "$LOG_FILE"
  exit 1
fi

awk -F',' 'NR==1 || $4 > 0 {print $0}' "$INPUT_FILE" > "$OUTPUT_FILE"
echo "[INFO] ETL success: $(date)" >> "$LOG_FILE"
