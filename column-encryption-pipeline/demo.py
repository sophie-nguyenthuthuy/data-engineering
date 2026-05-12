#!/usr/bin/env python3
"""
End-to-end demo of the column-level encryption pipeline.

Demonstrates:
  1. Customer registration → CMK creation in KMS
  2. PII ingest with column-level encryption
  3. Decryption / plaintext read-back
  4. Live key rotation with dual-read window
  5. Right-to-be-Forgotten (crypto-shredding)
  6. Erasure verification
"""

import os
import sys
import json
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure local mode (no AWS needed)
# ---------------------------------------------------------------------------
os.environ.setdefault("KMS_MODE", "local")
os.environ.setdefault("STORAGE_MODE", "local")

_tmpdir = tempfile.mkdtemp(prefix="enc_demo_")
os.environ["LOCAL_KMS_STORE_PATH"] = f"{_tmpdir}/kms_store.json"
os.environ["LOCAL_KMS_MASTER_KEY_PATH"] = f"{_tmpdir}/master.key"
os.environ["KEY_REGISTRY_PATH"] = f"{_tmpdir}/key_registry.json"
os.environ["LOCAL_STORAGE_PATH"] = f"{_tmpdir}/records"

# ---------------------------------------------------------------------------

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from src.kms.client import KMSClient
from src.kms.key_registry import KeyRegistry
from src.encryption.engine import EncryptionEngine
from src.storage.s3_store import RecordStore
from src.pipeline.ingest import IngestPipeline
from src.pipeline.rotation import RotationPipeline
from src.rtbf.executor import RTBFExecutor
from src.config import get_config

console = Console()

CUSTOMERS = ["alice_corp", "bob_ltd", "carol_inc"]

PII_ROWS = [
    {"ssn": "123-45-6789", "email": "alice@alice-corp.com", "full_name": "Alice Smith",   "product_id": "PROD-A", "amount": 1200.00},
    {"ssn": "987-65-4321", "email": "alice2@alice-corp.com", "full_name": "Alice Jones",  "product_id": "PROD-B", "amount": 850.50},
    {"ssn": "111-22-3333", "email": "alice3@alice-corp.com", "full_name": "Alice Wu",     "product_id": "PROD-C", "amount": 300.00},
    {"ssn": "444-55-6666", "email": "alice4@alice-corp.com", "full_name": "Alice Brown",  "product_id": "PROD-D", "amount": 999.99},
    {"ssn": "777-88-9999", "email": "alice5@alice-corp.com", "full_name": "Alice Green",  "product_id": "PROD-E", "amount": 49.99},
]


def sep(title: str):
    console.rule(f"[bold cyan]{title}[/bold cyan]")


def main():
    cfg = get_config()
    kms = KMSClient()
    registry = KeyRegistry(cfg.key_registry_path)
    store = RecordStore()
    engine = EncryptionEngine(kms)
    pipeline = IngestPipeline(kms, engine, registry, store)
    rotation = RotationPipeline(kms, engine, registry, store, progress=True)
    rtbf = RTBFExecutor(kms, registry, store, audit_log_path=f"{_tmpdir}/rtbf_audit.jsonl")

    # -----------------------------------------------------------------------
    # 1. Register customers
    # -----------------------------------------------------------------------
    sep("1. Customer Registration → CMK Creation")
    for cid in CUSTOMERS:
        rec = pipeline.register_customer(cid, description=f"Demo customer {cid}")
        active = rec.active_version()
        console.print(f"  [green]✓[/green] {cid}  →  CMK: [yellow]{active.cmk_id}[/yellow]")

    # -----------------------------------------------------------------------
    # 2. Ingest PII rows for alice_corp
    # -----------------------------------------------------------------------
    sep("2. Ingest PII Rows (alice_corp)")
    record_ids = pipeline.ingest_batch("alice_corp", PII_ROWS)
    console.print(f"  Ingested [bold]{len(record_ids)}[/bold] records")

    # Show raw encrypted storage
    raw_rec = store.get_record("alice_corp", record_ids[0])
    console.print(Panel(
        f"[dim]record_id:[/dim]  {raw_rec.record_id}\n"
        f"[dim]key_version:[/dim] {raw_rec.key_version}\n"
        f"[dim]encrypted_dek:[/dim] {raw_rec.encrypted_dek[:40]}...\n"
        f"[dim]encrypted_columns:[/dim] {list(raw_rec.encrypted_columns.keys())}\n"
        f"[dim]plaintext_columns:[/dim] {raw_rec.plaintext_columns}",
        title="Raw encrypted record (what S3 sees)",
        border_style="red",
    ))

    # -----------------------------------------------------------------------
    # 3. Read back decrypted
    # -----------------------------------------------------------------------
    sep("3. Decrypted Read-Back")
    table = Table("record_id", "ssn", "email", "full_name", "product_id", "amount")
    for rid in record_ids:
        row = pipeline.read("alice_corp", rid)
        table.add_row(rid[:8] + "...", row["ssn"], row["email"], row["full_name"], row["product_id"], str(row["amount"]))
    console.print(table)

    # -----------------------------------------------------------------------
    # 4. Key Rotation
    # -----------------------------------------------------------------------
    sep("4. Key Rotation  (live — dual-read window open during migration)")

    console.print("  [yellow]Before rotation:[/yellow]")
    pre_customer = registry.get_customer("alice_corp")
    console.print(f"    current_version = {pre_customer.current_version}")
    console.print(f"    rotation_in_progress = {pre_customer.rotation_in_progress}")

    result = rotation.rotate_customer_key("alice_corp", disable_old_key=True)
    console.print(f"\n  [green]Rotation complete:[/green]")
    console.print(f"    v{result.old_version} → v{result.new_version}")
    console.print(f"    migrated: [bold]{result.records_migrated}[/bold]  failed: {result.records_failed}")
    console.print(f"    duration: {result.duration_seconds:.3f}s")

    post_customer = registry.get_customer("alice_corp")
    console.print(f"\n  [yellow]After rotation:[/yellow]")
    console.print(f"    current_version = {post_customer.current_version}")
    console.print(f"    rotation_in_progress = {post_customer.rotation_in_progress}")

    # Verify records still readable
    sep("  4b. Verify Records Still Readable After Rotation")
    all_rows = pipeline.read_all("alice_corp")
    console.print(f"  [green]✓[/green] All {len(all_rows)} records decrypted successfully after rotation")

    # -----------------------------------------------------------------------
    # 5. Right to be Forgotten
    # -----------------------------------------------------------------------
    sep("5. Right-to-be-Forgotten  (crypto-shredding bob_ltd)")

    # Ingest some data for bob_ltd first
    pipeline.ingest_batch("bob_ltd", PII_ROWS[:3])
    console.print("  Ingested 3 records for bob_ltd")

    rtbf_result = rtbf.execute("bob_ltd", delete_records=False)
    console.print(f"\n  RTBF result:")
    console.print(f"    success:       [green]{rtbf_result.success}[/green]")
    console.print(f"    keys deleted:  {rtbf_result.keys_deleted}")
    console.print(f"    executed_at:   {rtbf_result.executed_at}")

    # -----------------------------------------------------------------------
    # 6. Verify erasure
    # -----------------------------------------------------------------------
    sep("6. Erasure Verification")
    report = rtbf.verify_erasure("bob_ltd")
    console.print(Panel(
        json.dumps(report, indent=2),
        title=f"Erasure report — bob_ltd  (verified={report['verified']})",
        border_style="green" if report["verified"] else "red",
    ))

    # Attempt to read should raise
    try:
        pipeline.read_all("bob_ltd")
        console.print("[red]ERROR: data still readable after RTBF![/red]")
    except Exception as exc:
        console.print(f"  [green]✓[/green] Read after RTBF raised: [yellow]{exc}[/yellow]")

    # -----------------------------------------------------------------------
    # Done
    # -----------------------------------------------------------------------
    sep("Demo complete")
    console.print(f"  Temp data directory: {_tmpdir}")
    console.print("  [dim]Run 'pytest tests/' to execute the full test suite.[/dim]")


if __name__ == "__main__":
    main()
