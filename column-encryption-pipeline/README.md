# Column-Level Encryption with Key Rotation Pipeline

A production-grade system for encrypting PII database columns using per-customer keys stored in a KMS, with live key rotation and cryptographic erasure (GDPR Right to be Forgotten).

## Architecture

```
Ingest                     Storage               KMS
──────                     ───────               ───
raw row                    S3 / local FS         CMK per customer
  │                            │                     │
  ▼                            │                     │
EncryptionEngine ─── DEK ──────┘             KMSClient
  │    │                                       (local or AWS)
  │    └── encrypt PII columns (AES-256-GCM)
  │
  └── encrypted_dek = KMS.encrypt(DEK, CMK)
```

### Envelope Encryption

Each record gets a unique 256-bit **Data Encryption Key (DEK)**:

1. `KMS.generate_data_key(cmk_id)` → `(plaintext_DEK, encrypted_DEK)`
2. Encrypt each PII column with `plaintext_DEK` using AES-256-GCM
3. Store `encrypted_DEK` alongside the ciphertext
4. Discard `plaintext_DEK` from memory

To decrypt: `KMS.decrypt(encrypted_DEK)` → `plaintext_DEK` → decrypt columns.

### Key Rotation (live, zero-downtime)

```
1. Create new CMK in KMS
2. Registry: old_version → "rotating_out", new_version → "active"
   ↳ Dual-read window opens (reads try new key, fall back to old)
3. Scan all S3 records for customer
4. Per record: re_encrypt(encrypted_DEK, old_CMK → new_CMK)  [O(1) per record]
5. Write updated record to S3
6. Registry: old_version → "retired", rotation_in_progress = false
   ↳ Dual-read window closes
7. Disable old CMK in KMS
```

Re-encryption only rewraps the envelope key — column ciphertext is unchanged.

### Right to be Forgotten (Crypto-Shredding)

```
1. Delete all CMK versions for customer from KMS
2. encrypted_DEK in S3 is now permanently unreadable
3. Mark customer as "forgotten" in registry with timestamp
4. Write immutable audit record to RTBF audit log
5. (Optional) Physically delete S3 objects for storage hygiene
```

No need to touch S3 objects for inaccessibility — once the CMK is gone, the
DEK cannot be decrypted and the column ciphertext is indistinguishable from
random bytes.

## Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# Run end-to-end demo (no AWS required)
python demo.py

# Run test suite
pytest tests/ -v
```

## AWS / LocalStack mode

Set environment variables (see `.env.example`):

```bash
cp .env.example .env
# Edit KMS_MODE=aws, STORAGE_MODE=s3, set AWS credentials

# Or use LocalStack
docker-compose up -d
KMS_MODE=aws KMS_ENDPOINT_URL=http://localhost:4566 \
S3_ENDPOINT_URL=http://localhost:4566 python demo.py
```

## Project layout

```
src/
  config.py               Configuration
  kms/
    local_kms.py          Local KMS simulation (AES-256-GCM, file-backed)
    client.py             KMS facade (local or AWS boto3)
    key_registry.py       Customer CMK version tracking
  encryption/
    engine.py             Envelope encryption + AES-256-GCM column encryption
  storage/
    s3_store.py           Record persistence (local FS or S3)
  pipeline/
    ingest.py             PII ingest + dual-read decryption
    rotation.py           Live key rotation pipeline
  rtbf/
    executor.py           Right-to-be-Forgotten / crypto-shredding
tests/                    pytest suite (all tests run without AWS)
demo.py                   Interactive end-to-end demonstration
```

## Key design properties

| Property | How |
|---|---|
| Per-customer key isolation | Separate CMK per customer in KMS |
| Zero-knowledge storage | S3 only sees encrypted DEKs + column ciphertext |
| Authenticated encryption | AES-256-GCM (detects tampering) |
| Zero-downtime rotation | Dual-read window + DEK re-wrap only |
| Cryptographic erasure | Delete CMK → all DEKs unreadable |
| Audit trail | Immutable JSONL log of all RTBF executions |
| No AWS required | `KMS_MODE=local` uses a file-backed simulation |
