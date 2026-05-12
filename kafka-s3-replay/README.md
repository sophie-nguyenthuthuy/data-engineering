# kafka-s3-replay

> **Disaster Recovery & Event Replay System** — replay any 30-day window of Kafka events stored in S3 into any downstream target.

```
replay run \
  --bucket my-kafka-archive \
  --topics orders payments \
  --days 3 \
  --target kafka \
  --kafka-brokers localhost:9092
```

---

## Why this exists

When an incident corrupts downstream state — a bad deploy, a misconfigured consumer, a database wipe — you need to re-process the original events. This tool reads the Kafka→S3 archive written by **Kafka Connect S3 Sink** and replays any time window up to 30 days into:

| Target | Use-case |
|--------|----------|
| `kafka` | Re-feed events into Kafka (with topic remapping) |
| `http`  | Drive a webhook / microservice directly |
| `file`  | Dump to JSONL/Avro for offline analysis |
| `stdout`| Debug / dry-run inspection |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        replay run                        │
└────────────────────┬────────────────────────────────────┘
                     │
          ┌──────────▼──────────┐
          │    ReplayEngine      │  • token-bucket rate limit
          │  (engine/engine.py)  │  • parallel partition reads
          │                      │  • checkpoint store (resume)
          └──┬──────────────┬───┘
             │              │
   ┌─────────▼─────┐  ┌─────▼──────────┐
   │ S3ArchiveReader│  │  BaseTarget     │
   │ (archive/s3.py)│  │  implementations│
   └────────────────┘  └────────────────┘
        │
   S3 / LocalStack
   (JSONL · Avro · gz)
```

### Key components

| Module | Responsibility |
|--------|---------------|
| `archive/s3.py` | Stream events from S3, filter to time window, parse JSONL/Avro/gz |
| `archive/manifest.py` | Pre-scan S3 and produce a file manifest |
| `engine/engine.py` | Orchestrate reads → target with rate limiting & checkpointing |
| `engine/window.py` | Parse and validate time windows (max 30 days) |
| `engine/checkpoint.py` | File-based resume state (JSON) |
| `targets/kafka.py` | Confluent Kafka producer with idempotent delivery |
| `targets/http.py` | aiohttp POST with exponential-back-off retries |
| `targets/file.py` | Write JSONL or Avro locally |
| `targets/stdout.py` | Pretty-print to terminal |
| `cli.py` | Rich-powered CLI with live progress bar |

---

## Installation

```bash
pip install kafka-s3-replay          # from PyPI (once published)
# or from source:
git clone https://github.com/YOUR_USER/kafka-s3-replay
cd kafka-s3-replay
pip install -e ".[dev]"
```

---

## CLI reference

### `replay run`

```
Usage: replay run [OPTIONS]

Options:
  -c, --config FILE          Path to YAML/JSON config file
  -t, --topics TEXT          Topic(s) to replay (repeatable)
  -b, --bucket TEXT          S3 bucket  [env: REPLAY_S3_BUCKET]
      --prefix TEXT          S3 key prefix
      --start TEXT           Window start (ISO-8601)
      --end TEXT             Window end  (ISO-8601, default: now)
      --days INTEGER         Last N days (max 30, overrides --start/--end)
      --target [kafka|http|file|stdout]
      --kafka-brokers TEXT   Kafka bootstrap servers [env: REPLAY_KAFKA_BROKERS]
      --http-url TEXT        Webhook URL  [env: REPLAY_HTTP_URL]
      --output-file PATH     Output file (for --target file)
      --rate-limit FLOAT     Max events/second
      --dry-run              Parse only, do NOT send
      --resume / --no-resume Resume from checkpoint (default: resume)
      --job-id TEXT          Explicit job ID (auto-generated if omitted)
      --format [jsonl|avro|parquet]
      --region TEXT          AWS region (default: us-east-1)
      --endpoint-url TEXT    Custom S3 endpoint [env: AWS_ENDPOINT_URL]
  -v, --verbose
```

### `replay manifest`

List all S3 files that would be read for a given window without executing the replay.

```bash
replay manifest \
  --topics orders \
  --bucket my-archive \
  --days 7 \
  --output manifest.json
```

### `replay status`

Inspect the checkpoint of a previous job.

```bash
replay status replay-20240315-120000-abc123
```

---

## Config file

```yaml
# config/example.yaml
job_id: incident-recovery-2024-03-15
topics: [orders, payments]
window:
  start: "2024-03-14T00:00:00Z"
  end:   "2024-03-15T23:59:59Z"
archive:
  bucket: my-kafka-archive-bucket
  prefix: kafka-connect/topics
  region: us-east-1
  format: jsonl
target_type: kafka
kafka_target:
  bootstrap_servers: "localhost:9092"
  topic_mapping:
    orders: orders-replay
rate_limit_per_second: 1000
dry_run: false
checkpoint_dir: /tmp/replay-checkpoints
```

---

## S3 archive format

Expects the **Kafka Connect S3 Sink** key layout:

```
{prefix}/{topic}/{partition:04d}/{topic}+{partition:04d}+{offset:020d}.json(.gz)
```

Each file is JSONL where every line is:
```json
{
  "topic": "orders",
  "offset": 42,
  "timestamp": "2024-03-14T10:05:00Z",
  "key": "order-002",
  "payload": { ... }
}
```

Gzip-compressed files (`.json.gz`, `.jsonl.gz`, `.avro.gz`) are transparently decompressed.

---

## Local development

```bash
# Start Kafka + LocalStack
make infra

# Seed a sample archive into LocalStack S3
make seed-s3

# Run a dry-run replay
make replay-dry

# Run tests
make test

# Run tests with coverage
make test-cov
```

---

## Checkpointing & resume

Every completed S3 file is recorded in `{checkpoint_dir}/{job_id}.json`.  
If a replay is interrupted, re-run the **same command** — already-processed files are skipped automatically.

To force a full re-run:
```bash
replay run ... --no-resume
```

---

## Rate limiting

```bash
replay run ... --rate-limit 500   # max 500 events/second
```

Uses a token-bucket algorithm so bursts are absorbed smoothly.

---

## Replay provenance headers

Every event replayed to a Kafka target carries extra headers so consumers can distinguish replayed traffic:

```
x-replay-source-topic:  orders
x-replay-source-offset: 42
x-replay-timestamp:     2024-03-14T10:05:00+00:00
```

---

## Running tests

```bash
pytest -v                        # all tests
pytest tests/test_window.py -v   # unit tests only
pytest --cov=src/replay          # with coverage
```

Tests use **moto** to mock AWS S3 — no real AWS credentials needed.

---

## License

MIT
