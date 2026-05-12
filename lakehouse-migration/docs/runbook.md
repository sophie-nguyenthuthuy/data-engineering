# Runbook

## First-time Full Migration

```bash
# 1. Copy and edit config
cp config/env.example.yaml config/env.yaml
# Edit: source JDBC URL, bucket paths, table list

# 2. Create databases
spark-sql -f sql/migrations/V001__initial_schema.sql

# 3. Create DDL tables
spark-sql -f sql/ddl/bronze_transactions.sql
spark-sql -f sql/ddl/silver_customers.sql

# 4. Run full historical load (Bronze + Gold)
python scripts/run_migration.py --env dev

# 5. Verify row counts
spark-sql -e "SELECT COUNT(*) FROM delta.\`s3://…/bronze/transactions\`"
```

## Daily Incremental Run

```bash
python scripts/run_incremental.py --env prod
```

## Maintenance (run nightly)

```bash
python scripts/optimize_and_vacuum.py --layer silver
python scripts/optimize_and_vacuum.py --layer gold
```

## Time-Travel Rollback

```sql
-- Restore Silver customers to version 10
RESTORE TABLE delta.`s3://…/silver/customers` TO VERSION AS OF 10;
```

## Schema Migration

1. Add migration script to `sql/migrations/V00N__description.sql`.
2. Run: `spark-sql -f sql/migrations/V00N__description.sql`.
3. Commit the script (treat migrations as immutable once applied in prod).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ConcurrentAppendException` | Two writers appending to same partition | Use `isolationLevel = Serializable` or serialize writers |
| VACUUM removes needed files | Retention too short | Increase `delta.logRetentionDuration` |
| Schema mismatch on MERGE | Source added column | Enable `autoMerge` or run schema migration first |
| High `_last_watermark` skew | Clock drift on source | Use monotonic sequence IDs instead of timestamps |
