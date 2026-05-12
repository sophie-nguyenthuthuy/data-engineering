# Reliability (Phase 3)

This phase closes the gap between "messages flow end-to-end on a sunny day"
and "the pipeline survives transient failures, duplicate deliveries, and
an incompatible schema change landing in main".

## Duplicate handling — ReplacingMergeTree + `exactlyOnce` sink

**Problem.** Kafka Connect's delivery guarantee is at-least-once by default.
A worker crash after an INSERT but before a commit will replay the records
on restart. Without dedup, every such failure would double-count events.

**Two layers of defense.**

1. **Sink connector `exactlyOnce=true`** (secure stack).
   `clickhouse-kafka-connect` writes Kafka offsets into ClickHouse in the
   same transaction as the data, so on restart it replays from the stored
   offset instead of the framework offset. The failure window collapses from
   "one commit interval" to "zero". `exactlyOnce` is still deliberately off
   on the dev stack (simpler to reason about; the dev stack isn't meant for
   production semantics anyway).

2. **`ReplacingMergeTree(ingested_at)` on `events.user_interactions`**.
   ORDER BY is `(event_type, occurred_at, event_id)`; `event_id` is globally
   unique so any genuine duplicate collapses on merge — the row with the
   larger `ingested_at` wins.

**Residual drift.** The 1-minute `AggregatingMergeTree` MV runs on INSERT
and cannot dedup. In the narrow window between a replayed insert and the
next merge of the source table, the rollup may over-count briefly. Phase 4
closes this with a Dagster asset that runs `OPTIMIZE TABLE ... FINAL
DEDUPLICATE` and rebuilds the rollup for a minute. For now, the operator
target is:

```bash
make ch-dedup      # runs OPTIMIZE ... FINAL DEDUPLICATE against the raw table
```

## Retry policy on the sink

The sink connector now keeps retrying transient failures (ClickHouse network
blip, 5xx, etc.) for up to 5 minutes before forwarding to the DLQ.

| Setting                     | Value  | Meaning                                       |
| --------------------------- | ------ | --------------------------------------------- |
| `errors.tolerance`          | `all`  | Don't kill the task on errors                 |
| `errors.retry.timeout`      | 300000 | Retry transient errors for up to 5 min        |
| `errors.retry.delay.max.ms` | 30000  | Exponential backoff capped at 30s             |
| `errors.deadletterqueue.*`  | set    | Forward un-retried errors to `…-dlq` + headers |

Only records that still fail after the retry window go to the DLQ — the DLQ
is for **poison payloads**, not **transient ClickHouse hiccups**. That's
the whole point of the tuning.

### Inspecting the DLQ

```bash
make dlq-peek                  # 10 messages, secure stack
N=50 make dlq-peek             # more
STACK=dev make dlq-peek        # against the dev stack
```

Each message is printed with its Kafka Connect error-context headers:

- `__connect.errors.topic` — original topic
- `__connect.errors.partition`, `__connect.errors.offset`
- `__connect.errors.class.name`, `__connect.errors.exception.message`
- `__connect.errors.exception.stacktrace`

## Schema-evolution guard in the producer

The producer now runs a **pre-flight compatibility check** before it opens
the serializer. It calls `POST /compatibility/subjects/{subject}/versions/latest`
against Schema Registry with the local `.avsc` file. If the subject already
exists and the local schema is not compatible under the subject's configured
level (default `BACKWARD`), the producer fails fast with a clear error
instead of silently registering a new incompatible version at the first
`produce()`.

What "compatible" means under BACKWARD (the Confluent default):

- **OK**: add a new field *with a default*; remove an optional field.
- **Not OK**: add a new required field; rename a field; change a field's type.

If you need to ship an incompatible schema deliberately, either:

1. Use a new topic (a different subject), or
2. Change the subject's level via SR
   (`PUT /config/<subject>` with `{"compatibility": "NONE"}`) and accept
   that old consumers will break.

Either way, the producer *forces you to acknowledge the trade-off*; there's
no "it just worked in staging and then my consumers all crashed in prod"
failure mode anymore.

## Kafka ACLs (secure stack)

With `StandardAuthorizer` enabled and `allow.everyone.if.no.acl.found=false`,
every Kafka client has to be explicitly authorized. `infra/kafka/acls.sh`
provisions the ACLs at init time; it's idempotent so re-runs are safe.

| Principal          | Grants                                                                     |
| ------------------ | -------------------------------------------------------------------------- |
| `User:producer`    | Write + Describe on `user-interactions`; IdempotentWrite on cluster        |
| `User:connect`     | Read + Describe on `user-interactions`; Write + Describe + Create on DLQ;  |
|                    | All on topics prefixed `_connect_`; Read + Describe on group prefix `connect-`; |
|                    | Read + Describe on group `pipeline-connect-cluster`;                        |
|                    | IdempotentWrite on cluster                                                 |
| `User:schemaregistry` | Read + Write + Describe on `_schemas`; Read + Describe on group `schema-registry` |
| `User:admin`       | — (super user; bypasses ACLs via `super.users`)                             |
| `User:api`         | no Kafka access (ClickHouse only)                                          |

### Verifying ACLs are active

```bash
make acls-list

# Expected failure — `producer` has no read permission on anything:
docker exec -it kafka bash -lc 'cat >/tmp/pprod.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="producer" password="'$(cat secrets/clients/producer_password)'";
ssl.truststore.type=PEM
ssl.truststore.location=/opt/bitnami/kafka/config/certs/kafka.truststore.pem
EOF
/opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --consumer.config /tmp/pprod.properties --topic user-interactions --max-messages 1 --timeout-ms 5000'
# -> TopicAuthorizationException: Not authorized to access topics: [user-interactions]
```

## Upgrade notes

- Changing the `user_interactions` engine to `ReplacingMergeTree` requires a
  fresh `clickhouse-data` volume. On an existing deployment, either:
  - Tear down with `make clean-secure` (destroys data) and re-run, or
  - Use ClickHouse `RENAME TABLE old → user_interactions_legacy`, create
    the new one, and backfill.

- Enabling ACLs on an existing cluster will blackhole every operation that
  doesn't have an ACL. Always provision ACLs *before* flipping
  `allow.everyone.if.no.acl.found` to false. The init container does this
  atomically on a fresh stack; on an upgrade, run `infra/kafka/acls.sh`
  first.

## What's still open (next phase territory)

- **DLQ replay pipeline** — right now you can peek the DLQ; a Dagster asset
  in Phase 4 will replay selected failures back onto `user-interactions`.
- **Connect REST basic auth** — deferred to Phase 6 (with CI) so the smoke
  test doesn't grow a basic-auth dance.
- **ClickHouse TLS on 8123/9000** — currently plaintext on the docker
  network. OK behind the network boundary; needed if Connect ever lands
  off-cluster.
