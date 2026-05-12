# Security (Phase 2)

This doc describes what the secure stack (`docker-compose.secure.yml`) locks
down, and which attacker capabilities it does **not** cover.

## What's secured

### Kafka — SASL_SCRAM-SHA-512 over TLS

- **Two client-facing listeners** (`INTERNAL` on 9092, `EXTERNAL` on 19092)
  both use `SASL_SSL`. The broker cert is signed by a local CA; clients
  verify the CA and authenticate with SCRAM-SHA-512.
- The **CONTROLLER listener** (9093) stays PLAINTEXT for the KRaft quorum.
  On a single-node dev cluster the controller speaks only to itself (loopback
  inside the container); a multi-node prod cluster would put it behind
  SASL_SSL too.
- **SCRAM users** (`producer`, `connect`, `schemaregistry`, `admin`, `api`)
  are provisioned by the Bitnami image at first boot from
  `KAFKA_CLIENT_USERS` / `KAFKA_CLIENT_PASSWORDS`, which the Makefile fills
  from `secrets/clients/*_password` at `up-secure` time. SCRAM salts +
  iterations are stored in Kafka's own metadata log.
- `auto.create.topics.enable=false` — a rogue producer can't create
  arbitrary topics.
- `admin` is the single superuser (`KAFKA_CFG_SUPER_USERS=User:admin`). ACL
  enforcement itself lands in Phase 3.

### Schema Registry

- Talks to Kafka over SASL_SSL (same CA, SCRAM user `schemaregistry`).
- **REST API is gated by HTTP basic auth** (Jetty `PropertyFileLoginModule`).
  Credentials live in `secrets/clients/sr_users.properties`, copied into the
  container at start. Roles: `admin` for mutation, `user` for reads.
- Each client (producer, connect, api) has its own SR identity; one
  compromise doesn't grant the others.

### Kafka Connect

- Worker + internal producer/consumer/admin all use SASL_SSL with
  identity `connect`.
- REST API on 8083 is currently open (no basic auth extension). That's
  deliberately deferred: the REST port is only exposed on localhost and
  the connector JSON it serves contains no secrets after phase 2 (passwords
  are only in `producer.override.sasl.jaas.config`, which Connect masks in
  its /connectors GET response). Adding REST basic auth lands in Phase 6
  alongside CI to keep the smoke test simple here.
- Sink connector uses SR basic auth (`USER_INFO` credential source) and
  embeds its own SASL_SSL `producer.override.*` config for DLQ writes.

### ClickHouse

- `default` user is **removed** via `users.d/pipeline.xml`.
- Two named users:
  - `pipeline` — used only by Kafka Connect for inserts.
  - `api_ro` — `readonly` profile, used only by the FastAPI service.
- Passwords stored as `password_sha256_hex` in `users.d/pipeline.xml`,
  mounted from `secrets/clients/clickhouse_users.xml`. Plaintext passwords
  are delivered to the FastAPI + Connect containers through **Docker
  secrets** (files under `/run/secrets/`), not env vars — so they don't
  appear in `docker inspect`.

### Secrets handling

- Everything sensitive is generated into `./secrets/` by
  `scripts/bootstrap-secrets.sh`. That directory is `.gitignore`d.
- In containers, secrets are delivered as **files** (Docker `secrets:`
  keyword, mounted at `/run/secrets/<name>`), read once by the service on
  startup. The producer + API code supports both `FOO` (env) and `FOO_FILE`
  (file path); file wins.
- The only place Kafka client passwords legitimately appear in env vars is
  the Bitnami broker's own `KAFKA_CLIENT_PASSWORDS` (required by the
  image to provision SCRAM users). Those users are then authenticated via
  the SCRAM handshake, not by re-sending those passwords.
- `.env.secure` is generated at `make up-secure` time from `.env` plus the
  generated password files; it's ignored by git.

## Verify it's actually encrypted

Once `make smoke-secure` is up:

```bash
# Should succeed — valid creds + CA:
docker run --rm --network pipeline_default \
  -v $(pwd)/secrets:/s:ro \
  confluentinc/cp-kafka:7.6.1 \
  kafka-topics --bootstrap-server kafka:9092 \
    --command-config <(printf '%s\n' \
      'security.protocol=SASL_SSL' \
      'sasl.mechanism=SCRAM-SHA-512' \
      "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"$(cat secrets/clients/admin_password)\";" \
      'ssl.truststore.type=PEM' \
      'ssl.truststore.location=/s/kafka/kafka.truststore.pem') \
    --list

# Should fail — no auth:
docker run --rm --network pipeline_default confluentinc/cp-kafka:7.6.1 \
  kafka-topics --bootstrap-server kafka:9092 --list
```

## What's **not** covered (yet)

| Gap                                                   | Planned in |
| ----------------------------------------------------- | ---------- |
| Kafka ACLs (authz, not just authn)                    | Phase 3    |
| Connect REST basic auth                               | Phase 6    |
| mTLS (client certs) — SASL_SCRAM is enough for now    | —          |
| HSM-backed key storage                                | AWS module (Phase 7: KMS + Secrets Manager) |
| Short-lived cert rotation                             | Phase 7    |
| ClickHouse TLS on 9000/8123                           | Phase 3    |
| OAuth2/OIDC-based human access to dashboards          | Phase 5    |

## Rotation

```bash
# Rotate a single password (producer):
rm secrets/clients/producer_password
make bootstrap           # regenerates only what's missing
make up-secure           # picks up new values
make register-secure     # re-registers the sink connector with the new creds

# Rotate everything — including the CA:
rm -rf secrets/ca secrets/kafka secrets/clients
make bootstrap
make clean-secure
make smoke-secure
```
