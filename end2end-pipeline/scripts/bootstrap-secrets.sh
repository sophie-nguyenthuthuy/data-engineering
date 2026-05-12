#!/usr/bin/env bash
# Generate (idempotently) all TLS material, SCRAM passwords, JAAS configs,
# and ClickHouse user files under ./secrets/.
#
# Re-running is safe: existing files are left alone. To rotate everything,
# delete the relevant subdir under secrets/ and re-run.
set -euo pipefail

SECRETS_DIR="${SECRETS_DIR:-secrets}"
DAYS="${CERT_DAYS:-3650}"

mkdir -p "$SECRETS_DIR"/{ca,kafka,clients}

_log() { printf '==> %s\n' "$*"; }

# ---------- CA ----------
if [[ ! -f "$SECRETS_DIR/ca/ca.key" ]]; then
  _log "Generating root CA"
  openssl genrsa -out "$SECRETS_DIR/ca/ca.key" 4096 2>/dev/null
  openssl req -new -x509 -key "$SECRETS_DIR/ca/ca.key" \
    -out "$SECRETS_DIR/ca/ca.crt" \
    -days "$DAYS" -sha256 \
    -subj "/CN=end2end-pipeline-ca/O=end2end-pipeline/OU=security"
else
  _log "CA already present"
fi

# ---------- Kafka broker cert ----------
if [[ ! -f "$SECRETS_DIR/kafka/kafka.crt" ]]; then
  _log "Generating Kafka broker cert"
  openssl genrsa -out "$SECRETS_DIR/kafka/kafka.key" 4096 2>/dev/null
  cat >"$SECRETS_DIR/kafka/kafka.cnf" <<'EOF'
[ req ]
distinguished_name = dn
req_extensions = v3_ext
prompt = no
[ dn ]
CN = kafka
O  = end2end-pipeline
[ v3_ext ]
subjectAltName   = @alt_names
keyUsage         = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
[ alt_names ]
DNS.1 = kafka
DNS.2 = localhost
IP.1  = 127.0.0.1
EOF
  openssl req -new \
    -key "$SECRETS_DIR/kafka/kafka.key" \
    -out "$SECRETS_DIR/kafka/kafka.csr" \
    -config "$SECRETS_DIR/kafka/kafka.cnf"
  openssl x509 -req \
    -in "$SECRETS_DIR/kafka/kafka.csr" \
    -CA "$SECRETS_DIR/ca/ca.crt" \
    -CAkey "$SECRETS_DIR/ca/ca.key" \
    -CAcreateserial \
    -out "$SECRETS_DIR/kafka/kafka.crt" \
    -days "$DAYS" -sha256 \
    -extfile "$SECRETS_DIR/kafka/kafka.cnf" -extensions v3_ext
else
  _log "Broker cert already present"
fi

# Bitnami Kafka expects PEM keystore files at fixed names.
cp "$SECRETS_DIR/kafka/kafka.crt" "$SECRETS_DIR/kafka/kafka.keystore.pem"
cp "$SECRETS_DIR/kafka/kafka.key" "$SECRETS_DIR/kafka/kafka.keystore.key"
cp "$SECRETS_DIR/ca/ca.crt"       "$SECRETS_DIR/kafka/kafka.truststore.pem"

# ---------- Client truststore (PKCS12, CA only) ----------
if [[ ! -f "$SECRETS_DIR/clients/truststore.p12" ]]; then
  _log "Generating client PKCS12 truststore"
  ts_pwd=$(openssl rand -hex 16)
  printf '%s' "$ts_pwd" >"$SECRETS_DIR/clients/truststore_password"
  openssl pkcs12 -export \
    -in "$SECRETS_DIR/ca/ca.crt" -nokeys \
    -out "$SECRETS_DIR/clients/truststore.p12" \
    -name ca-cert \
    -passout pass:"$ts_pwd"
fi

# PEM CA for Python / librdkafka clients.
cp "$SECRETS_DIR/ca/ca.crt" "$SECRETS_DIR/clients/ca.crt"

# ---------- Passwords (one per identity) ----------
gen_pw() { openssl rand -hex 16; }
for id in producer connect schemaregistry admin api replay clickhouse_pipeline clickhouse_api; do
  file="$SECRETS_DIR/clients/${id}_password"
  [[ -s "$file" ]] || { _log "Generating password for ${id}"; gen_pw >"$file"; }
done

# MinIO root credential (S3-compatible object store, Phase 4).
if [[ ! -s "$SECRETS_DIR/clients/minio_root_user" ]]; then
  _log "Generating MinIO root credentials"
  echo "pipeline" >"$SECRETS_DIR/clients/minio_root_user"
  gen_pw >"$SECRETS_DIR/clients/minio_root_password"
fi

# Grafana admin password (Phase 5).
if [[ ! -s "$SECRETS_DIR/clients/grafana_admin_password" ]]; then
  _log "Generating Grafana admin password"
  gen_pw >"$SECRETS_DIR/clients/grafana_admin_password"
fi

# ClickHouse wants password_sha256_hex.
for u in pipeline api; do
  sha_file="$SECRETS_DIR/clients/clickhouse_${u}_sha256"
  pw_file="$SECRETS_DIR/clients/clickhouse_${u}_password"
  python3 -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1]).read().strip().encode()).hexdigest())" "$pw_file" >"$sha_file"
done

# ---------- ClickHouse users.xml ----------
_log "Rendering clickhouse_users.xml"
pipeline_sha=$(cat "$SECRETS_DIR/clients/clickhouse_pipeline_sha256")
api_sha=$(cat "$SECRETS_DIR/clients/clickhouse_api_sha256")
cat >"$SECRETS_DIR/clients/clickhouse_users.xml" <<EOF
<?xml version="1.0"?>
<clickhouse>
  <users>
    <!-- Disable the default user so only named users can connect. -->
    <default remove="remove"/>

    <pipeline>
      <password_sha256_hex>${pipeline_sha}</password_sha256_hex>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>0</access_management>
    </pipeline>

    <api_ro>
      <password_sha256_hex>${api_sha}</password_sha256_hex>
      <networks><ip>::/0</ip></networks>
      <profile>readonly</profile>
      <quota>default</quota>
      <access_management>0</access_management>
    </api_ro>
  </users>
</clickhouse>
EOF

# ---------- Schema Registry basic-auth realm ----------
_log "Rendering sr_users.properties"
sr_out="$SECRETS_DIR/clients/sr_users.properties"
: >"$sr_out"
for u in producer connect api admin; do
  pw=$(cat "$SECRETS_DIR/clients/${u}_password")
  role=user
  [[ "$u" == "admin" ]] && role=admin
  echo "${u}: ${pw},${role}" >>"$sr_out"
done

# ---------- JAAS configs (per-client Kafka SASL) ----------
make_jaas() {
  local out=$1 username=$2 password_file=$3
  cat >"$out" <<EOF
KafkaClient {
  org.apache.kafka.common.security.scram.ScramLoginModule required
  username="${username}"
  password="$(cat "$password_file")";
};
EOF
}
make_jaas "$SECRETS_DIR/clients/producer_kafka.jaas"       producer       "$SECRETS_DIR/clients/producer_password"
make_jaas "$SECRETS_DIR/clients/connect_kafka.jaas"        connect        "$SECRETS_DIR/clients/connect_password"
make_jaas "$SECRETS_DIR/clients/schemaregistry_kafka.jaas" schemaregistry "$SECRETS_DIR/clients/schemaregistry_password"
make_jaas "$SECRETS_DIR/clients/replay_kafka.jaas"         replay         "$SECRETS_DIR/clients/replay_password"

# JAAS for the Schema Registry basic-auth Jetty realm.
cat >"$SECRETS_DIR/clients/schemaregistry_auth.jaas" <<'EOF'
SchemaRegistry-Props {
  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
  file="/etc/schema-registry/users.properties"
  debug="false";
};
EOF

# ---------- Permissions ----------
chmod 600 "$SECRETS_DIR"/clients/*_password "$SECRETS_DIR"/clients/*_sha256 \
          "$SECRETS_DIR"/clients/*.jaas "$SECRETS_DIR"/clients/truststore_password \
          "$SECRETS_DIR"/clients/sr_users.properties \
          "$SECRETS_DIR"/clients/clickhouse_users.xml \
          "$SECRETS_DIR"/clients/minio_root_user \
          "$SECRETS_DIR"/clients/grafana_admin_password \
          "$SECRETS_DIR"/ca/ca.key "$SECRETS_DIR"/kafka/kafka.key 2>/dev/null || true
chmod 644 "$SECRETS_DIR"/clients/ca.crt \
          "$SECRETS_DIR"/clients/truststore.p12 \
          "$SECRETS_DIR"/kafka/kafka.crt \
          "$SECRETS_DIR"/kafka/kafka.truststore.pem \
          "$SECRETS_DIR"/kafka/kafka.keystore.pem \
          "$SECRETS_DIR"/ca/ca.crt 2>/dev/null || true
chmod 640 "$SECRETS_DIR"/kafka/kafka.keystore.key 2>/dev/null || true

echo
_log "Done. Secrets under ${SECRETS_DIR}/ (git-ignored)."
