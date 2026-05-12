FROM confluentinc/cp-kafka-connect:7.6.1

USER root

# Install the official ClickHouse sink connector from Confluent Hub.
# Pinning the version keeps the image reproducible.
RUN confluent-hub install --no-prompt clickhouse/clickhouse-kafka-connect:v1.2.5

# Install jq so the entrypoint can poll/register connectors.
RUN microdnf install -y jq && microdnf clean all

USER appuser
