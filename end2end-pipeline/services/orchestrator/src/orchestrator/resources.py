"""Dagster resources: ClickHouse, S3 (MinIO), Kafka.

Each resource supports Docker secret files via a `*_FILE` env sibling, taking
precedence over the direct env var. Falls back gracefully when a secret is not
set so the same code runs against dev + secure stacks.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import boto3
from botocore.client import Config as BotoConfig
from clickhouse_connect import get_client
from dagster import ConfigurableResource


def read_secret(env_name: str, file_env: str | None = None, default: str | None = None) -> str:
    """Read a secret, preferring the `*_FILE` env variant if set."""
    if file_env:
        path = os.getenv(file_env)
        if path and Path(path).exists():
            return Path(path).read_text().strip()
    value = os.getenv(env_name, default)
    if value is None:
        raise RuntimeError(f"required secret {env_name} is not set")
    return value


class ClickHouseResource(ConfigurableResource):
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> ClickHouseResource:
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT_HTTP", "8123")),
            database=os.getenv("CLICKHOUSE_DB", "events"),
            user=os.getenv("CLICKHOUSE_PIPELINE_USER", "pipeline"),
            password=read_secret(
                "CLICKHOUSE_PIPELINE_PASSWORD",
                file_env="CLICKHOUSE_PIPELINE_PASSWORD_FILE",
            ),
        )

    def get_client(self) -> Any:
        return get_client(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.user,
            password=self.password,
        )


class S3Resource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    region: str = "us-east-1"

    @classmethod
    def from_env(cls) -> S3Resource:
        return cls(
            endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            access_key=read_secret("S3_ACCESS_KEY", file_env="S3_ACCESS_KEY_FILE"),
            secret_key=read_secret("S3_SECRET_KEY", file_env="S3_SECRET_KEY_FILE"),
            bucket=os.getenv("S3_BUCKET", "pipeline"),
        )

    def get_client(self) -> Any:
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=BotoConfig(signature_version="s3v4"),
        )


class KafkaResource(ConfigurableResource):
    bootstrap: str
    source_topic: str
    dlq_topic: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_ca_location: str | None = None

    @classmethod
    def from_env(cls) -> KafkaResource:
        protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        sasl_password = None
        if protocol != "PLAINTEXT":
            sasl_password = read_secret(
                "KAFKA_SASL_PASSWORD",
                file_env="KAFKA_SASL_PASSWORD_FILE",
            )
        return cls(
            bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            source_topic=os.getenv("KAFKA_TOPIC", "user-interactions"),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "user-interactions-dlq"),
            security_protocol=protocol,
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM") if protocol != "PLAINTEXT" else None,
            sasl_username=os.getenv("KAFKA_SASL_USERNAME") if protocol != "PLAINTEXT" else None,
            sasl_password=sasl_password,
            ssl_ca_location=os.getenv("KAFKA_SSL_CA_LOCATION") if protocol != "PLAINTEXT" else None,
        )

    def client_config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {"bootstrap.servers": self.bootstrap}
        if self.security_protocol != "PLAINTEXT":
            cfg["security.protocol"] = self.security_protocol
            if self.sasl_mechanism:
                cfg["sasl.mechanism"] = self.sasl_mechanism
            if self.sasl_username:
                cfg["sasl.username"] = self.sasl_username
            if self.sasl_password:
                cfg["sasl.password"] = self.sasl_password
            if self.ssl_ca_location:
                cfg["ssl.ca.location"] = self.ssl_ca_location
        return cfg


def build_resources() -> dict[str, Any]:
    return {
        "clickhouse": ClickHouseResource.from_env(),
        "s3": S3Resource.from_env(),
        "kafka": KafkaResource.from_env(),
    }
