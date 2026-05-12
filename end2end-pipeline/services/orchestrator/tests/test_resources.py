"""Resource/config tests that don't require a real broker / ClickHouse / Spark."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from orchestrator.resources import KafkaResource, read_secret


def test_read_secret_prefers_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    secret_file = tmp_path / "pw"
    secret_file.write_text("from-file\n")

    monkeypatch.setenv("FOO", "from-env")
    monkeypatch.setenv("FOO_FILE", str(secret_file))

    assert read_secret("FOO", file_env="FOO_FILE") == "from-file"


def test_read_secret_falls_back_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FOO", "from-env")
    monkeypatch.delenv("FOO_FILE", raising=False)

    assert read_secret("FOO", file_env="FOO_FILE") == "from-env"


def test_read_secret_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.delenv("FOO_FILE", raising=False)

    with pytest.raises(RuntimeError, match="FOO"):
        read_secret("FOO", file_env="FOO_FILE")


def test_kafka_resource_plaintext_emits_minimal_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key in list(os.environ):
        if key.startswith("KAFKA_"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "kafka:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "user-interactions")

    res = KafkaResource.from_env()
    cfg = res.client_config()

    assert cfg == {"bootstrap.servers": "kafka:9092"}


def test_kafka_resource_sasl_includes_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pw = tmp_path / "pw"
    pw.write_text("s3cr3t\n")

    monkeypatch.setenv("KAFKA_BOOTSTRAP", "kafka:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "user-interactions")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "replay")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD_FILE", str(pw))
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/run/secrets/ca.crt")

    res = KafkaResource.from_env()
    cfg = res.client_config()

    assert cfg["security.protocol"] == "SASL_SSL"
    assert cfg["sasl.mechanism"] == "SCRAM-SHA-512"
    assert cfg["sasl.username"] == "replay"
    assert cfg["sasl.password"] == "s3cr3t"
    assert cfg["ssl.ca.location"] == "/run/secrets/ca.crt"
