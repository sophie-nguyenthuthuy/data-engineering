from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SaslConfig:
    """SASL_SSL credentials for Kafka + Schema Registry.

    When ``security_protocol == "PLAINTEXT"`` all fields other than
    ``security_protocol`` are ignored — this lets the same code run in both
    the dev (plaintext) compose and the secure compose.
    """

    security_protocol: str
    mechanism: str | None
    username: str | None
    password: str | None
    ca_location: str | None
    sr_username: str | None
    sr_password: str | None

    @property
    def enabled(self) -> bool:
        return self.security_protocol.upper() != "PLAINTEXT"


@dataclass(frozen=True)
class Config:
    brokers: str
    topic: str
    schema_registry_url: str
    schema_path: Path
    events_per_sec: float
    error_rate: float
    sasl: SaslConfig

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            brokers=_require("KAFKA_BROKERS"),
            topic=_require("KAFKA_TOPIC"),
            schema_registry_url=_require("SCHEMA_REGISTRY_URL"),
            schema_path=Path(
                os.environ.get("PRODUCER_SCHEMA_PATH", "/app/schemas/user_interaction.avsc")
            ),
            events_per_sec=float(os.environ.get("PRODUCER_EVENTS_PER_SEC", "50")),
            error_rate=float(os.environ.get("PRODUCER_ERROR_RATE", "0.05")),
            sasl=_sasl_from_env(),
        )


def _sasl_from_env() -> SaslConfig:
    protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    return SaslConfig(
        security_protocol=protocol,
        mechanism=os.environ.get("KAFKA_SASL_MECHANISM"),
        username=os.environ.get("KAFKA_SASL_USERNAME"),
        password=_read_secret(
            env_value=os.environ.get("KAFKA_SASL_PASSWORD"),
            file_env="KAFKA_SASL_PASSWORD_FILE",
        ),
        ca_location=os.environ.get("KAFKA_SSL_CA_LOCATION"),
        sr_username=os.environ.get("SCHEMA_REGISTRY_USER"),
        sr_password=_read_secret(
            env_value=os.environ.get("SCHEMA_REGISTRY_PASSWORD"),
            file_env="SCHEMA_REGISTRY_PASSWORD_FILE",
        ),
    )


def _read_secret(*, env_value: str | None, file_env: str) -> str | None:
    """Prefer a mounted secret file over an env-var value when both are set.

    Returns ``None`` only when neither source is configured.
    """
    path = os.environ.get(file_env)
    if path:
        try:
            return Path(path).read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            raise RuntimeError(f"{file_env} points at missing file: {path}") from None
    return env_value


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required env var: {name}")
    return value
