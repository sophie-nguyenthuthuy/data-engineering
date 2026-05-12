"""Shared pytest fixtures — all tests run fully in-memory / on temp disk."""

import os
import pytest
import tempfile

# Force local mode for all tests
os.environ["KMS_MODE"] = "local"
os.environ["STORAGE_MODE"] = "local"


@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


@pytest.fixture
def cfg(tmp_path):
    """Override config to use temp directory."""
    import src.config as config_module
    config_module._config = None  # reset singleton

    os.environ["LOCAL_KMS_STORE_PATH"] = str(tmp_path / "kms_store.json")
    os.environ["LOCAL_KMS_MASTER_KEY_PATH"] = str(tmp_path / "master.key")
    os.environ["KEY_REGISTRY_PATH"] = str(tmp_path / "key_registry.json")
    os.environ["LOCAL_STORAGE_PATH"] = str(tmp_path / "records")

    cfg = config_module.get_config()
    yield cfg

    config_module._config = None


@pytest.fixture
def kms_client(cfg):
    from src.kms.client import KMSClient
    import src.kms.client as kms_mod
    kms_mod._local_kms_instance = None  # reset singleton
    return KMSClient()


@pytest.fixture
def registry(cfg):
    from src.kms.key_registry import KeyRegistry
    return KeyRegistry(cfg.key_registry_path)


@pytest.fixture
def store(cfg):
    from src.storage.s3_store import RecordStore
    return RecordStore()


@pytest.fixture
def engine(kms_client):
    from src.encryption.engine import EncryptionEngine
    return EncryptionEngine(kms_client)


@pytest.fixture
def pipeline(cfg, kms_client, engine, registry, store):
    from src.pipeline.ingest import IngestPipeline
    return IngestPipeline(kms_client, engine, registry, store)


@pytest.fixture
def rotation_pipeline(cfg, kms_client, engine, registry, store):
    from src.pipeline.rotation import RotationPipeline
    return RotationPipeline(kms_client, engine, registry, store, progress=False)


@pytest.fixture
def rtbf_executor(cfg, kms_client, registry, store, tmp_path):
    from src.rtbf.executor import RTBFExecutor
    return RTBFExecutor(kms_client, registry, store, audit_log_path=str(tmp_path / "audit.jsonl"))


@pytest.fixture
def sample_row():
    return {
        "ssn": "123-45-6789",
        "email": "alice@example.com",
        "phone": "555-0100",
        "full_name": "Alice Example",
        "product_id": "prod_abc",
        "amount": 99.99,
    }
