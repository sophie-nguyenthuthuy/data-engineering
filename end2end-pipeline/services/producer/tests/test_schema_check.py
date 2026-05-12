from unittest.mock import MagicMock

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError

from producer.schema_check import SchemaIncompatibleError, ensure_compatible, subject_for


def test_subject_for_uses_topic_name_strategy():
    assert subject_for("user-interactions") == "user-interactions-value"


def test_compatible_passes_silently():
    sr = MagicMock()
    sr.test_compatibility.return_value = True
    ensure_compatible(sr, "foo-value", '{"type":"string"}')
    sr.test_compatibility.assert_called_once()


def test_incompatible_raises():
    sr = MagicMock()
    sr.test_compatibility.return_value = False
    with pytest.raises(SchemaIncompatibleError):
        ensure_compatible(sr, "foo-value", '{"type":"string"}')


def test_first_time_subject_is_allowed():
    """SR returns 404 when the subject has never been registered — not an error."""
    sr = MagicMock()
    err = SchemaRegistryError(http_status_code=404, error_code=40401, error_message="not found")
    sr.test_compatibility.side_effect = err
    ensure_compatible(sr, "foo-value", '{"type":"string"}')  # should not raise


def test_other_sr_errors_propagate():
    sr = MagicMock()
    err = SchemaRegistryError(http_status_code=500, error_code=50001, error_message="boom")
    sr.test_compatibility.side_effect = err
    with pytest.raises(SchemaRegistryError):
        ensure_compatible(sr, "foo-value", '{"type":"string"}')
