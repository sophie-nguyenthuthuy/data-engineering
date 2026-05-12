import pytest
from src.transforms.mapper import FieldMapper
from src.models import FieldMapping


def test_no_mappings_passthrough():
    mapper = FieldMapper([])
    records = [{"a": 1, "b": "hello"}]
    assert mapper.apply(records) == records


def test_field_rename():
    mapper = FieldMapper([FieldMapping(source="old_name", destination="new_name")])
    result = mapper.apply([{"old_name": "Alice", "age": 30}])
    assert result[0]["new_name"] == "Alice"
    assert result[0]["age"] == 30  # unmapped field carried over


def test_transform_upper():
    mapper = FieldMapper([FieldMapping(source="name", destination="name", transform="upper")])
    result = mapper.apply([{"name": "alice"}])
    assert result[0]["name"] == "ALICE"


def test_transform_int():
    mapper = FieldMapper([FieldMapping(source="score", destination="score", transform="int")])
    result = mapper.apply([{"score": "42"}])
    assert result[0]["score"] == 42


def test_transform_invalid_silently_keeps_original():
    mapper = FieldMapper([FieldMapping(source="val", destination="val", transform="int")])
    result = mapper.apply([{"val": "not_a_number"}])
    # The transform fails; FieldMapper logs a warning and keeps the raw value
    assert result[0]["val"] == "not_a_number"


def test_multiple_records():
    mapper = FieldMapper([FieldMapping(source="email", destination="email", transform="lower")])
    records = [{"email": "Alice@Example.COM"}, {"email": "BOB@TEST.ORG"}]
    result = mapper.apply(records)
    assert result[0]["email"] == "alice@example.com"
    assert result[1]["email"] == "bob@test.org"
