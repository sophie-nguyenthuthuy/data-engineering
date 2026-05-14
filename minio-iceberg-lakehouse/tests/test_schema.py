"""Schema + evolution tests."""

from __future__ import annotations

import pytest

from lake.schema import Field, FieldType, Schema, SchemaEvolutionError


def _s() -> Schema:
    return Schema(
        schema_id=0,
        fields=(
            Field(id=1, name="id", type=FieldType.LONG, required=True),
            Field(id=2, name="amount", type=FieldType.DOUBLE),
        ),
    )


def test_field_rejects_bad_id():
    with pytest.raises(ValueError):
        Field(id=0, name="x", type=FieldType.INT)


def test_field_rejects_empty_name():
    with pytest.raises(ValueError):
        Field(id=1, name="", type=FieldType.INT)


def test_schema_rejects_empty_fields():
    with pytest.raises(ValueError):
        Schema(schema_id=0, fields=())


def test_schema_rejects_duplicate_field_id():
    with pytest.raises(ValueError):
        Schema(
            schema_id=0,
            fields=(
                Field(id=1, name="a", type=FieldType.INT),
                Field(id=1, name="b", type=FieldType.INT),
            ),
        )


def test_schema_rejects_duplicate_field_name():
    with pytest.raises(ValueError):
        Schema(
            schema_id=0,
            fields=(
                Field(id=1, name="a", type=FieldType.INT),
                Field(id=2, name="a", type=FieldType.INT),
            ),
        )


def test_field_by_id_and_name():
    s = _s()
    assert s.field_by_id(1).name == "id"
    assert s.field_by_name("amount").id == 2


def test_add_column_assigns_fresh_id():
    s = _s().add_column("country", FieldType.STRING)
    assert s.schema_id == 1
    assert s.field_by_name("country").id == 3


def test_add_column_rejects_existing_name():
    with pytest.raises(SchemaEvolutionError):
        _s().add_column("amount", FieldType.STRING)


def test_drop_column_keeps_others():
    s = _s().drop_column("amount")
    with pytest.raises(KeyError):
        s.field_by_name("amount")
    assert s.field_by_name("id").id == 1


def test_drop_last_column_rejected():
    s = Schema(schema_id=0, fields=(Field(id=1, name="only", type=FieldType.INT),))
    with pytest.raises(SchemaEvolutionError):
        s.drop_column("only")


def test_rename_preserves_id():
    s = _s().rename_column("amount", "total")
    assert s.field_by_name("total").id == 2


def test_rename_rejects_collision():
    with pytest.raises(SchemaEvolutionError):
        _s().rename_column("amount", "id")


def test_promote_type_allowed():
    s = _s().promote_type("id", FieldType.LONG)  # long → long is idempotent
    assert s.field_by_name("id").type is FieldType.LONG


def test_promote_type_rejects_lossy():
    s = _s()
    with pytest.raises(SchemaEvolutionError):
        s.promote_type("amount", FieldType.INT)  # double → int is lossy


def test_field_by_id_unknown_raises():
    with pytest.raises(KeyError):
        _s().field_by_id(999)
