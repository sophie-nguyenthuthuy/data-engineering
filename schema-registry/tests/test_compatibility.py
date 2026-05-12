import pytest
from src.registry.compatibility import CompatibilityChecker, check_compatibility
from src.registry.models import CompatibilityMode, SchemaVersion


@pytest.fixture
def checker():
    return CompatibilityChecker()


USER_V1 = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "email": {"type": "string"},
        "name": {"type": "string"},
    },
    "required": ["id", "email"],
}

USER_V2_BACKWARD_OK = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "email": {"type": "string"},
        "name": {"type": "string"},
        "age": {"type": "integer", "default": 0},  # new optional field → OK
    },
    "required": ["id", "email"],
}

USER_V2_BACKWARD_BREAK = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "email": {"type": "string"},
        "phone": {"type": "string"},  # new required field → BREAK
    },
    "required": ["id", "email", "phone"],
}


def test_backward_compatible_add_optional(checker):
    result = checker.check(USER_V2_BACKWARD_OK, USER_V1, CompatibilityMode.BACKWARD)
    assert result.compatible


def test_backward_incompatible_new_required(checker):
    result = checker.check(USER_V2_BACKWARD_BREAK, USER_V1, CompatibilityMode.BACKWARD)
    assert not result.compatible
    assert any(e.type == "NEW_REQUIRED_FIELD" for e in result.errors)


def test_forward_incompatible_remove_required(checker):
    # Remove 'email' from new schema but it was required in old → forward break
    new = {
        "type": "object",
        "properties": {"id": {"type": "integer"}},
        "required": ["id"],
    }
    result = checker.check(new, USER_V1, CompatibilityMode.FORWARD)
    assert not result.compatible
    assert any(e.type == "REMOVED_REQUIRED_FIELD" for e in result.errors)


def test_full_compatibility_ok(checker):
    result = checker.check(USER_V2_BACKWARD_OK, USER_V1, CompatibilityMode.FULL)
    assert result.compatible


def test_type_change_incompatible(checker):
    new = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},  # was integer → break
            "email": {"type": "string"},
        },
        "required": ["id", "email"],
    }
    result = checker.check(new, USER_V1, CompatibilityMode.BACKWARD)
    assert not result.compatible
    assert any(e.type == "INCOMPATIBLE_TYPE" for e in result.errors)


def test_none_mode_always_passes(checker):
    result = checker.check({}, USER_V1, CompatibilityMode.NONE)
    assert result.compatible


def test_enum_value_removed(checker):
    old = {"type": "object", "properties": {"status": {"type": "string", "enum": ["active", "inactive"]}}}
    new = {"type": "object", "properties": {"status": {"type": "string", "enum": ["active"]}}}
    result = checker.check(new, old, CompatibilityMode.BACKWARD)
    assert not result.compatible
    assert any(e.type == "ENUM_VALUE_REMOVED" for e in result.errors)


def test_transitive_checks_all_versions():
    v1 = SchemaVersion(subject="x", version=1, schema_definition=USER_V1)
    v2 = SchemaVersion(subject="x", version=2, schema_definition=USER_V2_BACKWARD_OK)
    # New schema incompatible with v1 even if compatible with v2
    new_schema = {
        "type": "object",
        "properties": {"id": {"type": "integer"}, "email": {"type": "string"}, "score": {"type": "number"}},
        "required": ["id", "email", "score"],  # 'score' is new required → breaks v1 and v2
    }
    result = check_compatibility(new_schema, [v1, v2], CompatibilityMode.BACKWARD_TRANSITIVE)
    assert not result.compatible
