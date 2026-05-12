import pytest
from src.migration.dsl import DSLParseError, TransformationDSL
from src.migration.executor import MigrationExecutor
from src.migration.generator import MigrationGenerator
from src.registry.models import MigrationScript, MigrationStep


DSL_YAML = """
version: 1
description: "Test migration"
steps:
  - op: rename_field
    path: "$.name"
    params:
      to: "full_name"
  - op: add_field
    path: "$.created_at"
    params:
      default: null
  - op: cast_field
    path: "$.age"
    params:
      to_type: integer
  - op: remove_field
    path: "$.legacy"
"""


@pytest.fixture
def dsl():
    return TransformationDSL()


@pytest.fixture
def executor():
    return MigrationExecutor()


def test_dsl_parse(dsl):
    steps = dsl.parse(DSL_YAML)
    assert len(steps) == 4
    assert steps[0].op == "rename_field"
    assert steps[1].op == "add_field"


def test_dsl_invalid_op(dsl):
    bad = "version: 1\nsteps:\n  - op: nonexistent\n    path: '$.x'"
    with pytest.raises(DSLParseError):
        dsl.parse(bad)


def test_dsl_round_trip(dsl):
    steps = dsl.parse(DSL_YAML)
    yaml_out = dsl.to_yaml(steps)
    steps2 = dsl.parse(yaml_out)
    assert len(steps) == len(steps2)


def test_executor_rename(executor):
    payload = {"name": "Alice", "email": "alice@example.com"}
    script = MigrationScript(
        subject="test", from_version=1, to_version=2,
        steps=[MigrationStep(op="rename_field", path="$.name", params={"to": "full_name"})],
    )
    result = executor.apply(payload, script)
    assert "full_name" in result
    assert "name" not in result
    assert result["full_name"] == "Alice"


def test_executor_split(executor):
    payload = {"full_name": "John Doe"}
    script = MigrationScript(
        subject="test", from_version=1, to_version=2,
        steps=[MigrationStep(
            op="split_field",
            path="$.full_name",
            params={"into": ["first_name", "last_name"], "separator": " "},
        )],
    )
    result = executor.apply(payload, script)
    assert result["first_name"] == "John"
    assert result["last_name"] == "Doe"
    assert "full_name" not in result


def test_executor_merge(executor):
    payload = {"street": "123 Main St", "city": "Springfield", "zip": "12345"}
    script = MigrationScript(
        subject="test", from_version=1, to_version=2,
        steps=[MigrationStep(
            op="merge_fields",
            path="$.address",
            params={
                "sources": ["street", "city", "zip"],
                "template": "{street}, {city} {zip}",
            },
        )],
    )
    result = executor.apply(payload, script)
    assert result["address"] == "123 Main St, Springfield 12345"


def test_executor_cast(executor):
    payload = {"age": "25"}
    script = MigrationScript(
        subject="test", from_version=1, to_version=2,
        steps=[MigrationStep(op="cast_field", path="$.age", params={"to_type": "integer"})],
    )
    result = executor.apply(payload, script)
    assert result["age"] == 25
    assert isinstance(result["age"], int)


def test_executor_map_value(executor):
    payload = {"status": "0"}
    script = MigrationScript(
        subject="test", from_version=1, to_version=2,
        steps=[MigrationStep(
            op="map_value",
            path="$.status",
            params={"mapping": {"0": "inactive", "1": "active"}},
        )],
    )
    result = executor.apply(payload, script)
    assert result["status"] == "inactive"


def test_generator_detect_breaking():
    old = {
        "type": "object",
        "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
        "required": ["id"],
    }
    new = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "full_name": {"type": "string"},
            "score": {"type": "number"},
        },
        "required": ["id", "score"],
    }
    gen = MigrationGenerator()
    script = gen.generate("users", 1, 2, old, new)
    assert script.auto_generated
    assert any("score" in bc for bc in script.breaking_changes)
    ops = [s.op for s in script.steps]
    assert "remove_field" in ops   # name removed
    assert "add_field" in ops      # full_name and score added
