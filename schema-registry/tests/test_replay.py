import pytest
import pytest_asyncio
from src.registry.core import SchemaRegistry
from src.registry.models import TransformEvent
from src.replay.engine import ReplayEngine


@pytest_asyncio.fixture
async def registry(tmp_path):
    reg = SchemaRegistry(db_path=str(tmp_path / "test.db"))
    await reg.start()

    v1 = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
        },
        "required": ["id"],
    }
    v2 = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "full_name": {"type": "string"},
            "age": {"type": "integer", "default": 0},
        },
        "required": ["id"],
    }
    await reg.set_config("users", "NONE")  # skip compat for test setup
    await reg.register_schema("users", v1)
    await reg.register_schema("users", v2)
    yield reg
    await reg.stop()


@pytest.mark.asyncio
async def test_replay_basic(registry):
    events = [
        TransformEvent(event_id="e1", subject="users", schema_version=1, payload={"id": 1, "name": "Alice"}),
        TransformEvent(event_id="e2", subject="users", schema_version=1, payload={"id": 2, "name": "Bob"}),
    ]
    engine = ReplayEngine(registry)
    result = await engine.replay("users", events, target_version=2, validate=False)
    assert result.total == 2
    assert result.succeeded == 2
    assert result.failed == 0


@pytest.mark.asyncio
async def test_replay_same_version(registry):
    events = [
        TransformEvent(event_id="e1", subject="users", schema_version=2, payload={"id": 1, "full_name": "Alice"}),
    ]
    engine = ReplayEngine(registry)
    result = await engine.replay("users", events, target_version=2, validate=False)
    assert result.succeeded == 1
    assert result.events[0]["payload"]["full_name"] == "Alice"
