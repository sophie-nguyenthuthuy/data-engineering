"""DLQ + schema + pipeline integration tests."""

from __future__ import annotations

import pytest

from cdc.dlq.router import DLQDecision, DLQReason, DLQRouter
from cdc.events.envelope import DebeziumEnvelope, Op
from cdc.pipeline import Pipeline
from cdc.schema.avro import generate_avro_schema, postgres_to_avro
from cdc.transforms.mask_pii import MaskPII


def _payload(**over) -> dict:
    base = {
        "op": "c",
        "ts_ms": 100,
        "before": None,
        "after": {"id": 1, "email": "a@b.com"},
        "source": {"db": "d", "schema": "public", "table": "u", "ts_ms": 99},
    }
    base.update(over)
    return base


# ----------------------------------------------------------------- DLQ


def test_dlq_routes_invalid_json_as_parse_error():
    out = DLQRouter().route(b"not json")
    assert isinstance(out, DLQDecision)
    assert out.reason is DLQReason.PARSE_ERROR


def test_dlq_routes_unknown_op_correctly():
    out = DLQRouter().route(_payload(op="x"))
    assert isinstance(out, DLQDecision)
    assert out.reason is DLQReason.UNKNOWN_OP


def test_dlq_routes_missing_field_as_missing_field():
    bad = _payload()
    del bad["source"]
    out = DLQRouter().route(bad)
    assert isinstance(out, DLQDecision)
    assert out.reason in (DLQReason.PARSE_ERROR, DLQReason.MISSING_FIELD)


def test_dlq_routes_custom_predicate_rejection():
    router = DLQRouter(
        custom_check=lambda env: env.source.table != "u",
        custom_message="table u is excluded",
    )
    out = router.route(_payload())
    assert isinstance(out, DLQDecision)
    assert out.reason is DLQReason.CUSTOM
    assert "table u is excluded" in out.message


def test_dlq_returns_envelope_on_success():
    out = DLQRouter().route(_payload())
    assert isinstance(out, DebeziumEnvelope)
    assert out.op is Op.CREATE


def test_dlq_router_counts_reasons():
    router = DLQRouter()
    router.route(b"oops")
    router.route(_payload(op="x"))
    router.route(_payload())
    counts = router.counts()
    assert counts.get(DLQReason.PARSE_ERROR, 0) == 1
    assert counts.get(DLQReason.UNKNOWN_OP, 0) == 1


# ------------------------------------------------------------- Avro


def test_postgres_to_avro_known_types():
    assert postgres_to_avro("integer") == "int"
    assert postgres_to_avro("bigint") == "long"
    assert postgres_to_avro("text") == "string"
    assert postgres_to_avro("boolean") == "boolean"


def test_postgres_to_avro_nullable_wraps_in_union():
    assert postgres_to_avro("text", nullable=True) == ["null", "string"]


def test_postgres_to_avro_unknown_type_defaults_to_string_with_logical():
    out = postgres_to_avro("hstore")
    assert isinstance(out, dict)
    assert out["type"] == "string"
    assert out["logicalType"] == "unknown"


def test_postgres_to_avro_uuid_and_timestamp_use_logical_types():
    assert postgres_to_avro("uuid") == {"type": "string", "logicalType": "uuid"}
    assert postgres_to_avro("timestamptz") == {
        "type": "long",
        "logicalType": "timestamp-millis",
    }


def test_generate_avro_schema_basic():
    schema = generate_avro_schema(
        namespace="cdc.public",
        name="Orders",
        columns=[("id", "int", False), ("name", "text", True)],
    )
    assert schema["type"] == "record"
    assert schema["namespace"] == "cdc.public"
    fields = {f["name"]: f for f in schema["fields"]}
    assert fields["id"]["type"] == "int"
    assert fields["name"]["type"] == ["null", "string"]
    assert fields["name"]["default"] is None


def test_generate_avro_schema_rejects_empty_columns():
    with pytest.raises(ValueError):
        generate_avro_schema(namespace="ns", name="N", columns=[])


def test_generate_avro_schema_rejects_duplicate_columns():
    with pytest.raises(ValueError):
        generate_avro_schema(
            namespace="ns",
            name="N",
            columns=[("id", "int", False), ("id", "int", False)],
        )


# ----------------------------------------------------------- Pipeline


def test_pipeline_routes_clean_event_through_transform():
    p = Pipeline(transforms=[MaskPII(columns=("email",))])
    result = p.run([_payload()])
    assert len(result.clean) == 1
    assert result.clean[0].after == {"id": 1, "email": "****"}


def test_pipeline_routes_malformed_to_dlq():
    p = Pipeline(transforms=[])
    result = p.run([_payload(op="x")])
    assert len(result.clean) == 0
    assert len(result.dlq) == 1
    assert result.dlq[0].reason is DLQReason.UNKNOWN_OP


def test_pipeline_records_transform_failures():
    class _Boom:
        name = "boom"

        def apply(self, _env):
            raise RuntimeError("boom")

    p = Pipeline(transforms=[_Boom()])  # type: ignore[list-item]
    result = p.run([_payload()])
    assert len(result.transform_failures) == 1
    assert "boom" in result.transform_failures[0][1]


def test_pipeline_success_rate():
    p = Pipeline()
    result = p.run([_payload(), b"not json"])
    assert result.success_rate() == 0.5


def test_pipeline_total_counts_all_paths():
    p = Pipeline()
    result = p.run([_payload(), b"x"])
    assert result.total() == 2


def test_pipeline_accepts_already_decoded_dict_payloads():
    p = Pipeline()
    result = p.run([_payload()])
    assert len(result.clean) == 1
