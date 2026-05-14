"""Transform tests."""

from __future__ import annotations

import re

import pytest

from cdc.events.envelope import DebeziumEnvelope, Op, SourceInfo
from cdc.transforms.flatten import FlattenAfter
from cdc.transforms.mask_pii import MaskPII
from cdc.transforms.rename import RenameColumns


def _src() -> SourceInfo:
    return SourceInfo(db="d", schema="public", table="t", ts_ms=1)


def _create(row: dict) -> DebeziumEnvelope:
    return DebeziumEnvelope(op=Op.CREATE, source=_src(), ts_ms=10, after=row)


def _update(before: dict, after: dict) -> DebeziumEnvelope:
    return DebeziumEnvelope(op=Op.UPDATE, source=_src(), ts_ms=10, before=before, after=after)


def _delete(row: dict) -> DebeziumEnvelope:
    return DebeziumEnvelope(op=Op.DELETE, source=_src(), ts_ms=10, before=row)


# ---------------------------------------------------------------- Flatten


def test_flatten_create_picks_after():
    env = _create({"id": 1, "name": "alice"})
    out = FlattenAfter().apply(env)
    assert out.extra["row"] == {"id": 1, "name": "alice"}


def test_flatten_delete_picks_before():
    env = _delete({"id": 1, "name": "alice"})
    out = FlattenAfter().apply(env)
    assert out.extra["row"] == {"id": 1, "name": "alice"}


def test_flatten_preserves_envelope_fields():
    env = _create({"id": 1})
    out = FlattenAfter().apply(env)
    assert out.op == env.op
    assert out.source == env.source
    assert out.ts_ms == env.ts_ms


# ---------------------------------------------------------- MaskPII


def test_mask_pii_replaces_named_columns():
    env = _create({"id": 1, "email": "a@b.com", "ssn": "123-45-6789"})
    out = MaskPII(columns=("email", "ssn")).apply(env)
    assert out.after == {"id": 1, "email": "****", "ssn": "****"}


def test_mask_pii_masks_both_before_and_after():
    env = _update(
        before={"id": 1, "email": "a@b.com"},
        after={"id": 1, "email": "c@d.com"},
    )
    out = MaskPII(columns=("email",)).apply(env)
    assert out.before == {"id": 1, "email": "****"}
    assert out.after == {"id": 1, "email": "****"}


def test_mask_pii_regex_redacts_substrings():
    env = _create({"id": 1, "note": "contact a@b.com asap"})
    out = MaskPII(
        regex_columns=("note",),
        pattern=re.compile(r"[\w\.]+@[\w\.]+"),
    ).apply(env)
    assert out.after == {"id": 1, "note": "contact **** asap"}


def test_mask_pii_rejects_empty_config():
    with pytest.raises(ValueError):
        MaskPII()


def test_mask_pii_requires_pattern_when_regex_columns_set():
    with pytest.raises(ValueError):
        MaskPII(regex_columns=("note",))


def test_mask_pii_no_op_on_unrelated_column():
    env = _create({"id": 1, "name": "alice"})
    out = MaskPII(columns=("email",)).apply(env)
    assert out.after == {"id": 1, "name": "alice"}


# ----------------------------------------------------------- Rename


def test_rename_renames_columns_in_after():
    env = _create({"id": 1, "old_name": "x"})
    out = RenameColumns(mapping={"old_name": "new_name"}).apply(env)
    assert out.after == {"id": 1, "new_name": "x"}


def test_rename_renames_columns_in_before():
    env = _update(before={"old_a": 1}, after={"old_a": 2})
    out = RenameColumns(mapping={"old_a": "new_a"}).apply(env)
    assert out.before == {"new_a": 1}
    assert out.after == {"new_a": 2}


def test_rename_rejects_empty_mapping():
    with pytest.raises(ValueError):
        RenameColumns(mapping={})


def test_rename_rejects_duplicate_destination():
    with pytest.raises(ValueError):
        RenameColumns(mapping={"a": "x", "b": "x"})


def test_rename_leaves_unlisted_columns_alone():
    env = _create({"id": 1, "x": 2, "y": 3})
    out = RenameColumns(mapping={"x": "x2"}).apply(env)
    assert out.after == {"id": 1, "x2": 2, "y": 3}
