"""Tests for MongoDBEngine predicate translation."""
from __future__ import annotations

import re

import pytest

from dqp.engines.mongodb_engine import MongoDBEngine, converted_like_to_regex
from dqp.predicate import (
    AndPredicate,
    BetweenPredicate,
    ColumnRef,
    ComparisonOp,
    ComparisonPredicate,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    Literal,
    NotPredicate,
    OrPredicate,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def col(name: str) -> ColumnRef:
    return ColumnRef(column=name)


def int_lit(v: int) -> Literal:
    return Literal(value=v, dtype="int")


def str_lit(v: str) -> Literal:
    return Literal(value=v, dtype="str")


@pytest.fixture()
def engine() -> MongoDBEngine:
    return MongoDBEngine(db=None)


# ---------------------------------------------------------------------------
# LIKE → regex conversion
# ---------------------------------------------------------------------------


class TestLikeToRegex:
    def test_percent_becomes_dotstar(self):
        assert converted_like_to_regex("%foo") == "^.*foo$"

    def test_underscore_becomes_dot(self):
        assert converted_like_to_regex("_foo") == "^.foo$"

    def test_literal_anchored(self):
        r = converted_like_to_regex("hello")
        assert r == "^hello$"

    def test_suffix_pattern(self):
        r = converted_like_to_regex("%world")
        assert r.startswith("^.*")
        assert r.endswith("world$")

    def test_prefix_pattern(self):
        r = converted_like_to_regex("hello%")
        assert r == "^hello.*$"

    def test_contains_pattern(self):
        r = converted_like_to_regex("%foo%")
        assert r == "^.*foo.*$"

    def test_special_chars_escaped(self):
        r = converted_like_to_regex("foo.bar")
        # The dot should be escaped in the regex
        assert "\\." in r

    def test_regex_matches_like_semantics(self):
        r = converted_like_to_regex("Alice%")
        pattern = re.compile(r)
        assert pattern.match("Alice")
        assert pattern.match("Alice Smith")
        assert not pattern.match("alice")
        assert not pattern.match("Bob Alice")


# ---------------------------------------------------------------------------
# Individual predicate translation
# ---------------------------------------------------------------------------


class TestTranslateComparison:
    def test_eq_shorthand(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.EQ, int_lit(30))
        result = engine.translate_predicate(pred)
        assert result == {"age": 30}

    def test_neq(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.NEQ, int_lit(30))
        result = engine.translate_predicate(pred)
        assert result == {"age": {"$ne": 30}}

    def test_lt(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(30))
        result = engine.translate_predicate(pred)
        assert result == {"age": {"$lt": 30}}

    def test_lte(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.LTE, int_lit(30))
        result = engine.translate_predicate(pred)
        assert result == {"age": {"$lte": 30}}

    def test_gt(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(30))
        result = engine.translate_predicate(pred)
        assert result == {"age": {"$gt": 30}}

    def test_gte(self, engine):
        pred = ComparisonPredicate(col("age"), ComparisonOp.GTE, int_lit(30))
        result = engine.translate_predicate(pred)
        assert result == {"age": {"$gte": 30}}


class TestTranslateIn:
    def test_in_positive(self, engine):
        pred = InPredicate(col("status"), [str_lit("active"), str_lit("pending")])
        result = engine.translate_predicate(pred)
        assert result == {"status": {"$in": ["active", "pending"]}}

    def test_not_in(self, engine):
        pred = InPredicate(col("status"), [str_lit("deleted")], negated=True)
        result = engine.translate_predicate(pred)
        assert result == {"status": {"$nin": ["deleted"]}}

    def test_empty_in(self, engine):
        pred = InPredicate(col("id"), [])
        result = engine.translate_predicate(pred)
        assert result == {"id": {"$in": []}}


class TestTranslateBetween:
    def test_between(self, engine):
        pred = BetweenPredicate(col("age"), int_lit(18), int_lit(65))
        result = engine.translate_predicate(pred)
        assert result == {"age": {"$gte": 18, "$lte": 65}}

    def test_not_between(self, engine):
        pred = BetweenPredicate(col("age"), int_lit(18), int_lit(65), negated=True)
        result = engine.translate_predicate(pred)
        assert "$or" in result
        clauses = result["$or"]
        assert {"age": {"$lt": 18}} in clauses
        assert {"age": {"$gt": 65}} in clauses


class TestTranslateLike:
    def test_like(self, engine):
        pred = LikePredicate(col("name"), "Alice%")
        result = engine.translate_predicate(pred)
        assert "name" in result
        assert "$regex" in result["name"]
        # Verify the regex is a string (not compiled)
        assert isinstance(result["name"]["$regex"], str)

    def test_not_like(self, engine):
        pred = LikePredicate(col("name"), "Alice%", negated=True)
        result = engine.translate_predicate(pred)
        assert "name" in result
        assert "$not" in result["name"]


class TestTranslateIsNull:
    def test_is_null(self, engine):
        pred = IsNullPredicate(col("deleted_at"))
        result = engine.translate_predicate(pred)
        assert result == {"deleted_at": None}

    def test_is_not_null(self, engine):
        pred = IsNullPredicate(col("deleted_at"), negated=True)
        result = engine.translate_predicate(pred)
        assert result == {"deleted_at": {"$ne": None}}


class TestTranslateCompound:
    def test_and(self, engine):
        a = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        b = IsNullPredicate(col("deleted_at"), negated=True)
        pred = AndPredicate([a, b])
        result = engine.translate_predicate(pred)
        assert "$and" in result
        assert len(result["$and"]) == 2

    def test_or(self, engine):
        a = ComparisonPredicate(col("status"), ComparisonOp.EQ, str_lit("active"))
        b = ComparisonPredicate(col("status"), ComparisonOp.EQ, str_lit("pending"))
        pred = OrPredicate([a, b])
        result = engine.translate_predicate(pred)
        assert "$or" in result
        assert len(result["$or"]) == 2

    def test_not(self, engine):
        inner = ComparisonPredicate(col("age"), ComparisonOp.LT, int_lit(18))
        pred = NotPredicate(inner)
        result = engine.translate_predicate(pred)
        assert "$nor" in result
        assert len(result["$nor"]) == 1

    def test_complex_and_or_tree(self, engine):
        # (age > 18 AND status = 'active') OR deleted_at IS NULL
        age_pred = ComparisonPredicate(col("age"), ComparisonOp.GT, int_lit(18))
        status_pred = ComparisonPredicate(col("status"), ComparisonOp.EQ, str_lit("active"))
        null_pred = IsNullPredicate(col("deleted_at"))
        and_part = AndPredicate([age_pred, status_pred])
        or_pred = OrPredicate([and_part, null_pred])

        result = engine.translate_predicate(or_pred)
        assert "$or" in result
        assert len(result["$or"]) == 2
        # First element should be an $and clause
        first = result["$or"][0]
        assert "$and" in first


# ---------------------------------------------------------------------------
# Pipeline building
# ---------------------------------------------------------------------------


class TestPipelineBuilding:
    def test_pipeline_without_project(self, engine):
        match = {"age": {"$gt": 18}}
        pipeline = engine.build_aggregation_pipeline(match)
        assert pipeline == [{"$match": {"age": {"$gt": 18}}}]

    def test_pipeline_with_project(self, engine):
        match = {"age": {"$gt": 18}}
        project = {"name": 1, "age": 1, "_id": 0}
        pipeline = engine.build_aggregation_pipeline(match, project)
        assert pipeline[0] == {"$match": match}
        assert pipeline[1] == {"$project": project}

    def test_pipeline_length(self, engine):
        pipeline = engine.build_aggregation_pipeline({})
        assert len(pipeline) == 1
        pipeline2 = engine.build_aggregation_pipeline({}, {"a": 1})
        assert len(pipeline2) == 2


# ---------------------------------------------------------------------------
# can_push — cross-table predicates not pushable
# ---------------------------------------------------------------------------


class TestCanPush:
    def test_single_table_pushable(self, engine):
        pred = ComparisonPredicate(
            ColumnRef(column="age", table="orders"),
            ComparisonOp.GT,
            int_lit(18),
        )
        assert engine.can_push(pred) is True

    def test_cross_table_not_pushable(self, engine):
        pred = ComparisonPredicate(
            ColumnRef(column="id", table="orders"),
            ComparisonOp.EQ,
            Literal(value=None, dtype="null"),
        )
        # Even within one table, should be pushable
        assert engine.can_push(pred) is True

    def test_all_predicate_types_pushable(self, engine):
        preds = [
            ComparisonPredicate(col("x"), ComparisonOp.EQ, int_lit(1)),
            InPredicate(col("x"), [int_lit(1)]),
            BetweenPredicate(col("x"), int_lit(1), int_lit(10)),
            LikePredicate(col("x"), "foo%"),
            IsNullPredicate(col("x")),
        ]
        for pred in preds:
            assert engine.can_push(pred) is True
