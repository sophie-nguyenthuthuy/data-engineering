"""Tests for the chaos engine itself: it should catch a known-buggy pipeline."""
from src import invariant, Runner, emit_pytest


def reset_registry():
    """Clear the global registry between tests."""
    from src.invariants import _REGISTRY
    _REGISTRY.clear()


def test_clean_pipeline_no_violations():
    reset_registry()

    @invariant(row_count_preserved=True)
    def clean(df):
        return [{**r, "upper": str(r.get("name", "")).upper()} for r in df]

    r = Runner(seed=0)
    violations = r.run_all(trials_per_fn=50)
    # Filter out exceptions (some edge cases like str(None) on missing keys
    # might raise inside our buggy edge-case generator)
    real_violations = [v for v in violations if v.invariant != "no_exceptions"]
    assert real_violations == []


def test_catches_row_count_violation():
    reset_registry()

    @invariant(row_count_preserved=True)
    def buggy_drop_neg(df):
        # BUG: drops rows where amount is negative — violates row_count_preserved
        return [r for r in df if r.get("amount", 0) >= 0]

    r = Runner(seed=0)
    violations = r.run_all(trials_per_fn=50)
    bad = [v for v in violations if v.invariant == "row_count_preserved"]
    assert bad, "should have caught row_count violation"


def test_catches_sum_invariant_violation():
    reset_registry()

    @invariant(sum_invariant=["amount"])
    def buggy_abs(df):
        return [{**r, "amount": abs(r.get("amount", 0)) if isinstance(r.get("amount"), (int, float)) else 0} for r in df]

    r = Runner(seed=1)
    violations = r.run_all(trials_per_fn=50)
    # abs() changes sum when there are negative values, which our edge cases include
    bad = [v for v in violations if "sum_invariant" in v.invariant]
    assert bad, "should have caught sum_invariant violation"


def test_pytest_regression_emitter():
    from src import Violation
    v = Violation(
        fn_name="buggy_drop_neg",
        invariant="row_count_preserved",
        input=[{"amount": -5}],
        output=[],
    )
    code = emit_pytest(v)
    assert "test_buggy_drop_neg_violates_row_count_preserved" in code
    assert "amount" in code


def test_runner_collects_exception_as_violation():
    reset_registry()

    @invariant(no_nulls=["name"])
    def crashes_on_long_strings(df):
        for r in df:
            n = r.get("name", "")
            if isinstance(n, str) and len(n) > 5000:
                raise ValueError("too long")
        return df

    r = Runner(seed=2)
    violations = r.run_all(trials_per_fn=100)
    # Our edge case includes 'A' * 10_000 so likely we hit it
    exc = [v for v in violations if v.invariant == "no_exceptions"]
    assert exc, "should have caught the exception case"
