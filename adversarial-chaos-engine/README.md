# adversarial-chaos-engine

Chaos engineering that doesn't just inject random failures — it **generates targeted adversarial inputs designed to violate pipeline invariants**. Property-based testing meets symbolic execution: the engine analyses pipeline code, identifies edge-case input regions, and produces inputs that hit them.

Discovered bugs auto-generate a regression suite.

> **Status:** Design / spec phase.

## Why

Random fuzzing wastes 99 % of cycles on inputs the system handles correctly. Targeted adversarial inputs find:

- The batch size that's exactly one row past the spill-to-disk threshold.
- The timestamp that lands on a daylight savings boundary.
- The string with 4 bytes of UTF-8 that look like 5 bytes of valid CESU-8.
- The Decimal that's exactly the max precision plus an exponent overflow.
- The join key with a hash collision against the spill partition pivot.

Each is a real production bug class. None come up under random fuzzing in a reasonable amount of time.

## Architecture

```
                Pipeline code
                      │
                      ▼
            ┌──────────────────────┐
            │ Static analyzer      │   AST → constraints
            │  (Python / SQL)      │   per branch
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │ Symbolic executor    │   path constraints
            │  (z3-backed)         │   per code path
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │ Invariant catalog    │   user-declared:
            │                      │   - row_count_preserved
            │                      │   - sum_invariant
            │                      │   - schema_compatible
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │ Adversarial gen      │   z3 solves for
            │                      │   inputs violating invariants
            └──────────┬───────────┘
                       │
                       ▼
              Run on pipeline → captured failures
                       │
                       ▼
            ┌──────────────────────┐
            │ Regression emitter   │   per-bug pytest case
            └──────────────────────┘
```

## Components

| Module | Role |
|---|---|
| `src/analyzer/python_ast.py` | Walk Python source, build path-constraint graph |
| `src/analyzer/sql_ast.py` | Same for SQL transforms (CTEs, window funcs) |
| `src/symbolic/` | z3-based path executor; tracks branch conditions |
| `src/invariants/` | Invariant DSL + catalog (row count, sum, schema, NULL-safety) |
| `src/generators/` | Hypothesis strategies seeded by symbolic edge cases |
| `src/runner/` | Spawns pipeline against generated input; captures violations |
| `src/regression/` | Emits pytest cases from captured failures |
| `src/eval/` | Compare bug-discovery rate vs. random fuzzing + Hypothesis alone |

## Invariant DSL

Users annotate transformations:

```python
@invariant(row_count="preserved")
@invariant(sum_invariant=["amount"])
def clean_transactions(df):
    df["amount"] = df["amount"].abs()  # bug: not invariant if amount was negative
    return df
```

The engine sees these annotations, generates inputs where `amount` is negative, and discovers the violation.

## Edge-case seeding

Static analysis identifies boundary conditions:

- Branches on numeric thresholds → generate values one above + below.
- String comparisons → generate Unicode normalization variants, RTL, zero-width joiners.
- Time arithmetic → DST boundaries, leap seconds, Y2K38, BC dates.
- Floating-point comparisons → ULP boundaries, subnormals, ±0.
- Hash-based operations → known collision-prone inputs.

These seed Hypothesis strategies, which then shrink failures to minimal counterexamples.

## Symbolic execution

For each code path, build a constraint:

```
path = [branch_1_true, branch_2_false, branch_3_true, ...]
constraint = ∧(branch conditions)
```

Solve with z3 for inputs satisfying the constraint AND violating an invariant. The combination ensures we find inputs that *reach* the buggy code AND *trigger* the bug.

For SQL: translate predicates and join conditions to first-order logic; z3 finds inputs satisfying join + filter + violating output invariant.

## The discovered-regression flow

1. Engine finds a violating input `I`.
2. Captures (input, code-revision, expected_invariant, actual_output).
3. Hypothesis shrinks `I` to minimal counter-example `I*`.
4. Emits a pytest:

```python
def test_clean_transactions_preserves_sum_invariant_amount():
    # auto-discovered 2026-05-11; bug in commit a7f3...
    df_in = pd.DataFrame({"amount": [-50, 30, -10]})
    df_out = clean_transactions(df_in)
    assert df_out["amount"].sum() == df_in["amount"].sum()  # FAILS
```

5. PR posted to the pipeline repo.

## Benchmarks

Compare bug-discovery on a corpus of pipelines with seeded bugs:

| Method | Bugs found / hour | False-positive rate |
|---|---|---|
| Random fuzzing | baseline | 0 |
| Hypothesis only | 3–5× | 0 |
| This (Hypothesis + symbolic + invariants) | target 20× | 0 |

Plus: discovers ≥ 80 % of the seeded bugs within 1 hour on each pipeline.

## References

- Cadar et al., "KLEE: Unassisted and Automatic Generation of High-Coverage Tests" (OSDI 2008)
- Hypothesis: MacIver, "Property-Based Testing in Python" (2015)
- Godefroid et al., "DART: Directed Automated Random Testing" (PLDI 2005)

## Roadmap

- [ ] Python AST analyzer + invariant decorators
- [ ] SQL AST analyzer
- [ ] z3 path solver
- [ ] Edge-case seed library (DST, Unicode, IEEE, hash)
- [ ] Hypothesis strategy generator
- [ ] Runner (pipeline-agnostic)
- [ ] Regression emitter
- [ ] Bug-discovery benchmark
