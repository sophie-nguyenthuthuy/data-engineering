# Contract Schema Reference

A data contract is a YAML file that declares everything a downstream consumer can depend on.

## Top-level fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | ✅ | Unique contract identifier (slug format) |
| `version` | string | ✅ | Semantic version: `MAJOR.MINOR.PATCH` |
| `producer` | string | ✅ | Service/team that owns and produces this data |
| `consumers` | list[string] | | Teams or services that depend on this data |
| `owner` | string | | Owning team |
| `description` | string | | Human-readable purpose |
| `tags` | list[string] | | Categorization labels |
| `fields` | list[Field] | | Schema definition |
| `sla_rules` | list[SLARule] | | Service-level agreement rules |
| `semantic_rules` | list[SemanticRule] | | Custom data quality predicates |

---

## Field definition

```yaml
fields:
  - name: order_id        # column name
    type: string          # see type list below
    nullable: false       # default false
    description: "..."
    constraints:
      unique: true
      min: 0
      max: 100000
      allowed_values: [a, b, c]
```

### Supported types

| Type | Maps to pandas dtype |
|------|----------------------|
| `string` | object, StringDtype |
| `integer` | int8–int64, Int8–Int64 |
| `number` | int/float variants |
| `boolean` | bool, boolean |
| `date` | object, datetime64 |
| `timestamp` | object, datetime64 |
| `array` | object |
| `object` | object |

---

## SLA rules

```yaml
sla_rules:
  - name: minimum_rows
    rule_type: row_count      # freshness | completeness | row_count | latency
    threshold: 500
    unit: ""                  # s (seconds) for freshness/latency
    description: "..."
```

### Rule types

| `rule_type` | Threshold meaning |
|------------|-------------------|
| `row_count` | Minimum number of rows |
| `completeness` | Minimum non-null fraction (0–1) |
| `freshness` | Maximum data age in seconds |
| `latency` | Maximum pipeline processing time in seconds |

---

## Semantic rules

```yaml
semantic_rules:
  - name: positive_totals
    expression: "(df['amount'] >= 0).all()"
    severity: error   # error | warning
    description: "..."
```

The expression is evaluated with `df` bound to the validated `pandas.DataFrame`.
Return `True` to pass, `False` to fail. Any exception is treated as an error.

---

## Versioning & breaking changes

Follow semantic versioning:

- **PATCH** (`1.0.0 → 1.0.1`): Description changes, new optional fields, loosened constraints.
- **MINOR** (`1.0.0 → 1.1.0`): New nullable fields, new SLA rules, tightened warnings.
- **MAJOR** (`1.0.0 → 2.0.0`): Removed fields, renamed fields, type changes, tightened nullability.

The platform automatically detects and flags MAJOR breaking changes in CI.
