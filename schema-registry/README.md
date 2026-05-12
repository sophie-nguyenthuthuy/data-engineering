# Schema Registry with Compatibility Enforcement + Auto-Migration

A production-grade schema registry that goes beyond Confluent's basic offering with:

- **7 compatibility modes** — BACKWARD, FORWARD, FULL, NONE, and all three `*_TRANSITIVE` variants
- **Auto-migration generation** — diff two schema versions and get a ready-to-use migration script
- **Declarative transformation DSL** — YAML/JSON DSL for hand-crafted migrations (rename, split, merge, cast, map, compute, etc.)
- **Event replay engine** — replay a batch of historical events through a migration chain to any target version
- **REST API** (FastAPI) + **CLI** (Typer/Rich)

---

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Start the server
schema-registry serve

# Or with Docker
docker compose up
```

---

## Architecture

```
schema-registry/
├── src/
│   ├── registry/
│   │   ├── models.py          # Pydantic data models
│   │   ├── storage.py         # Async SQLite backend
│   │   ├── compatibility.py   # 7-mode compatibility checker
│   │   └── core.py            # Registry façade
│   ├── migration/
│   │   ├── dsl.py             # Declarative DSL parser/serializer
│   │   ├── generator.py       # Auto-generate migration from schema diff
│   │   └── executor.py        # Apply migration steps to payloads
│   ├── replay/
│   │   └── engine.py          # Event replay engine (batch + streaming)
│   └── api/
│       ├── app.py             # FastAPI app factory
│       ├── routes.py          # All REST endpoints
│       └── schemas.py         # Request/response Pydantic models
├── cli/
│   └── main.py                # Typer CLI
├── tests/                     # pytest test suite
└── examples/                  # Sample schemas, DSL, events
```

---

## Compatibility Modes

| Mode | Description |
|------|-------------|
| `NONE` | No checks — any schema accepted |
| `BACKWARD` | New schema can read data written with old schema |
| `FORWARD` | Old schema can read data written with new schema |
| `FULL` | Both BACKWARD and FORWARD |
| `BACKWARD_TRANSITIVE` | BACKWARD checked against ALL previous versions |
| `FORWARD_TRANSITIVE` | FORWARD checked against ALL previous versions |
| `FULL_TRANSITIVE` | FULL checked against ALL previous versions |

---

## REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/subjects` | List all subjects |
| DELETE | `/api/v1/subjects/{subject}` | Delete subject + all versions |
| GET | `/api/v1/config/{subject}` | Get subject config |
| PUT | `/api/v1/config/{subject}` | Set compatibility mode |
| GET | `/api/v1/subjects/{subject}/versions` | List versions |
| GET | `/api/v1/subjects/{subject}/versions/{version}` | Get schema |
| POST | `/api/v1/subjects/{subject}/versions` | Register new schema |
| DELETE | `/api/v1/subjects/{subject}/versions/{version}` | Delete version |
| POST | `/api/v1/compatibility/subjects/{subject}/versions` | Check compatibility |
| GET | `/api/v1/subjects/{subject}/migrations` | List migrations |
| POST | `/api/v1/subjects/{subject}/migrations/generate/{from}/{to}` | Auto-generate migration |
| PUT | `/api/v1/subjects/{subject}/migrations/{from}/{to}/dsl` | Upload DSL migration |
| POST | `/api/v1/subjects/{subject}/migrate` | Apply migration to payload |
| POST | `/api/v1/subjects/{subject}/replay` | Replay events to target version |

Interactive docs: `http://localhost:8000/docs`

---

## Transformation DSL

```yaml
version: 1
description: "User v1 → v2"
steps:
  - op: rename_field
    path: "$.name"
    params:
      to: "full_name"

  - op: split_field
    path: "$.full_name"
    params:
      into: ["first_name", "last_name"]
      separator: " "

  - op: add_field
    path: "$.metadata"
    params:
      default: {}

  - op: cast_field
    path: "$.age"
    params:
      to_type: integer

  - op: map_value
    path: "$.status"
    params:
      mapping:
        "0": "inactive"
        "1": "active"

  - op: merge_fields
    path: "$.address"
    params:
      sources: ["street", "city", "zip"]
      template: "{street}, {city} {zip}"
```

### Supported Operations

| Op | Description |
|----|-------------|
| `rename_field` | Rename a field |
| `add_field` | Add field with default if missing |
| `remove_field` | Remove a field |
| `cast_field` | Cast field to a new type |
| `map_value` | Map discrete values to new values |
| `split_field` | Split one field into multiple |
| `merge_fields` | Merge multiple fields into one |
| `copy_field` | Copy field to a new key |
| `default_field` | Set default if field is null/missing |
| `flatten_field` | Flatten nested object into parent |
| `wrap_field` | Wrap field value in an object |
| `compute_field` | Compute field from expression |

---

## CLI

```bash
# Start server
schema-registry serve

# Subjects
schema-registry subjects list
schema-registry subjects delete users

# Schemas
schema-registry schemas register users examples/user_v1.json
schema-registry schemas register users examples/user_v2.json
schema-registry schemas list users
schema-registry schemas get users latest

# Compatibility
schema-registry config set users BACKWARD
schema-registry compat check users examples/user_v2.json

# Migrations
schema-registry migrate generate users 1 2
schema-registry migrate upload users 1 2 examples/migration_v1_v2.yaml
schema-registry migrate apply users 1 2 payload.json
schema-registry migrate list users

# Replay
schema-registry replay run users examples/events.json 2
```

---

## End-to-End Example

```bash
# 1. Start server
schema-registry serve &

# 2. Set compatibility mode
curl -X PUT http://localhost:8000/api/v1/config/users \
  -H 'Content-Type: application/json' \
  -d '{"compatibility": "BACKWARD"}'

# 3. Register schemas
curl -X POST http://localhost:8000/api/v1/subjects/users/versions \
  -H 'Content-Type: application/json' \
  -d @examples/user_v1.json  # wrapped in {"schema_definition": ...}

# 4. Auto-generate migration
curl -X POST http://localhost:8000/api/v1/subjects/users/migrations/generate/1/2

# 5. Replay historical events
curl -X POST http://localhost:8000/api/v1/subjects/users/replay \
  -H 'Content-Type: application/json' \
  -d '{"events": [...], "target_version": 2, "validate": true}'
```

---

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v --cov=src
```

---

## License

MIT
