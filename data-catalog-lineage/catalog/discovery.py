"""
Auto-discovery of database assets: schemas, tables, and columns.
Supports SQLite, PostgreSQL, and MySQL via SQLAlchemy inspection.
"""
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from catalog.pii_detector import detect_pii


class DiscoveryResult:
    def __init__(self):
        self.schemas: list[dict] = []
        self.tables: list[dict] = []
        self.columns: list[dict] = []
        self.errors: list[str] = []


def _sample_column(conn, table_name: str, col_name: str, schema: str | None, limit: int = 20) -> list:
    qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    try:
        rows = conn.execute(
            text(f'SELECT "{col_name}" FROM {qualified} WHERE "{col_name}" IS NOT NULL LIMIT {limit}')
        ).fetchall()
        return [row[0] for row in rows]
    except Exception:
        return []


def discover_source(connection_string: str, engine_type: str) -> DiscoveryResult:
    result = DiscoveryResult()
    try:
        engine = create_engine(connection_string, connect_args={"check_same_thread": False} if engine_type == "sqlite" else {})
        inspector = inspect(engine)

        db_schemas = inspector.get_schema_names()
        # Filter out internal schemas
        skip = {"information_schema", "pg_catalog", "pg_toast", "pg_temp_1", "pg_toast_temp_1"}
        db_schemas = [s for s in db_schemas if s not in skip]

        with engine.connect() as conn:
            for schema_name in db_schemas:
                result.schemas.append({"name": schema_name})

                table_names = inspector.get_table_names(schema=schema_name)
                view_names = inspector.get_view_names(schema=schema_name)

                for tname in table_names + view_names:
                    is_view = tname in view_names
                    row_count = None
                    try:
                        qualified = f'"{schema_name}"."{tname}"' if schema_name != "main" else f'"{tname}"'
                        row_count = conn.execute(text(f"SELECT COUNT(*) FROM {qualified}")).scalar()
                    except Exception:
                        pass

                    pk_cols = {c for c in inspector.get_pk_constraint(tname, schema=schema_name).get("constrained_columns", [])}

                    result.tables.append({
                        "schema": schema_name,
                        "name": tname,
                        "is_view": is_view,
                        "row_count": row_count,
                    })

                    raw_cols = inspector.get_columns(tname, schema=schema_name)
                    for col in raw_cols:
                        col_name = col["name"]
                        schema_arg = schema_name if schema_name != "main" else None
                        samples = _sample_column(conn, tname, col_name, schema_arg)
                        pii = detect_pii(col_name, samples)

                        result.columns.append({
                            "schema": schema_name,
                            "table": tname,
                            "name": col_name,
                            "data_type": str(col.get("type", "UNKNOWN")),
                            "is_nullable": col.get("nullable", True),
                            "is_primary_key": col_name in pk_cols,
                            "pii_tags": pii,
                            "sample_values": [str(s) for s in samples[:5]],
                        })

        engine.dispose()
    except SQLAlchemyError as exc:
        result.errors.append(str(exc))

    return result
