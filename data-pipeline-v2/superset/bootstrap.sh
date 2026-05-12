#!/usr/bin/env bash
set -euo pipefail

# Initialize metadata DB and create admin user on first boot.
superset db upgrade

superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER}" \
    --firstname admin \
    --lastname admin \
    --email admin@example.com \
    --password "${SUPERSET_ADMIN_PASSWORD}" || echo "admin already exists"

superset init

# Register the analytics database as a Superset data source using bi_read.
python - <<'PY'
import os
from superset import db
from superset.app import create_app
from superset.models.core import Database

app = create_app()
with app.app_context():
    uri = (
        f"postgresql+psycopg2://bi_read:{os.environ['BI_READ_PASSWORD']}"
        f"@postgres:5432/analytics"
    )
    existing = db.session.query(Database).filter_by(database_name="analytics").first()
    if existing:
        existing.sqlalchemy_uri = uri
    else:
        db.session.add(Database(database_name="analytics", sqlalchemy_uri=uri))
    db.session.commit()
    print("registered analytics database")
PY

exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 4 \
    --timeout 120 \
    "superset.app:create_app()"
