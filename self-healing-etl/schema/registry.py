from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session

from models import Base, SchemaVersion, init_db

logger = logging.getLogger(__name__)


class SchemaRegistry:
    """Persists expected schemas per source and versions them on evolution."""

    def __init__(self, db_url: str):
        self.engine = init_db(db_url)

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, source_name: str, schema: dict[str, str]) -> int:
        """Store a new schema version; returns the version number."""
        with Session(self.engine) as session:
            current_version = self._latest_version(session, source_name)
            new_version = (current_version or 0) + 1

            # deactivate previous
            if current_version is not None:
                session.execute(
                    update(SchemaVersion)
                    .where(SchemaVersion.source_name == source_name)
                    .where(SchemaVersion.is_active == True)
                    .values(is_active=False)
                )

            session.add(
                SchemaVersion(
                    source_name=source_name,
                    version=new_version,
                    schema_json=json.dumps(schema),
                    is_active=True,
                )
            )
            session.commit()
            logger.info("Registered schema v%d for source '%s'", new_version, source_name)
            return new_version

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get_active(self, source_name: str) -> Optional[tuple[int, dict[str, str]]]:
        """Return (version, schema_dict) for the active schema, or None."""
        with Session(self.engine) as session:
            row = session.scalars(
                select(SchemaVersion)
                .where(SchemaVersion.source_name == source_name)
                .where(SchemaVersion.is_active == True)
            ).first()
            if row is None:
                return None
            return row.version, row.get_schema()

    def get_history(self, source_name: str) -> list[tuple[int, dict[str, str], datetime]]:
        with Session(self.engine) as session:
            rows = session.scalars(
                select(SchemaVersion)
                .where(SchemaVersion.source_name == source_name)
                .order_by(SchemaVersion.version)
            ).all()
            return [(r.version, r.get_schema(), r.registered_at) for r in rows]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _latest_version(self, session: Session, source_name: str) -> Optional[int]:
        row = session.scalars(
            select(SchemaVersion)
            .where(SchemaVersion.source_name == source_name)
            .where(SchemaVersion.is_active == True)
        ).first()
        return row.version if row else None
