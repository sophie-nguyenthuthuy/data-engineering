from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select, update, delete
from sqlalchemy.orm import Session

from models import QuarantineRecord, DriftEvent, init_db

logger = logging.getLogger(__name__)


class QuarantineStore:
    """Persist bad records and drift events; surface them for review and reprocessing."""

    def __init__(self, db_url: str):
        self.engine = init_db(db_url)

    # ------------------------------------------------------------------
    # Record quarantine
    # ------------------------------------------------------------------

    def quarantine_records(
        self,
        records: list[dict[str, Any]],
        pipeline_name: str,
        source_name: str,
        run_id: str,
        error_type: str,
        error_detail: str,
        root_cause_hint: str = "",
        schema_version: int | None = None,
    ) -> int:
        """Bulk-insert quarantined records. Returns count stored."""
        rows = [
            QuarantineRecord(
                pipeline_name=pipeline_name,
                source_name=source_name,
                run_id=run_id,
                record_json=json.dumps(rec, default=str),
                error_type=error_type,
                error_detail=error_detail,
                root_cause_hint=root_cause_hint,
                schema_version=schema_version,
            )
            for rec in records
        ]
        with Session(self.engine) as session:
            session.add_all(rows)
            session.commit()
        logger.info(
            "Quarantined %d records [%s] for source '%s' run '%s'",
            len(rows), error_type, source_name, run_id,
        )
        return len(rows)

    # ------------------------------------------------------------------
    # Drift event logging
    # ------------------------------------------------------------------

    def log_drift_event(
        self,
        pipeline_name: str,
        source_name: str,
        run_id: str,
        drift_type: str,
        details_json: str,
        healed: bool = False,
        healing_action: str = "",
    ) -> int:
        event = DriftEvent(
            pipeline_name=pipeline_name,
            source_name=source_name,
            run_id=run_id,
            drift_type=drift_type,
            details_json=details_json,
            healed=healed,
            healing_action=healing_action,
        )
        with Session(self.engine) as session:
            session.add(event)
            session.commit()
            session.refresh(event)
            return event.id

    # ------------------------------------------------------------------
    # Review helpers
    # ------------------------------------------------------------------

    def get_quarantined(
        self,
        source_name: str | None = None,
        run_id: str | None = None,
        error_type: str | None = None,
        resolved: bool = False,
        limit: int = 500,
    ) -> list[QuarantineRecord]:
        with Session(self.engine) as session:
            q = select(QuarantineRecord).where(QuarantineRecord.resolved == resolved)
            if source_name:
                q = q.where(QuarantineRecord.source_name == source_name)
            if run_id:
                q = q.where(QuarantineRecord.run_id == run_id)
            if error_type:
                q = q.where(QuarantineRecord.error_type == error_type)
            q = q.order_by(QuarantineRecord.quarantined_at.desc()).limit(limit)
            return list(session.scalars(q).all())

    def get_drift_events(
        self, source_name: str | None = None, limit: int = 100
    ) -> list[DriftEvent]:
        with Session(self.engine) as session:
            q = select(DriftEvent).order_by(DriftEvent.detected_at.desc()).limit(limit)
            if source_name:
                q = q.where(DriftEvent.source_name == source_name)
            return list(session.scalars(q).all())

    def mark_resolved(self, record_ids: list[int]) -> int:
        with Session(self.engine) as session:
            session.execute(
                update(QuarantineRecord)
                .where(QuarantineRecord.id.in_(record_ids))
                .values(resolved=True)
            )
            session.commit()
        return len(record_ids)

    def purge_old(self, older_than_days: int) -> int:
        cutoff = datetime.utcnow() - timedelta(days=older_than_days)
        with Session(self.engine) as session:
            result = session.execute(
                delete(QuarantineRecord)
                .where(QuarantineRecord.quarantined_at < cutoff)
                .where(QuarantineRecord.resolved == True)
            )
            session.commit()
            n = result.rowcount
        logger.info("Purged %d resolved quarantine records older than %d days", n, older_than_days)
        return n

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self, pipeline_name: str | None = None) -> dict[str, int]:
        with Session(self.engine) as session:
            base = select(QuarantineRecord)
            if pipeline_name:
                base = base.where(QuarantineRecord.pipeline_name == pipeline_name)
            all_records = list(session.scalars(base).all())
        return {
            "total": len(all_records),
            "unresolved": sum(1 for r in all_records if not r.resolved),
            "resolved": sum(1 for r in all_records if r.resolved),
            "by_error_type": _count_by(all_records, lambda r: r.error_type),
            "by_source": _count_by(all_records, lambda r: r.source_name),
        }


def _count_by(records: list, key_fn) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in records:
        k = key_fn(r)
        counts[k] = counts.get(k, 0) + 1
    return counts
