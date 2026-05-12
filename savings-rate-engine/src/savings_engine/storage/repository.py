from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, desc, func

from savings_engine.models.db_models import Bank, RateSnapshot, RateRecord
from savings_engine.models.schemas import NormalizedRate


class RateRepository:
    def __init__(self, db: Session):
        self.db = db

    # --- writes ---

    def save_snapshot(self, bank_code: str, rates: list[NormalizedRate], *, error: Optional[str] = None) -> RateSnapshot:
        snapshot = RateSnapshot(
            bank_code=bank_code,
            scraped_at=datetime.utcnow(),
            scrape_success=error is None,
            error_message=error,
        )
        self.db.add(snapshot)
        self.db.flush()  # get snapshot.id

        for r in rates:
            self.db.add(RateRecord(
                snapshot_id=snapshot.id,
                bank_code=r.bank_code,
                term_days=r.term_days,
                term_label=r.term_label,
                rate_pa=r.rate_pa,
                rate_type=r.rate_type,
                min_amount_vnd=r.min_amount_vnd,
                currency=r.currency,
            ))

        self.db.commit()
        self.db.refresh(snapshot)
        return snapshot

    # --- reads ---

    def get_latest_rates(self, bank_code: Optional[str] = None) -> list[RateRecord]:
        """Return the most recent successful rate records for each bank (or a specific bank)."""
        # Subquery: latest snapshot id per bank
        sub = (
            select(func.max(RateSnapshot.id).label("max_id"))
            .where(RateSnapshot.scrape_success == True)  # noqa: E712
            .group_by(RateSnapshot.bank_code)
        )
        if bank_code:
            sub = sub.where(RateSnapshot.bank_code == bank_code)

        stmt = (
            select(RateRecord)
            .where(RateRecord.snapshot_id.in_(sub))
            .order_by(RateRecord.bank_code, RateRecord.term_days)
        )
        return list(self.db.execute(stmt).scalars())

    def get_rate_history(
        self,
        bank_code: str,
        term_days: int,
        rate_type: str = "standard",
        since: Optional[datetime] = None,
    ) -> list[tuple[datetime, float]]:
        """Return (scraped_at, rate_pa) time series for a specific bank+term."""
        if since is None:
            since = datetime.utcnow() - timedelta(days=90)

        stmt = (
            select(RateSnapshot.scraped_at, RateRecord.rate_pa)
            .join(RateRecord, RateRecord.snapshot_id == RateSnapshot.id)
            .where(
                RateSnapshot.bank_code == bank_code,
                RateSnapshot.scrape_success == True,  # noqa: E712
                RateSnapshot.scraped_at >= since,
                RateRecord.term_days == term_days,
                RateRecord.rate_type == rate_type,
            )
            .order_by(RateSnapshot.scraped_at)
        )
        return [(row.scraped_at, row.rate_pa) for row in self.db.execute(stmt)]

    def get_best_rates(self, term_days: int, top_n: int = 10) -> list[RateRecord]:
        """Best rates across all banks for a given term from the latest snapshots."""
        sub = (
            select(func.max(RateSnapshot.id).label("max_id"))
            .where(RateSnapshot.scrape_success == True)  # noqa: E712
            .group_by(RateSnapshot.bank_code)
        )
        stmt = (
            select(RateRecord)
            .where(
                RateRecord.snapshot_id.in_(sub),
                RateRecord.term_days == term_days,
            )
            .order_by(desc(RateRecord.rate_pa))
            .limit(top_n)
        )
        return list(self.db.execute(stmt).scalars())

    def get_all_banks(self, active_only: bool = True) -> list[Bank]:
        stmt = select(Bank)
        if active_only:
            stmt = stmt.where(Bank.active == True)  # noqa: E712
        return list(self.db.execute(stmt).scalars())

    def get_bank(self, bank_code: str) -> Optional[Bank]:
        return self.db.get(Bank, bank_code)

    def get_available_terms(self) -> list[int]:
        stmt = select(RateRecord.term_days).distinct().order_by(RateRecord.term_days)
        return [row for row in self.db.execute(stmt).scalars()]

    def get_snapshot_count(self, bank_code: str) -> int:
        stmt = (
            select(func.count())
            .select_from(RateSnapshot)
            .where(RateSnapshot.bank_code == bank_code)
        )
        return self.db.execute(stmt).scalar_one()
