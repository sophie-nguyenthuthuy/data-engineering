from datetime import datetime
from sqlalchemy import (
    Column, String, Float, Integer, DateTime, ForeignKey, Boolean, Index, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Bank(Base):
    __tablename__ = "banks"

    code = Column(String(20), primary_key=True)  # e.g. "VCB", "BIDV"
    name_vi = Column(String(200), nullable=False)
    name_en = Column(String(200), nullable=False)
    website = Column(String(300))
    logo_url = Column(String(300))
    active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    snapshots = relationship("RateSnapshot", back_populates="bank", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Bank {self.code}>"


class RateSnapshot(Base):
    """One scrape run per bank produces one snapshot containing N rate records."""
    __tablename__ = "rate_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bank_code = Column(String(20), ForeignKey("banks.code"), nullable=False)
    scraped_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    scrape_success = Column(Boolean, default=True, nullable=False)
    error_message = Column(String(500))

    bank = relationship("Bank", back_populates="snapshots")
    records = relationship("RateRecord", back_populates="snapshot", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_snapshots_bank_scraped", "bank_code", "scraped_at"),
    )


class RateRecord(Base):
    """A single interest rate row within a snapshot."""
    __tablename__ = "rate_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(Integer, ForeignKey("rate_snapshots.id"), nullable=False)
    bank_code = Column(String(20), nullable=False)  # denormalized for fast queries
    term_days = Column(Integer, nullable=False)       # canonical term in days
    term_label = Column(String(50))                   # original label, e.g. "3 tháng"
    rate_pa = Column(Float, nullable=False)            # % per annum
    rate_type = Column(String(30), default="standard") # standard | online | promotional
    min_amount_vnd = Column(Integer)                   # minimum deposit in VND, nullable
    currency = Column(String(5), default="VND")

    snapshot = relationship("RateSnapshot", back_populates="records")

    __table_args__ = (
        Index("ix_records_bank_term", "bank_code", "term_days"),
        Index("ix_records_snapshot", "snapshot_id"),
    )
