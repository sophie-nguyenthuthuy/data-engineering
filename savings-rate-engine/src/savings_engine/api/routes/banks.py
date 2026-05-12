from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from savings_engine.storage.database import get_db
from savings_engine.storage.repository import RateRepository

router = APIRouter()


class BankOut(BaseModel):
    code: str
    name_vi: str
    name_en: str
    website: str | None
    snapshot_count: int

    model_config = {"from_attributes": True}


@router.get("", response_model=list[BankOut])
def list_banks(db: Session = Depends(get_db)):
    """Return all active banks tracked by the engine."""
    repo = RateRepository(db)
    banks = repo.get_all_banks()
    return [
        BankOut(
            code=b.code,
            name_vi=b.name_vi,
            name_en=b.name_en,
            website=b.website,
            snapshot_count=repo.get_snapshot_count(b.code),
        )
        for b in banks
    ]


@router.get("/{bank_code}", response_model=BankOut)
def get_bank(bank_code: str, db: Session = Depends(get_db)):
    repo = RateRepository(db)
    bank = repo.get_bank(bank_code.upper())
    if not bank:
        raise HTTPException(404, f"Bank '{bank_code}' not found")
    return BankOut(
        code=bank.code,
        name_vi=bank.name_vi,
        name_en=bank.name_en,
        website=bank.website,
        snapshot_count=repo.get_snapshot_count(bank.code),
    )
