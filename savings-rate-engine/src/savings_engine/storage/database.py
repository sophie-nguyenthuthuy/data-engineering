import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from savings_engine.config import settings
from savings_engine.models.db_models import Base

# Ensure the data directory exists for SQLite
if settings.database_url.startswith("sqlite"):
    db_path = settings.database_url.replace("sqlite:///", "")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(settings.database_url, connect_args=connect_args, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _seed_banks()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _seed_banks() -> None:
    from savings_engine.models.db_models import Bank
    from savings_engine.storage.database import SessionLocal

    BANKS = [
        Bank(code="VCB",  name_vi="Vietcombank",         name_en="Bank for Foreign Trade of Vietnam",  website="https://www.vietcombank.com.vn"),
        Bank(code="BIDV", name_vi="BIDV",                 name_en="Bank for Investment and Development", website="https://www.bidv.com.vn"),
        Bank(code="CTG",  name_vi="VietinBank",           name_en="Vietnam Joint Stock Commercial Bank", website="https://www.vietinbank.vn"),
        Bank(code="TCB",  name_vi="Techcombank",          name_en="Vietnam Technological & Commercial Bank", website="https://www.techcombank.com.vn"),
        Bank(code="MBB",  name_vi="MB Bank",              name_en="Military Commercial Joint Stock Bank",  website="https://www.mbbank.com.vn"),
        Bank(code="ACB",  name_vi="ACB",                  name_en="Asia Commercial Bank",                  website="https://www.acb.com.vn"),
        Bank(code="VPB",  name_vi="VPBank",               name_en="Vietnam Prosperity Bank",               website="https://www.vpbank.com.vn"),
        Bank(code="STB",  name_vi="Sacombank",            name_en="Sai Gon Thuong Tin Bank",               website="https://www.sacombank.com"),
        Bank(code="TPB",  name_vi="TPBank",               name_en="Tien Phong Commercial Joint Stock Bank", website="https://tpb.vn"),
        Bank(code="HDB",  name_vi="HDBank",               name_en="Ho Chi Minh City Development Bank",     website="https://www.hdbank.com.vn"),
        Bank(code="MSB",  name_vi="MSB",                  name_en="Maritime Bank",                          website="https://www.msb.com.vn"),
        Bank(code="VIB",  name_vi="VIB",                  name_en="Vietnam International Bank",             website="https://www.vib.com.vn"),
        Bank(code="OCB",  name_vi="OCB",                  name_en="Orient Commercial Bank",                 website="https://www.ocb.com.vn"),
        Bank(code="SEA",  name_vi="SeABank",              name_en="Southeast Asia Commercial Bank",         website="https://www.seabank.com.vn"),
        Bank(code="NAB",  name_vi="Nam A Bank",           name_en="Nam A Commercial Joint Stock Bank",      website="https://www.namabank.com.vn"),
    ]

    with SessionLocal() as db:
        for bank in BANKS:
            if not db.get(Bank, bank.code):
                db.add(bank)
        db.commit()
