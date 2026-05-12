"""
Shared pytest fixtures.

All tests run against an in-memory SQLite DB so they are fast and isolated.
USE_MOCK_DATA is forced True so no real HTTP requests are made.
"""
import os
import pytest

# Force mock data before any engine import
os.environ.setdefault("USE_MOCK_DATA", "true")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from savings_engine.models.db_models import Base, Bank
from savings_engine.storage.repository import RateRepository


@pytest.fixture(scope="session")
def db_engine():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def db_session(db_engine):
    connection = db_engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    # Seed banks
    for code, name_vi, name_en in [
        ("VCB",  "Vietcombank",  "Bank for Foreign Trade"),
        ("BIDV", "BIDV",         "Bank for Investment"),
        ("TCB",  "Techcombank",  "Tech & Commercial Bank"),
        ("MBB",  "MB Bank",      "Military Bank"),
        ("ACB",  "ACB",          "Asia Commercial Bank"),
        ("VPB",  "VPBank",       "Vietnam Prosperity Bank"),
        ("CTG",  "VietinBank",   "Vietnam Joint Stock Bank"),
    ]:
        if not session.get(Bank, code):
            session.add(Bank(code=code, name_vi=name_vi, name_en=name_en))
    session.commit()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture()
def repo(db_session):
    return RateRepository(db_session)
