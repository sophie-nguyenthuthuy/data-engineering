from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

CATALOG_DB_URL = "sqlite:///./catalog.db"

engine = create_engine(CATALOG_DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    from catalog import models  # noqa: F401
    Base.metadata.create_all(bind=engine)
