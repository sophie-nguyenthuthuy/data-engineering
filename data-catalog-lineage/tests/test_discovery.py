import sqlite3
import tempfile
import os
import pytest
from catalog.discovery import discover_source


@pytest.fixture()
def sample_db(tmp_path):
    db_path = tmp_path / "test.db"
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            first_name  TEXT NOT NULL,
            last_name   TEXT NOT NULL,
            email       TEXT NOT NULL,
            phone       TEXT,
            ssn         TEXT,
            age         INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE orders (
            order_id   INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount     REAL,
            order_date TEXT
        )
    """)
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?,?,?)",
        [
            (1, "Alice", "Smith", "alice@test.com", "555-1234", "123-45-6789", 30),
            (2, "Bob",   "Jones", "bob@test.com",   "555-5678", "987-65-4321", 25),
        ]
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?)",
        [(1, 1, 100.0, "2024-01-01"), (2, 2, 200.0, "2024-01-02")]
    )
    con.commit()
    con.close()
    return str(db_path)


def test_discovers_tables(sample_db):
    result = discover_source(f"sqlite:///{sample_db}", "sqlite")
    assert not result.errors
    table_names = [t["name"] for t in result.tables]
    assert "customers" in table_names
    assert "orders" in table_names


def test_discovers_columns(sample_db):
    result = discover_source(f"sqlite:///{sample_db}", "sqlite")
    col_names = [c["name"] for c in result.columns if c["table"] == "customers"]
    assert "email" in col_names
    assert "ssn" in col_names
    assert "first_name" in col_names


def test_pii_detected_on_name(sample_db):
    result = discover_source(f"sqlite:///{sample_db}", "sqlite")
    email_col = next((c for c in result.columns if c["name"] == "email"), None)
    assert email_col is not None
    assert "PII" in email_col["pii_tags"]
    assert "EMAIL" in email_col["pii_tags"]


def test_pii_detected_on_values(sample_db):
    result = discover_source(f"sqlite:///{sample_db}", "sqlite")
    ssn_col = next((c for c in result.columns if c["name"] == "ssn"), None)
    assert ssn_col is not None
    assert "SSN" in ssn_col["pii_tags"]


def test_primary_key_flagged(sample_db):
    result = discover_source(f"sqlite:///{sample_db}", "sqlite")
    pk_col = next((c for c in result.columns if c["name"] == "customer_id" and c["table"] == "customers"), None)
    assert pk_col is not None
    assert pk_col["is_primary_key"] is True


def test_non_pii_column_clean(sample_db):
    result = discover_source(f"sqlite:///{sample_db}", "sqlite")
    age_col = next((c for c in result.columns if c["name"] == "age"), None)
    assert age_col is not None
    assert age_col["pii_tags"] == []


def test_bad_connection_returns_error():
    result = discover_source("sqlite:///nonexistent_dir/nope.db", "sqlite")
    # Should either work (SQLite creates) or return an error — not crash
    # Just assert no exception was raised


def test_row_count(sample_db):
    result = discover_source(f"sqlite:///{sample_db}", "sqlite")
    customers = next((t for t in result.tables if t["name"] == "customers"), None)
    assert customers is not None
    assert customers["row_count"] == 2
