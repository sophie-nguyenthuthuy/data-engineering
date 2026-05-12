import sys
from pathlib import Path

# Add lab04_python_etl directory to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from src.etl_pipeline import run_etl

def test_run_etl_returns_clean_rows():
    df = run_etl()
    assert len(df) == 2
    assert (df["amount"] > 0).all()
    assert df["order_date"].notna().all()
    assert "year_month" in df.columns
