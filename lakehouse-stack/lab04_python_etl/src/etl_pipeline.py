from pathlib import Path
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

RAW_PATH = Path("data/raw/orders.csv")
OUT_PATH = Path("data/processed/orders_clean.csv")

def run_etl() -> pd.DataFrame:
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {RAW_PATH}")

    df = pd.read_csv(RAW_PATH)
    required_cols = {"order_id", "customer_id", "order_date", "amount", "status"}
    if not required_cols.issubset(df.columns):
        missing = required_cols.difference(df.columns)
        raise ValueError(f"Missing columns: {missing}")

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["amount", "order_date"])
    df = df[df["amount"] > 0].copy()
    df["year_month"] = df["order_date"].dt.strftime("%Y-%m")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    logging.info("Wrote %s rows to %s", len(df), OUT_PATH)
    return df

if __name__ == "__main__":
    run_etl()
