"""Generate realistic sample transaction data for testing."""
import csv
import random
import hashlib
from datetime import datetime, timedelta

random.seed(42)

ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "LOAN", "FOREX", "TERM_DEPOSIT"]
CURRENCIES = ["VND", "USD", "EUR", "JPY", "GBP"]
TRANSACTION_TYPES = [
    "TRANSFER_IN", "TRANSFER_OUT", "CASH_DEPOSIT", "CASH_WITHDRAWAL",
    "FX_BUY", "FX_SELL", "LOAN_DISBURSEMENT", "LOAN_REPAYMENT",
    "INTEREST_CREDIT", "FEE_DEBIT",
]
BRANCHES = ["HN001", "HN002", "HCM001", "HCM002", "DN001", "CT001"]
COUNTERPARTY_BANKS = [
    "BIDV", "VCB", "ACB", "TCB", "MBB", "VPB", "STB", "EIB", None
]

def make_account(i):
    return f"{random.choice(['1', '2', '3', '4', '5'])}{i:09d}"

def make_txn_id(dt, seq):
    raw = f"{dt.strftime('%Y%m%d')}{seq:06d}"
    return f"TXN{raw}{hashlib.md5(raw.encode()).hexdigest()[:4].upper()}"

def generate(n=500, start="2025-01-01"):
    rows = []
    accounts = [make_account(i) for i in range(1, 51)]
    base = datetime.strptime(start, "%Y-%m-%d")

    for i in range(n):
        dt = base + timedelta(
            days=random.randint(0, 89),
            hours=random.randint(8, 17),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        currency = random.choices(CURRENCIES, weights=[70, 15, 7, 5, 3])[0]
        txn_type = random.choice(TRANSACTION_TYPES)

        if currency == "VND":
            amount = random.choices(
                [random.uniform(50_000, 5_000_000),
                 random.uniform(5_000_000, 100_000_000),
                 random.uniform(100_000_000, 500_000_000),
                 random.uniform(500_000_000, 5_000_000_000)],
                weights=[50, 30, 15, 5]
            )[0]
        else:
            amount = random.uniform(100, 50_000)

        debit_acct = random.choice(accounts)
        credit_acct = random.choice([a for a in accounts if a != debit_acct])

        rows.append({
            "transaction_id": make_txn_id(dt, i + 1),
            "transaction_date": dt.strftime("%Y-%m-%d"),
            "transaction_time": dt.strftime("%H:%M:%S"),
            "transaction_type": txn_type,
            "debit_account": debit_acct,
            "credit_account": credit_acct,
            "account_type": random.choice(ACCOUNT_TYPES),
            "currency": currency,
            "amount": round(amount, 2),
            "vnd_equivalent": round(amount * (1 if currency == "VND" else
                                    23_500 if currency == "USD" else
                                    25_000 if currency == "EUR" else
                                    160 if currency == "JPY" else
                                    29_000), 0),
            "branch_code": random.choice(BRANCHES),
            "counterparty_bank": random.choice(COUNTERPARTY_BANKS),
            "counterparty_account": make_account(random.randint(51, 200)),
            "counterparty_name": f"Khách hàng {random.randint(1000, 9999)}",
            "purpose_code": random.choice(["01", "02", "03", "04", "05", "06", "07"]),
            "status": random.choices(["SUCCESS", "FAILED", "PENDING"], weights=[90, 5, 5])[0],
            "channel": random.choice(["COUNTER", "INTERNET_BANKING", "MOBILE", "ATM", "API"]),
            "operator_id": f"OP{random.randint(1000, 9999)}",
            "reference_number": f"REF{random.randint(100000, 999999)}",
            "narrative": f"Giao dịch số {i+1}",
        })

    return rows

if __name__ == "__main__":
    rows = generate(500)
    fieldnames = list(rows[0].keys())
    with open("transactions.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Generated {len(rows)} transactions → transactions.csv")
