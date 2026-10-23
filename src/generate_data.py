"""Synthetic daily e-commerce order files with injected data-quality issues.

Generates one CSV per day for 14 days (~5,000 orders/day) into
data/raw/orders_YYYY-MM-DD.csv, mimicking files landing in an S3 bucket
from an upstream order system.

Injected issues are counted as they are generated and written to
outputs/injected_issues.json, so the pipeline's quarantine counts can be
verified against ground truth:

  duplicate_order_id   extra rows re-using an existing order_id
  null_customer_id     customer_id left blank
  negative_amount      amount < 0
  bad_timestamp        order_timestamp that cannot be parsed
  string_amount        amount formatted as "$1,234.56" (fixable in transform)

AWS mapping: in production these files arrive in s3://bucket/landing/
via the order system's export / Kinesis Firehose; this generator stands
in for that upstream producer.
"""
import json
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
OUT = ROOT / "outputs"

PRODUCTS = [
    # product_id, name, category, base_price
    ("P001", "Wireless Headphones", "electronics", 89.99),
    ("P002", "Running Shoes", "apparel", 119.95),
    ("P003", "Coffee Beans 1kg", "groceries", 18.50),
    ("P004", "Yoga Mat", "fitness", 39.99),
    ("P005", "Desk Lamp", "home", 45.00),
    ("P006", "Bluetooth Speaker", "electronics", 59.99),
    ("P007", "Winter Jacket", "apparel", 149.00),
    ("P008", "Protein Powder", "groceries", 54.95),
    ("P009", "Standing Desk", "home", 499.00),
    ("P010", "Smart Watch", "electronics", 249.99),
    ("P011", "Trail Backpack", "fitness", 79.95),
    ("P012", "Ceramic Dinner Set", "home", 69.99),
]
CHANNELS = ["web", "mobile", "marketplace"]
STATUSES = ["completed", "completed", "completed", "completed", "shipped", "returned"]


def generate(days: int = 14, orders_per_day: int = 5_000, seed: int = 42,
             start_date: str = "2026-10-01") -> dict:
    rng = np.random.default_rng(seed)
    RAW.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    prod_ids = [p[0] for p in PRODUCTS]
    prod_price = {p[0]: p[3] for p in PRODUCTS}

    injected = {"duplicate_order_id": 0, "null_customer_id": 0,
                "negative_amount": 0, "bad_timestamp": 0, "string_amount": 0}
    dates = pd.date_range(start_date, periods=days, freq="D")

    for day_idx, date in enumerate(dates):
        n = orders_per_day
        order_ids = [f"ORD-{date.strftime('%Y%m%d')}-{i:05d}" for i in range(n)]
        qty = rng.integers(1, 5, n)
        chosen = rng.choice(prod_ids, n)
        amounts = np.round(np.array([prod_price[p] for p in chosen]) * qty *
                           rng.uniform(0.9, 1.1, n), 2)
        df = pd.DataFrame({
            "order_id": order_ids,
            "customer_id": [f"CUST-{i:05d}" for i in rng.integers(1, 3001, n)],
            "product_id": chosen,
            "quantity": qty,
            "amount": amounts.astype(str),
            "order_timestamp": [
                (date + pd.Timedelta(seconds=int(s))).strftime("%Y-%m-%d %H:%M:%S")
                for s in rng.integers(0, 86400, n)],
            "channel": rng.choice(CHANNELS, n, p=[.55, .35, .10]),
            "status": rng.choice(STATUSES, n),
        })

        # --- inject issues in known counts (per day) -------------------
        def pick(k):
            return rng.choice(df.index, size=k, replace=False)

        idx = pick(20)  # null customer_id
        df.loc[idx, "customer_id"] = ""
        injected["null_customer_id"] += 20

        idx = pick(15)  # negative amount
        df.loc[idx, "amount"] = (-df.loc[idx, "amount"].astype(float)).astype(str)
        injected["negative_amount"] += 15

        idx = pick(10)  # bad timestamp
        df.loc[idx, "order_timestamp"] = rng.choice(
            ["not-a-timestamp", "32/13/2026 99:99", ""], size=10)
        injected["bad_timestamp"] += 10

        idx = pick(40)  # fixable string amount with currency symbol
        df.loc[idx, "amount"] = df.loc[idx, "amount"].astype(float).map(
            lambda v: f"${v:,.2f}")
        injected["string_amount"] += 40

        dup_src = df.iloc[rng.choice(len(df), size=25, replace=False)]
        df = pd.concat([df, dup_src], ignore_index=True)  # duplicate order_ids
        injected["duplicate_order_id"] += 25

        df = df.sample(frac=1.0, random_state=seed + day_idx).reset_index(drop=True)
        df.to_csv(RAW / f"orders_{date.strftime('%Y-%m-%d')}.csv", index=False)

    (OUT / "injected_issues.json").write_text(json.dumps(injected, indent=2))
    print(json.dumps(injected, indent=2))
    return injected


if __name__ == "__main__":
    generate()
