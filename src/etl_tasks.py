"""ETL task functions for the AWS ETL pipeline.

These are the EXACT functions the Airflow DAG (dags/aws_etl_dag.py) calls
via PythonOperator, and the exact functions the local runner
(run_etl.py) calls. Only the storage layer differs between local and AWS:

  Local (this repo)                     AWS production equivalent
  ----------------------------------    ----------------------------------------
  data/raw/            (landing)        s3://bucket/landing/ — read with
                                        boto3 / awswrangler, or a Glue crawler
  data/processed/      (clean Parquet)  s3://bucket/processed/ written by an
                                        AWS Glue PySpark job (same transforms:
                                        this module is the job's logic ported
                                        1:1 from pandas to DataFrame API)
  data/quarantine/                      s3://bucket/quarantine/ + SNS alert
  data/warehouse/warehouse.db (SQLite)  Amazon Redshift — load with
                                        COPY fact_orders FROM 's3://.../'
                                        or INSERT after a staging-table
                                        DELETE for the load date (the same
                                        delete-then-insert idempotency used
                                        here in load_warehouse())

No AWS calls happen at import time or in the local runner. Every task takes
an execution `date` (the Airflow logical date / {{ ds }}) so runs are
per-day, retryable and idempotent.
"""
import sqlite3
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
QUARANTINE = ROOT / "data" / "quarantine"
WAREHOUSE = ROOT / "data" / "warehouse"
DB_PATH = WAREHOUSE / "warehouse.db"

PRODUCT_CATALOG = {
    "P001": ("Wireless Headphones", "electronics"), "P002": ("Running Shoes", "apparel"),
    "P003": ("Coffee Beans 1kg", "groceries"), "P004": ("Yoga Mat", "fitness"),
    "P005": ("Desk Lamp", "home"), "P006": ("Bluetooth Speaker", "electronics"),
    "P007": ("Winter Jacket", "apparel"), "P008": ("Protein Powder", "groceries"),
    "P009": ("Standing Desk", "home"), "P010": ("Smart Watch", "electronics"),
    "P011": ("Trail Backpack", "fitness"), "P012": ("Ceramic Dinner Set", "home"),
}
EXPECTED_COLUMNS = ["order_id", "customer_id", "product_id", "quantity",
                    "amount", "order_timestamp", "channel", "status"]


class CorruptFileError(RuntimeError):
    """Raised when a day's landing file is missing or unparseable.

    In Airflow this exception fails the extract task, Airflow retries it
    (retries=3), and the rest of that day's DAG run never starts — other
    days are unaffected.
    """


# ---------------------------------------------------------------- extract
def extract(date: str) -> dict:
    """Check the day's landing file exists and is readable; return row count.

    AWS: boto3 s3.head_object + a Glue crawler schema check on
    s3://bucket/landing/orders_<date>.csv before the Glue job starts.
    """
    path = RAW / f"orders_{date}.csv"
    if not path.exists():
        raise CorruptFileError(f"landing file missing: {path.name}")
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception as e:  # truncated / binary-garbled export
        raise CorruptFileError(f"landing file unreadable: {path.name}: {e}") from e
    if list(df.columns) != EXPECTED_COLUMNS or len(df) == 0:
        raise CorruptFileError(
            f"landing file corrupt: {path.name} "
            f"(columns={list(df.columns)}, rows={len(df)})")
    return {"date": date, "rows_in": int(len(df))}


# --------------------------------------------------------------- validate
def validate(date: str) -> dict:
    """Run data-quality checks; write bad rows to quarantine with a reason.

    Fixable string amounts ("$1,234.56") are NOT quarantined — they are
    repaired in transform(). Everything else that fails a check is
    quarantined per row with the first failing reason, and counted.

    AWS: same checks as Great Expectations / Glue Data Quality rules;
    quarantine written to s3://bucket/quarantine/<date>/ with an SNS
    alert when any count is non-zero.
    """
    df = pd.read_csv(RAW / f"orders_{date}.csv", dtype=str, keep_default_na=False)
    df["_reason"] = ""

    def flag(mask, reason):
        df.loc[mask & (df["_reason"] == ""), "_reason"] = reason

    flag(df["order_id"].duplicated(keep="first"), "duplicate_order_id")
    flag(df["customer_id"].str.strip() == "", "null_customer_id")
    parsed_ts = pd.to_datetime(df["order_timestamp"], errors="coerce")
    flag(parsed_ts.isna(), "bad_timestamp")
    numeric_amt = pd.to_numeric(
        df["amount"].str.replace(r"[$,]", "", regex=True), errors="coerce")
    flag(numeric_amt.isna(), "bad_amount")
    flag(numeric_amt.fillna(0) < 0, "negative_amount")

    bad = df[df["_reason"] != ""].drop(columns=[])
    good = df[df["_reason"] == ""].drop(columns=["_reason"])
    QUARANTINE.mkdir(parents=True, exist_ok=True)
    bad_out = bad.rename(columns={"_reason": "quarantine_reason"})
    bad_out.to_csv(QUARANTINE / f"quarantine_{date}.csv", index=False)
    good.to_parquet(PROCESSED / f"validated_{date}.parquet", index=False)
    counts = bad_out["quarantine_reason"].value_counts().to_dict()
    return {"date": date, "rows_in": int(len(df)),
            "rows_valid": int(len(good)),
            "rows_quarantined": int(len(bad)),
            "quarantine_by_issue": {k: int(v) for k, v in counts.items()}}


# -------------------------------------------------------------- transform
def transform(date: str) -> dict:
    """Repair fixable values and type the clean rows (the Glue job step).

    Fixes: "$1,234.56" -> 1234.56 float, timestamps parsed to datetime,
    quantity to int, strings trimmed. Writes typed Parquet.
    AWS: this is the AWS Glue PySpark job reading s3 processed/validated
    and writing partitioned Parquet back to s3://bucket/processed/.
    """
    df = pd.read_parquet(PROCESSED / f"validated_{date}.parquet")
    fixed_amounts = int(df["amount"].astype(str).str.contains(r"[$,]").sum())
    df["amount"] = pd.to_numeric(
        df["amount"].astype(str).str.replace(r"[$,]", "", regex=True))
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])
    df["quantity"] = df["quantity"].astype(int)
    df["customer_id"] = df["customer_id"].str.strip()
    df["load_date"] = date
    df.to_parquet(PROCESSED / f"clean_{date}.parquet", index=False)
    return {"date": date, "rows_clean": int(len(df)),
            "string_amounts_fixed": fixed_amounts}


# ------------------------------------------------------------------- load
def load_warehouse(date: str) -> dict:
    """Load one day into the star schema, idempotently.

    Idempotency: fact_orders rows for this load_date are DELETEd before
    INSERT, and dimensions are upserted on their natural keys — so
    re-running a day (Airflow retry, backfill, manual rerun) never
    double-loads. AWS/Redshift equivalent: staging table + transaction
        BEGIN; DELETE FROM fact_orders WHERE load_date = '<date>';
        INSERT INTO fact_orders SELECT ... FROM staging; COMMIT;
    or a Redshift COPY into staging followed by the same merge.
    """
    WAREHOUSE.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(PROCESSED / f"clean_{date}.parquet")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id TEXT PRIMARY KEY, first_seen_date TEXT);
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id TEXT PRIMARY KEY, product_name TEXT, category TEXT);
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INTEGER PRIMARY KEY, full_date TEXT, year INT,
            month INT, day INT, day_of_week TEXT);
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id TEXT PRIMARY KEY, customer_id TEXT, product_id TEXT,
            date_key INTEGER, quantity INT, amount REAL, channel TEXT,
            status TEXT, load_date TEXT);
    """)
    # dimensions (upserts on natural keys)
    for pid, (name, cat) in PRODUCT_CATALOG.items():
        cur.execute("INSERT OR IGNORE INTO dim_product VALUES (?,?,?)",
                    (pid, name, cat))
    for cid, first_seen in df.groupby("customer_id")["load_date"].min().items():
        cur.execute("INSERT OR IGNORE INTO dim_customer VALUES (?,?)",
                    (cid, first_seen))
    d = pd.Timestamp(date)
    cur.execute("INSERT OR IGNORE INTO dim_date VALUES (?,?,?,?,?,?)",
                (int(d.strftime("%Y%m%d")), date, d.year, d.month, d.day,
                 d.day_name()))
    # fact: delete-then-insert for this load_date
    cur.execute("DELETE FROM fact_orders WHERE load_date = ?", (date,))
    fact = df.assign(date_key=int(d.strftime("%Y%m%d")))[
        ["order_id", "customer_id", "product_id", "date_key", "quantity",
         "amount", "channel", "status", "load_date"]]
    fact.to_sql("fact_orders", con, if_exists="append", index=False)
    total = cur.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    day_rows = cur.execute(
        "SELECT COUNT(*) FROM fact_orders WHERE load_date = ?",
        (date,)).fetchone()[0]
    con.commit(); con.close()
    return {"date": date, "rows_loaded_day": int(day_rows),
            "fact_orders_total": int(total)}


# ------------------------------------------------------------ quality gate
def quality_gate(date: str) -> dict:
    """Fail the run if the loaded day looks wrong (post-load checks).

    AWS: a final Airflow task running Redshift SQL checks (or Glue Data
    Quality) — row-count reconciliation landing vs warehouse and a
    zero-null-key check; failure here marks the DAG run failed and
    Airflow's on_failure_callback pages via SNS/Slack.
    """
    con = sqlite3.connect(DB_PATH)
    day_rows = con.execute(
        "SELECT COUNT(*) FROM fact_orders WHERE load_date = ?",
        (date,)).fetchone()[0]
    null_keys = con.execute(
        "SELECT COUNT(*) FROM fact_orders WHERE load_date = ? AND "
        "(customer_id IS NULL OR customer_id = '' OR amount IS NULL)",
        (date,)).fetchone()[0]
    dup_ids = con.execute(
        "SELECT COUNT(*) FROM (SELECT order_id FROM fact_orders "
        "GROUP BY order_id HAVING COUNT(*) > 1)").fetchone()[0]
    con.close()
    if day_rows == 0 or null_keys > 0 or dup_ids > 0:
        raise RuntimeError(
            f"quality gate failed for {date}: rows={day_rows}, "
            f"null_keys={null_keys}, duplicate_order_ids={dup_ids}")
    return {"date": date, "rows_loaded": int(day_rows),
            "null_keys": int(null_keys), "duplicate_order_ids": int(dup_ids),
            "gate": "passed"}


def run_day(date: str) -> dict:
    """Execute the DAG's task chain for one day, in dependency order."""
    result = {"extract": extract(date)}
    result["validate"] = validate(date)
    result["transform"] = transform(date)
    result["load"] = load_warehouse(date)
    result["quality_gate"] = quality_gate(date)
    return result
