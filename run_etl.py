"""Local runner + failure/retry and idempotency demonstrations.

Executes the IDENTICAL task functions the Airflow DAG calls
(src/etl_tasks.py) against local folders standing in for S3 / Redshift.

What a full run does:
  1. Generates 14 days of landing files (with injected DQ issues).
  2. Runs extract -> validate -> transform -> load -> quality_gate
     for each day, in order.
  3. IDEMPOTENCY PROOF: re-runs one day end-to-end and checks the
     warehouse fact count for that day is unchanged.
  4. FAILURE / RETRY PROOF: corrupts one day's landing file, shows the
     day fails at extract (in Airflow: task fails, retries=3), shows
     nothing was loaded for it, regenerates the file correctly,
     re-runs ("retry succeeds"), and verifies the load.
  5. Writes outputs/metrics.json with the measured numbers.

Run:  python run_etl.py
"""
import json, shutil, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))
import pandas as pd
import etl_tasks
from etl_tasks import run_day, CorruptFileError, RAW, WAREHOUSE
from generate_data import generate

ROOT = Path(__file__).parent
OUT = ROOT / "outputs"
DB = WAREHOUSE / "warehouse.db"


def fact_count_for(date: str) -> int:
    import sqlite3
    if not DB.exists():
        return 0
    con = sqlite3.connect(DB)
    n = con.execute("SELECT COUNT(*) FROM fact_orders WHERE load_date = ?",
                    (date,)).fetchone()[0]
    con.close()
    return int(n)


def main():
    t0 = time.time()
    # clean slate
    for folder in [RAW, ROOT / "data" / "processed",
                   ROOT / "data" / "quarantine", WAREHOUSE]:
        shutil.rmtree(folder, ignore_errors=True)
        folder.mkdir(parents=True, exist_ok=True)

    injected = generate(days=14)
    dates = [d.strftime("%Y-%m-%d")
             for d in pd.date_range("2026-10-01", periods=14)]

    # ---- main run ------------------------------------------------------
    totals = {"rows_in": 0, "rows_quarantined": 0, "quarantine_by_issue": {},
              "string_amounts_fixed": 0}
    days_ok = 0
    for date in dates:
        r = run_day(date)
        days_ok += 1
        totals["rows_in"] += r["validate"]["rows_in"]
        totals["rows_quarantined"] += r["validate"]["rows_quarantined"]
        totals["string_amounts_fixed"] += r["transform"]["string_amounts_fixed"]
        for k, v in r["validate"]["quarantine_by_issue"].items():
            totals["quarantine_by_issue"][k] = \
                totals["quarantine_by_issue"].get(k, 0) + v
    rows_loaded = sum(fact_count_for(d) for d in dates)

    # ---- idempotency proof: run 2026-10-05 a second time --------------
    idem_date = "2026-10-05"
    before = fact_count_for(idem_date)
    run_day(idem_date)
    after = fact_count_for(idem_date)
    idempotency = {"date": idem_date, "fact_rows_before_rerun": before,
                   "fact_rows_after_rerun": after,
                   "passed": before == after and before > 0}

    # ---- failure / retry proof on 2026-10-12 ---------------------------
    fail_date = "2026-10-12"
    landing = RAW / f"orders_{fail_date}.csv"
    good_bytes = landing.read_bytes()
    rows_before_corruption = fact_count_for(fail_date)
    landing.write_bytes(b"\x00\x01GARBAGE-NOT-A-CSV\xff\xfe" * 50)
    failure = {"date": fail_date,
               "fact_rows_from_earlier_good_run": rows_before_corruption}
    try:
        etl_tasks.extract(fail_date)
        failure["first_attempt_failed"] = False
    except CorruptFileError as e:
        failure["first_attempt_failed"] = True
        failure["failed_at_task"] = "extract"
        failure["error"] = str(e)[:120]
    failure["rows_added_by_corrupt_attempt"] = \
        fact_count_for(fail_date) - rows_before_corruption
    # NOTE: fact rows from the earlier good run are still there; the
    # corrupt attempt added none. Remove the day and prove a clean retry:
    import sqlite3
    con = sqlite3.connect(DB)
    con.execute("DELETE FROM fact_orders WHERE load_date = ?", (fail_date,))
    con.commit(); con.close()
    landing.write_bytes(good_bytes)  # upstream fixes / regenerates the file
    r = run_day(fail_date)           # the "retry"
    failure["retry_succeeded"] = r["quality_gate"]["gate"] == "passed"
    failure["rows_loaded_on_retry"] = fact_count_for(fail_date)
    failure["passed"] = (failure["first_attempt_failed"]
                         and failure["retry_succeeded"]
                         and failure["rows_loaded_on_retry"] > 0)

    total_fact = 0
    import sqlite3
    con = sqlite3.connect(DB)
    total_fact = con.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    dims = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ["dim_customer", "dim_product", "dim_date"]}
    con.close()

    metrics = {
        "days_processed": days_ok,
        "total_rows_in": totals["rows_in"],
        "rows_loaded_fact_orders": int(total_fact),
        "rows_loaded_across_14_days_before_retry_demo": int(rows_loaded),
        "rows_quarantined": totals["rows_quarantined"],
        "quarantine_by_issue": totals["quarantine_by_issue"],
        "injected_issues_ground_truth": injected,
        "string_amounts_fixed_in_transform": totals["string_amounts_fixed"],
        "dimension_rows": dims,
        "idempotency_check": idempotency,
        "failure_retry_check": failure,
        "pipeline_runtime_seconds": round(time.time() - t0, 2),
    }
    OUT.mkdir(exist_ok=True)
    (OUT / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
