import sqlite3, sys
from pathlib import Path
import pandas as pd
import pytest
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
import etl_tasks


@pytest.fixture()
def tiny_day(tmp_path, monkeypatch):
    """Redirect the pipeline's folders to a temp dir with one small day."""
    raw = tmp_path / "raw"; proc = tmp_path / "processed"
    quar = tmp_path / "quarantine"; wh = tmp_path / "warehouse"
    for d in [raw, proc, quar, wh]:
        d.mkdir(parents=True)
    monkeypatch.setattr(etl_tasks, "RAW", raw)
    monkeypatch.setattr(etl_tasks, "PROCESSED", proc)
    monkeypatch.setattr(etl_tasks, "QUARANTINE", quar)
    monkeypatch.setattr(etl_tasks, "WAREHOUSE", wh)
    monkeypatch.setattr(etl_tasks, "DB_PATH", wh / "warehouse.db")
    rows = [
        # order_id, customer, product, qty, amount, ts, channel, status
        ("ORD-1", "CUST-1", "P001", "1", "89.99", "2026-10-01 09:00:00", "web", "completed"),
        ("ORD-1", "CUST-1", "P001", "1", "89.99", "2026-10-01 09:00:00", "web", "completed"),  # duplicate
        ("ORD-2", "", "P002", "1", "119.95", "2026-10-01 10:00:00", "web", "completed"),        # null customer
        ("ORD-3", "CUST-2", "P003", "2", "-37.00", "2026-10-01 11:00:00", "mobile", "completed"),  # negative
        ("ORD-4", "CUST-3", "P004", "1", "39.99", "not-a-time", "web", "completed"),            # bad timestamp
        ("ORD-5", "CUST-4", "P005", "1", "$45.00", "2026-10-01 12:00:00", "web", "completed"),  # fixable string amount
        ("ORD-6", "CUST-5", "P006", "1", "59.99", "2026-10-01 13:00:00", "mobile", "shipped"),
    ]
    pd.DataFrame(rows, columns=etl_tasks.EXPECTED_COLUMNS).to_csv(
        raw / "orders_2026-10-01.csv", index=False)
    return "2026-10-01"


def test_extract_counts_rows(tiny_day):
    assert etl_tasks.extract(tiny_day)["rows_in"] == 7


def test_extract_rejects_corrupt_file(tiny_day):
    (etl_tasks.RAW / "orders_2026-10-01.csv").write_bytes(b"\x00garbage\xff")
    with pytest.raises(etl_tasks.CorruptFileError):
        etl_tasks.extract(tiny_day)


def test_validate_quarantines_each_issue(tiny_day):
    r = etl_tasks.validate(tiny_day)
    assert r["rows_quarantined"] == 4
    assert r["quarantine_by_issue"] == {
        "duplicate_order_id": 1, "null_customer_id": 1,
        "negative_amount": 1, "bad_timestamp": 1}
    assert r["rows_valid"] == 3  # ORD-1, ORD-5 (string amount), ORD-6


def test_transform_fixes_string_amount(tiny_day):
    etl_tasks.validate(tiny_day)
    r = etl_tasks.transform(tiny_day)
    assert r["string_amounts_fixed"] == 1
    df = pd.read_parquet(etl_tasks.PROCESSED / "clean_2026-10-01.parquet")
    row = df[df.order_id == "ORD-5"].iloc[0]
    assert float(row.amount) == 45.00


def test_load_is_idempotent(tiny_day):
    etl_tasks.validate(tiny_day); etl_tasks.transform(tiny_day)
    first = etl_tasks.load_warehouse(tiny_day)
    second = etl_tasks.load_warehouse(tiny_day)  # full re-run of the day
    assert first["fact_orders_total"] == second["fact_orders_total"] == 3
    gate = etl_tasks.quality_gate(tiny_day)
    assert gate["gate"] == "passed" and gate["duplicate_order_ids"] == 0
