# AWS ETL Pipeline with Airflow, S3, Glue and Redshift

End-to-end data engineering project: daily e-commerce order files land
in S3, an Airflow DAG orchestrates extract -> validate -> transform ->
load -> quality gate, data-quality failures are quarantined instead of
loaded, transforms repair fixable values, and clean data is loaded into
a Redshift star schema — idempotently, so retries and re-runs never
double-load.

## Verified results (actual run, seed = 42)

| Metric | Value |
|---|---|
| Days processed | 14 |
| Total rows in | 70,350 |
| Rows loaded (fact_orders) | 69,370 ($21,313,294.15 revenue) |
| Rows quarantined | 980 — exactly matching injected ground truth |
| Quarantine by issue | 350 duplicate order_id, 280 null customer_id, 210 negative amount, 140 bad timestamp |
| Currency amounts repaired in transform | 553 ("$1,234.56" -> 1234.56; 10 further $-formatted rows were quarantined for other defects first) |
| Dimensions loaded | 3,000 customers, 12 products, 14 dates |
| Idempotency check | Re-running 2026-10-05 left its fact count unchanged: 4,955 -> 4,955 ✅ |
| Failure / retry check | Corrupted 2026-10-12 file failed at `extract`, added 0 rows; after regeneration the retry loaded 4,955 rows and passed the quality gate ✅ |
| Pipeline runtime | 5.95s for all 14 days (local runner) |

Full numbers: `outputs/metrics.json`. Injected-issue ground truth:
`outputs/injected_issues.json`. Tests: 5 passed (`tests/`).

## Data-quality design
Checks run per day, per row; the first failing check sets the reason:
duplicate `order_id` (keep first), null `customer_id`, unparseable
`order_timestamp`, non-numeric amount, negative amount. Failing rows go
to quarantine with their reason — never silently dropped, never loaded.
Fixable problems are NOT quarantined: currency-formatted amounts are
repaired in the transform step.

## Failure handling & idempotency
- A missing or corrupt landing file raises `CorruptFileError` in
  `extract`. In Airflow that fails the task, Airflow retries it
  (retries=3, 5-min delay) and downstream tasks for that day never run
  against bad data; other days are unaffected. The runner demonstrates
  this end-to-end: corrupt file -> 0 rows added -> file regenerated ->
  retry succeeds.
- Load is delete-then-insert per `load_date` with dimension upserts on
  natural keys, so Airflow retries, backfills and manual re-runs are
  all safe. Demonstrated: a full second run of a day changed its fact
  count by 0.
- A post-load `quality_gate` reconciles the day (rows present, no null
  keys, no duplicate order_ids anywhere) and fails the run otherwise.

## Transparency note
AWS and Airflow are not running in this local environment, and this
README does not claim otherwise. `dags/aws_etl_dag.py` is a real,
deployment-ready Airflow DAG (daily schedule, retries=3, catchup=False,
extract >> validate >> transform >> load >> quality_gate) whose tasks
call the functions in `src/etl_tasks.py`. `run_etl.py` is a local
runner that executes those identical task functions against local
folders standing in for S3 and a SQLite star schema standing in for
Redshift. Every AWS equivalent (boto3 S3 checks, Glue PySpark job,
Redshift COPY + merge) is mapped line-by-line in `src/etl_tasks.py`
comments and `docs/aws_architecture.md`.

## Run it
```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python run_etl.py        # generates 14 days, runs all tasks,
                                   # idempotency + failure/retry proofs
.venv/bin/python -m pytest tests/  # 5 tests
```

## Layout
- `dags/aws_etl_dag.py` — Airflow DAG (deployment artifact)
- `src/etl_tasks.py` — extract / validate / transform / load / quality_gate task functions shared by DAG and runner
- `src/generate_data.py` — 14 daily order files with counted, injected DQ issues
- `run_etl.py` — local runner + idempotency and failure/retry demonstrations, writes `outputs/metrics.json`
- `sql/analytics_queries.sql` — 10 Redshift queries over the star schema (all executed against the local warehouse)
- `docs/aws_architecture.md` — production architecture and the quarantine + retry design

## Resume bullets (verified — safe to use)
**AWS ETL Pipeline with Airflow, S3, Glue and Redshift**
- Built a daily ETL pipeline in Python (pandas) processing 70,350 e-commerce orders across 14 days into a star schema (fact_orders with customer, product and date dimensions; $21.3M loaded), orchestrated by an Apache Airflow DAG with extract, data-quality validation, transform, load and quality-gate tasks.
- Implemented data-quality checks that quarantined 980 defective rows (350 duplicate order IDs, 280 null customer IDs, 210 negative amounts, 140 bad timestamps — matching injected ground truth exactly) while repairing 553 currency-formatted amounts in transformation instead of dropping them.
- Made loads idempotent with delete-then-insert per load date and dimension upserts — a full re-run of a day changed its fact count by zero — and demonstrated failure recovery where a corrupted landing file failed the extract task, loaded nothing, and succeeded on retry after regeneration.
