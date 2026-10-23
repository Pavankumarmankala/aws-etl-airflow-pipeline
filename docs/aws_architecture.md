# AWS Production Architecture

| Layer (local) | AWS service |
|---|---|
| Daily order-file export | Upstream system -> S3 landing bucket (optionally via Kinesis Firehose) |
| `data/raw/` | s3://bucket/landing/ (Glue crawler catalogues the files) |
| Airflow DAG (`dags/aws_etl_dag.py`) | Amazon MWAA or self-managed Airflow, `@daily`, retries=3 |
| extract | boto3 `head_object` + schema check before processing |
| validate | Glue Data Quality / shared Python checks; failures to s3://bucket/quarantine/ + SNS alert |
| transform | AWS Glue PySpark job (same repair/typing logic as `src/etl_tasks.py`) writing partitioned Parquet to s3://bucket/processed/ |
| `data/warehouse/` (SQLite) | Amazon Redshift: COPY into a staging table, then a transaction that DELETEs the load_date from fact_orders and INSERTs from staging — the same delete-then-insert idempotency implemented locally |
| quality_gate | Post-load Redshift SQL checks; DAG `on_failure_callback` -> SNS/Slack |
| Star schema | fact_orders (DISTKEY order date, SORTKEY load_date), dim_customer, dim_product, dim_date |

## Quarantine + retry design
- Bad rows are quarantined per row with a machine-readable reason and
  counted per issue type; nothing is silently dropped, and quarantined
  data never reaches Redshift.
- Fixable values (currency-formatted amounts) are repaired in the
  transform step rather than quarantined.
- Task-level failure (corrupt/missing landing file) fails fast at
  extract; Airflow retries handle transient or quickly-fixed upstream
  problems, and per-day `load_date` partitioning means one bad day
  never blocks or contaminates another.
- Loads are idempotent by construction (delete-then-insert per
  load_date + dimension upserts), so retries, backfills and manual
  re-runs cannot create duplicates — verified locally and by the
  quality gate's duplicate-order_id check on every run.
