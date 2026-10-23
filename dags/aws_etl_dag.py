"""Apache Airflow DAG: daily AWS ETL for e-commerce orders.

Deployment artifact — deploy this file to Airflow (self-managed or
Amazon MWAA) with src/etl_tasks.py on the Python path. The task
functions are storage-agnostic in structure; their AWS equivalents are
documented inline in etl_tasks.py and summarised here:

  extract      -> S3 landing check (boto3 head_object / Glue crawler)
  validate     -> DQ checks; bad rows to s3://bucket/quarantine/ + SNS
  transform    -> AWS Glue PySpark job (GlueJobOperator in a fuller
                  deployment; PythonOperator calling the shared logic
                  here keeps DAG and local runner on identical code)
  load         -> Redshift COPY into staging + delete/insert merge
                  (idempotent per load_date)
  quality_gate -> post-load SQL checks; on failure the DAG's
                  on_failure_callback alerts via SNS/Slack

Failure handling: retries=3 with a 5-minute retry_delay on every task.
A corrupt landing file fails `extract`, Airflow retries it, and if the
upstream export is fixed within the retry window the run recovers on
its own; downstream tasks for that day never run against bad data, and
other days' runs are unaffected.
"""
from datetime import datetime, timedelta
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make src/ importable when this DAG is deployed next to the project.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import etl_tasks  # noqa: E402  (no AWS calls happen at import time)

default_args = {
    "owner": "pavan",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    # In production: "on_failure_callback": send_sns_alert,
}

with DAG(
    dag_id="aws_etl_daily_orders",
    description="Daily S3 -> Glue-style transform -> Redshift star schema "
                "load for e-commerce orders, with DQ quarantine.",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "aws", "ecommerce"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=etl_tasks.extract,
        op_kwargs={"date": "{{ ds }}"},
    )
    validate = PythonOperator(
        task_id="validate",
        python_callable=etl_tasks.validate,
        op_kwargs={"date": "{{ ds }}"},
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=etl_tasks.transform,
        op_kwargs={"date": "{{ ds }}"},
    )
    load = PythonOperator(
        task_id="load_warehouse",
        python_callable=etl_tasks.load_warehouse,
        op_kwargs={"date": "{{ ds }}"},
    )
    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=etl_tasks.quality_gate,
        op_kwargs={"date": "{{ ds }}"},
    )

    extract >> validate >> transform >> load >> quality_gate
