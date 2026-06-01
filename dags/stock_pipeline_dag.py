"""
Stock Market Pipeline DAG

Schedule: hourly
Tasks (in order):
  1. load_s3_to_snowflake_raw  — COPY INTO new Parquet files from S3 → RAW schema
  2. dbt_run_transformations   — run all dbt models (staging → intermediate → marts)
  3. dbt_test_data_quality     — run dbt schema tests to validate the output data

The Kafka producer and consumer run as always-on Docker services and are NOT
managed by this DAG — they handle real-time streaming independently.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── Default arguments applied to every task in this DAG ────────────────────
default_args = {
    "owner":            "stock-pipeline",
    "depends_on_past":  False,
    # Start from today — Airflow will not try to backfill historical runs
    "start_date":       datetime(2026, 5, 31),
    "email_on_failure": False,
    "email_on_retry":   False,
    # Retry up to 3 times with a 5-minute gap between attempts
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}


# ── Task functions ───────────────────────────────────────────────────────────

def load_s3_to_snowflake(**context):
    """
    Import and call the Snowflake loader module.
    sys.path is extended so Airflow can find the snowflake/ package that lives
    in the /opt/airflow directory (mounted from the project root via docker-compose).
    """
    sys.path.insert(0, "/opt/airflow")
    from snowflake.load import copy_s3_to_raw  # noqa: E402

    rows_loaded = copy_s3_to_raw()
    print(f"Loaded {rows_loaded} rows into STOCK_DB.RAW.STOCK_PRICES")

    # Push the count to XCom so downstream tasks can reference it if needed
    context["ti"].xcom_push(key="rows_loaded", value=rows_loaded)
    return rows_loaded


# ── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    description="Hourly: load S3 → Snowflake RAW, then dbt transforms + tests",
    schedule_interval="@hourly",
    catchup=False,               # don't run for every past hour since start_date
    max_active_runs=1,           # prevent overlapping runs if one takes too long
    tags=["stock", "snowflake", "dbt"],
) as dag:

    # ── Task 1 ───────────────────────────────────────────────────────────────
    # Loads all NEW Parquet files from S3 into Snowflake RAW.
    # Snowflake's COPY INTO tracks previously loaded files so this is idempotent.
    load_task = PythonOperator(
        task_id="load_s3_to_snowflake_raw",
        python_callable=load_s3_to_snowflake,
    )

    # ── Task 2 ───────────────────────────────────────────────────────────────
    # Runs all dbt models: stg_stock_prices → int_daily_ohlcv → fct_stock_performance
    # --profiles-dir points at the profiles.yml inside the mounted dbt_stock folder
    # so dbt can read Snowflake credentials from environment variables at runtime.
    dbt_run_task = BashOperator(
        task_id="dbt_run_transformations",
        bash_command=(
            "cd /opt/airflow/dbt_stock && "
            "dbt run --profiles-dir /opt/airflow/dbt_stock --target prod"
        ),
    )

    # ── Task 3 ───────────────────────────────────────────────────────────────
    # Runs all dbt schema tests (not_null, accepted_values, etc.) defined in
    # the schema.yml files. If any test fails, Airflow marks this task as
    # failed and sends a retry — alerting you to data quality issues.
    dbt_test_task = BashOperator(
        task_id="dbt_test_data_quality",
        bash_command=(
            "cd /opt/airflow/dbt_stock && "
            "dbt test --profiles-dir /opt/airflow/dbt_stock --target prod"
        ),
    )

    # ── Task ordering ────────────────────────────────────────────────────────
    # >> is Airflow's dependency operator: A >> B means "run B after A succeeds"
    # Full chain: load S3 → run dbt models → test dbt output
    load_task >> dbt_run_task >> dbt_test_task
