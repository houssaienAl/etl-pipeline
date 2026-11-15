from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.alerts import notify_failure
from etl.extract_api import extract_fx_rates
from etl.generate_data import generate
from etl.quality import run_checks
from etl.transform import run as spark_transform_load

with DAG(
    dag_id="etl_pipeline",
    description="CSV + JSON + REST API -> PySpark -> PostgreSQL, with quality checks",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": notify_failure,
    },
    tags=["etl", "spark"],
) as dag:
    # Stand-in for the upstream systems dropping their CSV/JSON exports.
    land_files = PythonOperator(task_id="land_source_files", python_callable=generate)
    extract_api = PythonOperator(task_id="extract_fx_rates_api", python_callable=extract_fx_rates)
    transform_load = PythonOperator(task_id="spark_transform_load", python_callable=spark_transform_load)
    quality = PythonOperator(task_id="quality_checks", python_callable=run_checks, retries=0)

    [land_files, extract_api] >> transform_load >> quality
