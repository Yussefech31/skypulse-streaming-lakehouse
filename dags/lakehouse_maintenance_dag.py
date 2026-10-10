"""Airflow DAG for orchestrating Lakehouse batch compaction and data quality assertions."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "youssef",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def task_compact_lakehouse():
    """Execute hourly compaction to solve small file problem."""
    from src.lakehouse.compactor import LakehouseCompactor

    compactor = LakehouseCompactor()
    stats = compactor.run_compaction()
    print(f"Compaction completed: {stats}")
    return stats.records_compacted


def task_run_data_quality():
    """Execute DuckDB data quality validation suite."""
    from src.quality.assertions import DataQualityChecker

    checker = DataQualityChecker()
    report = checker.run_checks()
    print(f"Data Quality Report: {report}")
    if not report.overall_passed:
        raise ValueError(f"Data Quality checks failed: {report.failed_rules} rules failed.")
    return report.overall_passed


with DAG(
    "lakehouse_maintenance_hourly",
    default_args=default_args,
    description="Hourly batch compaction and data quality verification for SkyPulse Lakehouse",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "streaming", "data-quality", "compaction"],
) as dag:
    compact_task = PythonOperator(
        task_id="compact_silver_partitions",
        python_callable=task_compact_lakehouse,
    )

    quality_task = PythonOperator(
        task_id="verify_data_quality_suite",
        python_callable=task_run_data_quality,
    )

    compact_task >> quality_task
