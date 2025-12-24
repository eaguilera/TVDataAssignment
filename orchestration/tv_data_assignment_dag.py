
# ****************************************************
#
# Developed by Emerick Aguilera Gonzalez
# Project: Filtering and Enrichment of TV Programming
#
# ****************************************************
from datetime import datetime, timedelta 

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-eng-tv",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False, 
}

with DAG(
    dag_id="tv_data_assignment_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tv", "spark", "dbt", "analytics"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_and_validate_events = BashOperator(
        task_id="extract_and_validate_events",
        bash_command=(
            "spark-submit "
            "scripts/validate_and_enrich.py "
            "--data-dir data "
            "--output-dir output "
            "--write-human-readable"
        ),
    )

    detect_schedule_overlaps = BashOperator(
        task_id="detect_schedule_overlaps",
        bash_command=(
            "psql $TV_DB_CONN_STRING "
            "-f sql/detect_schedule_overlaps.sql"
        ),
    )

    dbt_build_dim_program_metadata = BashOperator(
        task_id="dbt_build_dim_program_metadata",
        bash_command=(
            "cd dbt && "
            "dbt run --models stg_program_metadata dim_program_metadata"
        ),
    )

    def run_data_quality_gate(**context):
        import json
        from pathlib import Path

        summary_path = Path("output/validation_summary.json")

        if not summary_path.exists():
            raise RuntimeError("validation_summary.json not found")

        with open(summary_path / "part-00000", "r", encoding="utf-8") as f:
            summary = json.loads(f.read())

        total = summary["total_events_processed"]
        invalid = summary["invalid_events"]

        if total == 0:
            raise RuntimeError("No events processed - failing DQ gate")

        invalid_ratio = invalid / total

        if invalid_ratio > 0.05:
            raise RuntimeError(
                f"Invalid ratio too high: {invalid_ratio:.2%} (> 5%)"
            )

    data_quality_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=run_data_quality_gate,
        provide_context=True,
    )

    pipeline_done = EmptyOperator(task_id="pipeline_done")

    alert_failure = BashOperator(
        task_id="alert_failure",
        bash_command='echo "TV data pipeline FAILURE – check Airflow logs"',
        trigger_rule="one_failed",
    )

    start >> extract_and_validate_events
    extract_and_validate_events >> detect_schedule_overlaps
    detect_schedule_overlaps >> dbt_build_dim_program_metadata
    dbt_build_dim_program_metadata >> data_quality_gate
    data_quality_gate >> pipeline_done

    [
        extract_and_validate_events,
        detect_schedule_overlaps,
        dbt_build_dim_program_metadata,
        data_quality_gate,
    ] >> alert_failure
