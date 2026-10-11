"""
Airflow DAG: Lakehouse Incremental Hourly Sync.
Runs periodically every hour to reconcile incremental windows, late arriving bars,
and exchange corrections into Curated Gold using priority resolution.
"""

from datetime import datetime, timedelta, timezone
import uuid

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "lakehouse-admin",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def build_hourly_window(**context) -> dict:
    interval_end = context.get("data_interval_end")
    if interval_end:
        end_dt = interval_end.in_timezone("UTC").start_of("hour")
    else:
        end_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(hours=1)
    window = {
        "start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "batch_id": str(uuid.uuid4()),
    }
    print(f"Hourly Window: {window['start']} -> {window['end']} (Batch: {window['batch_id']})")
    return window


with DAG(
    dag_id="lakehouse_incremental_hourly",
    default_args=default_args,
    schedule="0 * * * *",  # Hourly schedule
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "incremental", "hourly"],
    description="Hourly incremental synchronization and priority reconciliation into Curated Gold",
) as dag:

    start_sync = EmptyOperator(task_id="start_sync")

    get_window = PythonOperator(
        task_id="build_hourly_window",
        python_callable=build_hourly_window,
    )

    fetch_rest_bars = BashOperator(
        task_id="fetch_rest_bars",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='build_hourly_window')['batch_id'] }}"
        echo "TASK: Fetching REST API Bars (Batch: $BATCH_ID)..."
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/incremental/rest_fetcher.py \
            --batch-id "$BATCH_ID" \
            --limit 60
        """,
    )

    incremental_bronze = BashOperator(
        task_id="incremental_raw_to_bronze",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='build_hourly_window')['batch_id'] }}"
        echo "TASK: Appending Incremental to Bronze..."
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/bronze/raw_to_bronze.py \
            --batch-id "$BATCH_ID"
        """,
    )

    incremental_silver = BashOperator(
        task_id="incremental_bronze_to_silver",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='build_hourly_window')['batch_id'] }}"
        echo "TASK: Cleansing Incremental to Silver..."
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/silver/bronze_to_silver.py \
            --batch-id "$BATCH_ID"
        """,
    )

    incremental_sync = BashOperator(
        task_id="incremental_reconcile_merge_sync",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='build_hourly_window')['batch_id'] }}"
        W_START="{{ ti.xcom_pull(task_ids='build_hourly_window')['start'] }}"
        W_END="{{ ti.xcom_pull(task_ids='build_hourly_window')['end'] }}"
        echo "TASK: Reconciling & MERGE INTO Curated Silver and Gold (Window: $W_START to $W_END)..."
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/incremental/incremental_sync.py \
            --window-start "$W_START" \
            --window-end "$W_END" \
            --batch-id "$BATCH_ID"
        """,
    )

    materialize_gold = BashOperator(
        task_id="materialize_gold_aggregates",
        bash_command="""
        set -e
        echo "TASK: Refreshing & Materializing Gold Aggregates and Dimensions..."
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/gold/materialize_gold_aggregates.py
        """,
    )

    validate_incremental = BashOperator(
        task_id="validate_incremental",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='build_hourly_window')['batch_id'] }}"
        echo "TASK: Validating Data Quality & Integrity..."
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/quality/data_validation.py \
            --run-id "$BATCH_ID"
        """,
    )

    end_sync = EmptyOperator(task_id="end_sync")

    (
        start_sync
        >> get_window
        >> fetch_rest_bars
        >> incremental_bronze
        >> incremental_silver
        >> incremental_sync
        >> materialize_gold
        >> validate_incremental
        >> end_sync
    )


