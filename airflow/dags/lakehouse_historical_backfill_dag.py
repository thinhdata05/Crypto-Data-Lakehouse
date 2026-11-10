"""
Airflow DAG: Lakehouse Historical Backfill (Multi-Exchange — Option 1 Local Producer).
Pipeline Flow:
1. Alpaca Source Preprocessing (src/preprocessing/alpaca.py)
2. Binance Source Preprocessing (src/preprocessing/binance.py)
3. Iceberg Local Tables DDL Creation (sql/ddl/01_create_iceberg_tables.sql)
4. Raw Staging -> Iceberg Bronze (src/pipeline/bronze/raw_to_bronze.py)
5. Bronze -> Silver (src/pipeline/silver/bronze_to_silver.py -> lakehouse.silver.bars)
6. Multi-Exchange Data Quality Validation on Silver (src/pipeline/quality/data_validation.py)

Note (Option 1): Local stops at authoritative Silver (`lakehouse.silver.bars`).
Gold tables and views are materialized exclusively on Cloud after convergence.
"""

from datetime import datetime, timedelta
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
    "retry_delay": timedelta(minutes=2),
}


def generate_batch_id(**context) -> str:
    batch_id = str(uuid.uuid4())
    print(f"Generated Historical Ingestion Batch ID: {batch_id}")
    return batch_id


with DAG(
    dag_id="lakehouse_historical_backfill",
    default_args=default_args,
    schedule=None,  # Triggered manually or via API
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "historical", "backfill", "alpaca", "binance"],
    description="End-to-end multi-exchange historical backfill pipeline from local raw Parquet to Iceberg Silver (lakehouse.silver.bars)",
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    gen_batch_id = PythonOperator(
        task_id="generate_batch_id",
        python_callable=generate_batch_id,
    )

    # 1. Alpaca Source Preprocessing (Python boundary)
    alpaca_preprocessing = BashOperator(
        task_id="alpaca_source_preprocessing",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='generate_batch_id') }}"
        echo "TASK: Alpaca Source Preprocessing (Batch: $BATCH_ID)"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/preprocessing/alpaca.py \
            --batch-id "$BATCH_ID"
        """,
    )

    # 2. Binance Source Preprocessing (Python boundary)
    binance_preprocessing = BashOperator(
        task_id="binance_source_preprocessing",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='generate_batch_id') }}"
        echo "TASK: Binance Source Preprocessing (Batch: $BATCH_ID)"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/preprocessing/binance.py \
            --batch-id "$BATCH_ID"
        """,
    )

    # 3. Ensure Iceberg Tables exist and strictly conform to partition invariants
    init_iceberg_tables = BashOperator(
        task_id="init_iceberg_tables",
        bash_command="""
        set -e
        echo "TASK: Initialize DDL and Verify Partition Invariants"
        docker exec lakehouse-spark-iceberg spark-sql -f /home/iceberg/sql/ddl/01_create_iceberg_tables.sql
        docker exec lakehouse-spark-iceberg python3 /home/iceberg/src/utils/verify_and_migrate_catalog.py
        """,
    )

    # 4. Raw Staging -> Iceberg Bronze
    raw_to_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='generate_batch_id') }}"
        echo "TASK: Raw Staging -> Iceberg Bronze"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/pipeline/bronze/raw_to_bronze.py \
            --batch-id "$BATCH_ID"
        """,
    )

    # 5. Iceberg Bronze -> Silver (lakehouse.silver.bars)
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='generate_batch_id') }}"
        echo "TASK: Iceberg Bronze -> Silver (lakehouse.silver.bars)"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/pipeline/silver/bronze_to_silver.py \
            --batch-id "$BATCH_ID" \
            --target-table silver.bars
        """,
    )

    # 6. Multi-Exchange Data Quality Validation on Silver (Option 1: Local stops here)
    run_data_validation = BashOperator(
        task_id="run_data_validation",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='generate_batch_id') }}"
        echo "TASK: Multi-Exchange Data Quality Validation on Silver (silver.bars)"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/pipeline/quality/data_validation.py \
            --run-id "$BATCH_ID" \
            --target-table silver.bars
        """,
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    (
        start_pipeline
        >> gen_batch_id
        >> [alpaca_preprocessing, binance_preprocessing]
        >> init_iceberg_tables
        >> raw_to_bronze
        >> bronze_to_silver
        >> run_data_validation
        >> end_pipeline
    )
