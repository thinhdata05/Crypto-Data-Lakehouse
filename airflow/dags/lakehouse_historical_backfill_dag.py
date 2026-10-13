"""
Airflow DAG: Lakehouse Historical Backfill (Multi-Exchange).
Pipeline Flow:
1. Alpaca Source Preprocessing (src/preprocessing/alpaca.py)
2. Binance Source Preprocessing (src/preprocessing/binance.py)
3. Iceberg Tables DDL Creation (sql/ddl/01_create_iceberg_tables.sql)
4. Raw Staging -> Iceberg Bronze (src/spark/bronze/raw_to_bronze.py)
5. Bronze -> Silver per exchange (src/spark/silver/bronze_to_silver.py)
6. Silver -> Curated Silver (MERGE on exchange, raw_symbol, timestamp) (src/spark/curated_silver/silver_to_curated_silver.py)
7. Curated Silver -> Curated Gold (src/spark/gold/curated_silver_to_gold.py)
8. Gold Analytical Views Creation (sql/ddl/02_create_gold_views.sql)
9. Multi-Exchange Data Quality Validation (src/spark/quality/data_validation.py)

In accordance with PROJECT_LAKEHOUSE_MIGRATION_SPEC_FINAL.md.
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
    description="End-to-end multi-exchange historical backfill pipeline from local raw Parquet to Iceberg Curated Gold",
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
            /home/iceberg/src/spark/bronze/raw_to_bronze.py \
            --batch-id "$BATCH_ID"
        """,
    )

    # 5. Iceberg Bronze -> Silver (per exchange)
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='generate_batch_id') }}"
        echo "TASK: Iceberg Bronze -> Silver"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/silver/bronze_to_silver.py \
            --batch-id "$BATCH_ID"
        """,
    )

    # 6. Silver -> Curated Silver (Intra-exchange Priority Merge)
    silver_to_curated_silver = BashOperator(
        task_id="silver_to_curated_silver",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='generate_batch_id') }}"
        echo "TASK: Silver -> Curated Silver (Merge on exchange, raw_symbol, timestamp)"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/curated_silver/silver_to_curated_silver.py \
            --batch-id "$BATCH_ID"
        """,
    )

    # 7. Curated Silver -> Curated Gold
    curated_silver_to_gold = BashOperator(
        task_id="curated_silver_to_gold",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='generate_batch_id') }}"
        echo "TASK: Curated Silver -> Curated Gold"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/gold/curated_silver_to_gold.py \
            --batch-id "$BATCH_ID"
        """,
    )

    # 8. Materialize Gold Dimensional & Aggregate Tables
    materialize_gold_aggregates = BashOperator(
        task_id="materialize_gold_aggregates",
        bash_command="""
        set -e
        echo "TASK: Materialize Gold Aggregates and Dimension Tables"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/gold/materialize_gold_aggregates.py
        """,
    )

    # 9. Create / Refresh Gold Analytical Views
    create_gold_views = BashOperator(
        task_id="create_gold_views",
        bash_command="""
        set -e
        docker exec lakehouse-spark-iceberg spark-sql -f /home/iceberg/sql/ddl/02_create_gold_views.sql
        """,
    )

    # 10. Multi-Exchange Data Quality Validation
    run_data_validation = BashOperator(
        task_id="run_data_validation",
        bash_command="""
        set -e
        BATCH_ID="{{ ti.xcom_pull(task_ids='generate_batch_id') }}"
        echo "TASK: Multi-Exchange Data Quality Validation"
        docker exec lakehouse-spark-iceberg python3 \
            /home/iceberg/src/spark/quality/data_validation.py \
            --run-id "$BATCH_ID"
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
        >> silver_to_curated_silver
        >> curated_silver_to_gold
        >> materialize_gold_aggregates
        >> create_gold_views
        >> run_data_validation
        >> end_pipeline
    )
