---
description: Docker container execution protocols, JVM thread protection, MinIO storage guardrails, VHDX disk management, and pipeline idempotency
globs: "docker/**, docker-compose.yml, airflow/**, config/**, src/**, sql/**"
---

# 03. Safety, DevOps & Execution Guardrails

Operational safety standards for Docker container execution, memory/disk protection, MinIO `s3://lakehouse/` storage governance, and idempotent pipeline replay.

---

## 1. Containerized Execution Environment
- All interactions with Apache Spark, Iceberg REST Catalog, Airflow, and MinIO run inside Docker containers on `lakehouse-net`:
  - **Spark SQL CLI (DDL execution)**:
    ```bash
    docker exec -w /home/iceberg lakehouse-spark-iceberg spark-sql -f /home/iceberg/sql/ddl/01_create_iceberg_tables.sql
    ```
  - **Zero-JVM PyIceberg + DuckDB Pipelines**:
    ```bash
    docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/pipeline/bronze/raw_to_bronze.py --batch-id <uuid>
    docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/pipeline/silver/bronze_to_silver.py --batch-id <uuid> --target-table silver.bars
    docker exec -w /home/iceberg lakehouse-spark-iceberg python3 src/pipeline/quality/data_validation.py --run-id <uuid> --target-table silver.bars
    ```
  - **Airflow CLI**:
    ```bash
    docker exec lakehouse-airflow-scheduler airflow dags test lakehouse_incremental_hourly 2026-11-08T07:00:00Z
    ```

---

## 2. Memory, Small-File & WSL2 VHDX Protection
- `lakehouse.bronze.crypto_bars` (`104,706,818` rows, `2,071` files) and `lakehouse.silver.bars` (`104,657,836` rows, `2,058` files) span `1,859+` daily partitions.
- **Mandatory Query Guardrail**:
  - Every ad-hoc query or cross-exchange join on `silver.bars` **MUST include a bounded `timestamp` partition filter**:
    ```sql
    WHERE timestamp >= TIMESTAMP '2026-08-01 00:00:00+00:00'
      AND timestamp < TIMESTAMP '2026-08-08 00:00:00+00:00'
    ```
  - Running an unfiltered full-table join across 104M+ rows in Spark will exhaust the JVM thread pool (`RejectedExecutionException`) or trigger OOM.
- **Python 3.10 PyIceberg Timestamp Rule**:
  - Inside `lakehouse-spark-iceberg` (Python 3.10), PyIceberg's `datetime.fromisoformat()` rejects trailing `"Z"` on `TimestamptzType` columns. Always normalize ISO timestamps to explicit offset `"+00:00"` (e.g. `2026-11-08T06:00:00+00:00`) via `_normalize_pyiceberg_ts()`.
- **WSL2 `docker_data.vhdx` Disk Hygiene**:
  - Never write per-symbol daily files in an outer symbol loop (which creates $68 \times 1,300 = 84,149$ small files per table and bloats `E:\DockerWSL\docker_data.vhdx`). Always batch across all 68 symbols by Quarter (`raw_to_bronze.py`) or Month (`bronze_to_silver.py`).
  - If large data deletions occur inside Docker volumes, run `fstrim -a -v` inside the Linux VM before running `wsl --shutdown` and `diskpart -> compact vdisk` on Windows.

---

## 3. Storage & Filesystem Semantics
- **Local `./staging` (`staging/canonical`, `staging/confirmed`)**:
  - Transient local staging buffer only — **NOT** the Data Lakehouse.
- **MinIO Object Storage (`s3://lakehouse/`)**:
  - The authoritative physical Data Lakehouse bucket storing `ops/*`, `bronze/*`, and `silver/*`.
  - Never delete `s3://lakehouse/` without explicit user confirmation.
  - Keep a backup of the REST Catalog SQLite DB at `staging/iceberg_catalog.db.bak` (`docker cp lakehouse-iceberg-rest:/tmp/iceberg_catalog.db staging/iceberg_catalog.db.bak`).

---

## 4. Pipeline Idempotency & Replayability Standard
- Every pipeline script and Airflow DAG must be 100% idempotent on repeated execution:
  1. **Bronze Per-File SHA-256 Gate**: `raw_to_bronze.py` checks `ops.ingestion_manifest` via `load_ingested_file_hashes()` and skips already-ingested Parquet files unless `--force-replay` is specified.
  2. **Silver Monthly Checkpoint & Scoped Overwrite**: `bronze_to_silver.py` checks each `YYYY-MM` window in `silver.bars` and skips already-populated months unless `--force-replay` (which performs a scoped `overwrite_filter` for that month).
  3. **Confirmed Parquet Publisher**: `publish_confirmed_parquet.py` verifies existing per-file SHA-256 hashes in `_manifest.json` before rewriting.

---

## 5. Security & Secret Isolation
- Never commit `.env` or hardcode credentials (`APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`, MinIO/Postgres passwords) into source files or Markdown documentation.
