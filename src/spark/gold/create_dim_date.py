"""
Kimball Conformed Date Dimension Generator (2020 - 2030).
Populates lakehouse.gold.dim_date with 4,018 calendar days,
including financial calendar fields, weekend flags, US holiday indicators,
and crypto-specific Bitcoin Halving Cycle epochs.
"""

import os
import sys
from datetime import date, timedelta
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DateType, IntegerType, NestedField, StringType


def get_catalog():
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
    s3_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

    return load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": rest_uri,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
        },
    )


def generate_date_dimension_arrow() -> pa.Table:
    """Generates 2020 to 2030 dates with rich Kimball attributes using DuckDB vectorization."""
    con = duckdb.connect()
    df = con.execute("""
        WITH date_series AS (
            SELECT (CAST('2020-01-01' AS DATE) + INTERVAL (d) DAY)::DATE AS dt
            FROM range(0, 4018) t(d)
        )
        SELECT
            CAST(strftime(dt, '%Y%m%d') AS INTEGER) AS date_key,
            dt AS calendar_date,
            CAST(EXTRACT(year FROM dt) AS INTEGER) AS year,
            CAST(EXTRACT(quarter FROM dt) AS INTEGER) AS quarter,
            CAST(EXTRACT(month FROM dt) AS INTEGER) AS month,
            strftime(dt, '%B') AS month_name,
            CAST(EXTRACT(week FROM dt) AS INTEGER) AS week_of_year,
            CAST(EXTRACT(day FROM dt) AS INTEGER) AS day_of_month,
            CAST(EXTRACT(dow FROM dt) AS INTEGER) AS day_of_week,
            strftime(dt, '%A') AS day_name,
            CASE WHEN EXTRACT(dow FROM dt) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
            CASE WHEN dt = (DATE_TRUNC('month', dt) + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE THEN TRUE ELSE FALSE END AS is_month_end,
            CASE WHEN dt = (DATE_TRUNC('quarter', dt) + INTERVAL 3 MONTH - INTERVAL 1 DAY)::DATE THEN TRUE ELSE FALSE END AS is_quarter_end,
            -- Major US Fixed Holidays (New Year, Juneteenth, Independence Day, Christmas)
            CASE 
                WHEN (EXTRACT(month FROM dt) = 1 AND EXTRACT(day FROM dt) = 1) THEN TRUE
                WHEN (EXTRACT(month FROM dt) = 6 AND EXTRACT(day FROM dt) = 19) THEN TRUE
                WHEN (EXTRACT(month FROM dt) = 7 AND EXTRACT(day FROM dt) = 4) THEN TRUE
                WHEN (EXTRACT(month FROM dt) = 12 AND EXTRACT(day FROM dt) = 25) THEN TRUE
                ELSE FALSE
            END AS is_us_holiday,
            -- Crypto Bitcoin Halving Epochs (Epoch 3: May 2020 - April 2024, Epoch 4: April 2024 - 2028)
            CASE
                WHEN dt < DATE '2020-05-11' THEN CAST(2 AS INTEGER)
                WHEN dt >= DATE '2020-05-11' AND dt < DATE '2024-04-19' THEN CAST(3 AS INTEGER)
                WHEN dt >= DATE '2024-04-19' AND dt < DATE '2028-04-17' THEN CAST(4 AS INTEGER)
                ELSE CAST(5 AS INTEGER)
            END AS crypto_halving_epoch,
            CASE
                WHEN dt < DATE '2020-05-11' THEN 'Epoch 2 (2016-2020)'
                WHEN dt >= DATE '2020-05-11' AND dt < DATE '2024-04-19' THEN 'Epoch 3 (2020-2024)'
                WHEN dt >= DATE '2024-04-19' AND dt < DATE '2028-04-17' THEN 'Epoch 4 (2024-2028)'
                ELSE 'Epoch 5 (2028-2032)'
            END AS halving_cycle_name
        FROM date_series
        ORDER BY dt
    """).arrow()
    return df


def main():
    print("Generating Kimball Conformed Date Dimension (lakehouse.gold.dim_date)...")
    arrow_tbl = generate_date_dimension_arrow()

    catalog = get_catalog()
    table_name = "gold.dim_date"

    # Define PyIceberg schema with optional fields
    schema = Schema(
        NestedField(1, "date_key", IntegerType(), required=False),
        NestedField(2, "calendar_date", DateType(), required=False),
        NestedField(3, "year", IntegerType(), required=False),
        NestedField(4, "quarter", IntegerType(), required=False),
        NestedField(5, "month", IntegerType(), required=False),
        NestedField(6, "month_name", StringType(), required=False),
        NestedField(7, "week_of_year", IntegerType(), required=False),
        NestedField(8, "day_of_month", IntegerType(), required=False),
        NestedField(9, "day_of_week", IntegerType(), required=False),
        NestedField(10, "day_name", StringType(), required=False),
        NestedField(11, "is_weekend", BooleanType(), required=False),
        NestedField(12, "is_month_end", BooleanType(), required=False),
        NestedField(13, "is_quarter_end", BooleanType(), required=False),
        NestedField(14, "is_us_holiday", BooleanType(), required=False),
        NestedField(15, "crypto_halving_epoch", IntegerType(), required=False),
        NestedField(16, "halving_cycle_name", StringType(), required=False),
    )

    try:
        catalog.drop_table(table_name)
        print(f"Dropped existing {table_name}")
    except Exception:
        pass

    dim_date_table = catalog.create_table(
        identifier=table_name,
        schema=schema,
    )
    print(f"Created Iceberg table {table_name}")

    # Cast arrow table to match Iceberg schema perfectly
    target_arrow_schema = dim_date_table.schema().as_arrow()
    arrow_tbl = arrow_tbl.cast(target_arrow_schema)

    dim_date_table.append(arrow_tbl)
    print(f"Successfully populated {len(arrow_tbl):,} rows into {table_name}!")


if __name__ == "__main__":
    main()
