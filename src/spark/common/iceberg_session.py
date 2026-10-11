"""
SparkSession factory configured with Apache Iceberg REST Catalog and MinIO S3FileIO.
"""

from __future__ import annotations

import os
from typing import Optional
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "LakehouseCryptoSparkJob",
    master: Optional[str] = None,
) -> SparkSession:
    """
    Creates or retrieves an active SparkSession configured for Iceberg REST Catalog.
    """
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
    catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "lakehouse")
    warehouse_path = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/")

    builder = (
        SparkSession.builder.appName(app_name)
        # Iceberg SQL Extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Configure Iceberg REST Catalog
        .config(
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{catalog_name}.uri", rest_uri)
        .config(
            f"spark.sql.catalog.{catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", s3_endpoint)
        .config(
            f"spark.sql.catalog.{catalog_name}.s3.path-style-access",
            "true",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.s3.access-key-id",
            s3_access_key,
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.s3.secret-access-key",
            s3_secret_key,
        )
        .config("spark.sql.defaultCatalog", catalog_name)
        # S3A FileSystem configurations for legacy/direct Parquet reads
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Timezone & optimization
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "2500m")
        .config("spark.executor.memory", "2500m")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.iceberg.write.distribution-mode", "range")
        .config("spark.sql.iceberg.write.fanout.enabled", "false")
    )


    if master:
        builder = builder.master(master)

    return builder.getOrCreate()
