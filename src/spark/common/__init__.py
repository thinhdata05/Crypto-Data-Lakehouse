"""
Spark common utilities for Apache Iceberg Lakehouse.
"""
from src.spark.common.iceberg_session import get_spark_session

__all__ = ["get_spark_session"]
