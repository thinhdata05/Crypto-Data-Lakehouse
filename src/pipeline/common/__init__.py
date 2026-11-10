"""
Common utilities for Apache Iceberg Lakehouse.
"""
from src.pipeline.common.iceberg_session import get_catalog, get_spark_session

__all__ = ["get_catalog", "get_spark_session"]
