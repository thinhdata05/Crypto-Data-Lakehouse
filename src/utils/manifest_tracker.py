"""
Source Ingestion Manifest & Checkpointing Tracker.

Tracks lineage and processing checkpoints for raw source files:
- Persists to `lakehouse.bronze.source_manifest`.
- Computes SHA-256 hashes to guarantee data integrity.
- Enables idempotent replays and skipping of already-processed source batches.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
import sys

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo


def compute_file_sha256(filepath: str, block_size: int = 65536) -> str:
    """Computes SHA-256 hash of a file on disk."""
    hasher = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for block in iter(lambda: f.read(block_size), b""):
                hasher.update(block)
        return hasher.hexdigest()
    except Exception as e:
        return f"hash_err_{e}"


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


def record_manifest_entry(
    catalog,
    source_file: str,
    exchange: str,
    raw_symbol: str,
    batch_id: str,
    file_hash: str,
    record_count: int,
    min_timestamp: datetime | None,
    max_timestamp: datetime | None,
    status: str = "INGESTED_BRONZE",
) -> None:
    """Records an ingestion/processing checkpoint in bronze.source_manifest."""
    try:
        manifest_tbl = catalog.load_table("bronze.source_manifest")
        target_schema = manifest_tbl.schema().as_arrow()
        now_utc = datetime.now(timezone.utc)

        # Ensure datetime timezone awareness
        if min_timestamp and min_timestamp.tzinfo is None:
            min_timestamp = min_timestamp.replace(tzinfo=timezone.utc)
        if max_timestamp and max_timestamp.tzinfo is None:
            max_timestamp = max_timestamp.replace(tzinfo=timezone.utc)

        record = pa.Table.from_pydict(
            {
                "source_file": [source_file],
                "exchange": [exchange],
                "raw_symbol": [raw_symbol],
                "batch_id": [batch_id],
                "file_hash": [file_hash],
                "record_count": [record_count],
                "min_timestamp": [min_timestamp or now_utc],
                "max_timestamp": [max_timestamp or now_utc],
                "status": [status],
                "processed_at": [now_utc],
            },
            schema=target_schema,
        )
        manifest_tbl.append(record)
    except Exception as e:
        print(f"[WARN] Failed to write manifest entry for {source_file}: {e}", file=sys.stderr)


def is_file_already_ingested(catalog, source_file: str, file_hash: str) -> bool:
    """Checks if a source file with matching hash has already been recorded in the manifest."""
    try:
        manifest_tbl = catalog.load_table("bronze.source_manifest")
        if not manifest_tbl.current_snapshot():
            return False
        scan = manifest_tbl.scan(
            row_filter=And(EqualTo("source_file", source_file), EqualTo("file_hash", file_hash)),
            selected_fields=("source_file",),
        )
        return len(scan.to_arrow()) > 0
    except Exception:
        return False
