"""
Source Ingestion Manifest & Checkpointing Tracker.

Tracks lineage and processing checkpoints for raw source files:
- Persists to `lakehouse.ops.ingestion_manifest` (Control Plane namespace).
- Computes SHA-256 hashes to guarantee data integrity.
- Enables idempotent replays and skipping of already-processed source batches.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
import sys
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

MANIFEST_TABLE_NAME = "ops.ingestion_manifest"


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


def load_ingested_file_hashes(catalog) -> set[str]:
    """Loads all recorded file_hash fingerprints from ops.ingestion_manifest in a single scan."""
    try:
        manifest_tbl = catalog.load_table(MANIFEST_TABLE_NAME)
        if not manifest_tbl.current_snapshot():
            return set()
        scan = manifest_tbl.scan(selected_fields=("file_hash",))
        arr = scan.to_arrow()
        if len(arr) == 0:
            return set()
        return set(arr["file_hash"].to_pylist())
    except Exception:
        return set()


def record_manifest_batch(catalog, entries: list[dict[str, Any]]) -> None:
    """Records a batch of ingestion/processing checkpoints in ops.ingestion_manifest in 1 atomic commit."""
    if not entries:
        return
    try:
        manifest_tbl = catalog.load_table(MANIFEST_TABLE_NAME)
        target_schema = manifest_tbl.schema().as_arrow()
        now_utc = datetime.now(timezone.utc)

        normalized_entries = []
        for entry in entries:
            min_ts = entry.get("min_timestamp") or now_utc
            max_ts = entry.get("max_timestamp") or now_utc
            if hasattr(min_ts, "tzinfo") and min_ts.tzinfo is None:
                min_ts = min_ts.replace(tzinfo=timezone.utc)
            if hasattr(max_ts, "tzinfo") and max_ts.tzinfo is None:
                max_ts = max_ts.replace(tzinfo=timezone.utc)
            normalized_entries.append(
                {
                    "source_file": entry["source_file"],
                    "exchange": entry["exchange"],
                    "raw_symbol": entry["raw_symbol"],
                    "batch_id": entry["batch_id"],
                    "file_hash": entry["file_hash"],
                    "record_count": int(entry["record_count"]),
                    "min_timestamp": min_ts,
                    "max_timestamp": max_ts,
                    "status": entry.get("status", "INGESTED_BRONZE"),
                    "processed_at": entry.get("processed_at", now_utc),
                }
            )

        record_tbl = pa.Table.from_pylist(normalized_entries, schema=target_schema)
        manifest_tbl.append(record_tbl)
    except Exception as e:
        print(f"[WARN] Failed to write manifest batch ({len(entries)} entries): {e}", file=sys.stderr)


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
    """Records a single ingestion/processing checkpoint in ops.ingestion_manifest."""
    record_manifest_batch(
        catalog,
        [
            {
                "source_file": source_file,
                "exchange": exchange,
                "raw_symbol": raw_symbol,
                "batch_id": batch_id,
                "file_hash": file_hash,
                "record_count": record_count,
                "min_timestamp": min_timestamp,
                "max_timestamp": max_timestamp,
                "status": status,
                "processed_at": datetime.now(timezone.utc),
            }
        ],
    )


def is_file_already_ingested(catalog, source_file: str, file_hash: str) -> bool:
    """Checks if a source file with matching content hash has already been recorded in the manifest."""
    try:
        manifest_tbl = catalog.load_table(MANIFEST_TABLE_NAME)
        if not manifest_tbl.current_snapshot():
            return False
        scan = manifest_tbl.scan(
            row_filter=EqualTo("file_hash", file_hash),
            selected_fields=("file_hash", "source_file"),
        )
        return len(scan.to_arrow()) > 0
    except Exception:
        return False
