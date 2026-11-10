"""
Local Curated Silver -> Confirmed Parquet & _manifest.json Publisher.

Implements the Local Producer boundary of Phase 2 Hybrid Architecture (Option 1):
- Queries newly reconciled rows from Local Curated Silver (`lakehouse.silver.curated_crypto_bars`)
  within the bounded `[window_start, window_end)` interval.
- Exports immutable per-exchange Parquet files (`alpaca_bars.parquet`, `binance_bars.parquet`)
  with `priority = 2` preserved.
- Computes per-file SHA-256 checksums, record counts, and min/max timestamps.
- Writes `_manifest.json` LAST as the atomic commit marker for Cloud Silver Single-Writer convergence.
- Enforces idempotency: re-running for the same `batch_id` verifies existing SHA-256 checksums and skips.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional
import urllib.parse
import uuid

import duckdb
import pyarrow as pa
from pyarrow.fs import FileType, S3FileSystem
import pyarrow.parquet as pq

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan, Or
except ImportError:  # pragma: no cover - optional on host without pyiceberg
    load_catalog = None
    And = EqualTo = GreaterThanOrEqual = LessThan = Or = None


def _to_arrow_table(duck_rel) -> pa.Table:
    if hasattr(duck_rel, "to_arrow_table"):
        return duck_rel.to_arrow_table()
    return duck_rel.fetch_arrow_table()


def compute_file_sha256(file_path: str) -> str:
    """Computes hex SHA-256 digest of a file in 1MB chunks."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def write_json_atomic(target_path: str, data: Dict[str, Any]) -> None:
    """Writes JSON payload to a temporary file and atomically renames it to target_path."""
    tmp_path = f"{target_path}.tmp_{uuid.uuid4().hex[:8]}"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, target_path)


def get_s3_filesystem(s3_uri: Optional[str] = None) -> S3FileSystem:
    """
    Constructs an authenticated PyArrow S3FileSystem using standard environment configurations.
    Reads credentials from CLOUD_AWS_* or AWS_* variables without hardcoded secrets.
    """
    access_key = os.getenv("CLOUD_AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("CLOUD_AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("CLOUD_AWS_SESSION_TOKEN") or os.getenv("AWS_SESSION_TOKEN")
    region = os.getenv("CLOUD_AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    endpoint = (
        os.getenv("CLOUD_AWS_ENDPOINT_URL")
        or os.getenv("AWS_ENDPOINT_URL")
        or os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    )

    scheme = "http" if endpoint and endpoint.startswith("http://") else "https"
    clean_endpoint = None
    force_virtual_addressing = True
    if endpoint:
        clean_endpoint = endpoint.replace("http://", "").replace("https://", "").rstrip("/")
        if any(h in endpoint for h in ("minio", "localhost", "127.0.0.1", ":9000")):
            force_virtual_addressing = False

    return S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
        region=region,
        endpoint_override=clean_endpoint,
        scheme=scheme,
        force_virtual_addressing=force_virtual_addressing,
    )


def upload_batch_to_s3(
    batch_dir: str,
    manifest_data: Dict[str, Any],
    s3_base_uri: str,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Uploads confirmed Parquet files to Cloud S3 first, verifies byte checksums,
    and publishes _manifest.json as the final commit marker.
    """
    parsed = urllib.parse.urlsplit(s3_base_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/").rstrip("/")

    s3_fs = get_s3_filesystem(s3_base_uri)
    batch_id = manifest_data["batch_id"]

    # Step 1: Upload and verify all Parquet data files
    updated_files: List[Dict[str, Any]] = []
    for f_entry in manifest_data.get("files", []):
        filename = f_entry["filename"]
        local_path = os.path.join(batch_dir, filename)
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local Parquet file missing before S3 upload: {local_path}")

        file_size = os.path.getsize(local_path)
        expected_sha256 = f_entry.get("sha256") or compute_file_sha256(local_path)
        s3_key_path = f"{bucket}/{prefix}/batch_id={batch_id}/{filename}" if prefix else f"{bucket}/batch_id={batch_id}/{filename}"
        s3_full_uri = f"s3://{bucket}/{prefix}/batch_id={batch_id}/{filename}" if prefix else f"s3://{bucket}/batch_id={batch_id}/{filename}"

        # Upload with retry loop
        uploaded = False
        last_err: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                with open(local_path, "rb") as f_in:
                    with s3_fs.open_output_stream(s3_key_path) as f_out:
                        while True:
                            chunk = f_in.read(1024 * 1024)
                            if not chunk:
                                break
                            f_out.write(chunk)

                # Verify uploaded file size
                info = s3_fs.get_file_info(s3_key_path)
                if info.type != FileType.File or info.size != file_size:
                    raise IOError(
                        f"Size mismatch on S3 for {s3_key_path}: expected {file_size} bytes, got {info.size}"
                    )

                # Stream remote object back from S3 to verify SHA-256 byte checksum
                remote_h = hashlib.sha256()
                with s3_fs.open_input_stream(s3_key_path) as s3_in:
                    while True:
                        chunk = s3_in.read(1024 * 1024)
                        if not chunk:
                            break
                        remote_h.update(chunk)
                remote_sha256 = remote_h.hexdigest()
                if remote_sha256 != expected_sha256:
                    raise IOError(
                        f"Checksum mismatch on S3 for {s3_key_path}: local={expected_sha256} vs remote={remote_sha256}"
                    )

                uploaded = True
                break
            except Exception as e:
                last_err = e
                time.sleep(1.0 * (2 ** (attempt - 1)))

        if not uploaded:
            raise RuntimeError(
                f"Failed to upload {filename} to S3 {s3_full_uri} after {max_retries} attempts: {last_err}"
            )

        f_copy = dict(f_entry)
        f_copy["uri"] = s3_full_uri
        f_copy["sha256"] = expected_sha256
        updated_files.append(f_copy)

    # Step 2: Upload _manifest.json LAST as the atomic commit marker
    manifest_data["files"] = updated_files
    manifest_data["s3_confirmed_uri"] = (
        f"s3://{bucket}/{prefix}/batch_id={batch_id}/_manifest.json"
        if prefix
        else f"s3://{bucket}/batch_id={batch_id}/_manifest.json"
    )

    s3_manifest_key = f"{bucket}/{prefix}/batch_id={batch_id}/_manifest.json" if prefix else f"{bucket}/batch_id={batch_id}/_manifest.json"
    manifest_bytes = json.dumps(manifest_data, indent=2).encode("utf-8")

    manifest_uploaded = False
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            with s3_fs.open_output_stream(s3_manifest_key) as f_out:
                f_out.write(manifest_bytes)

            m_info = s3_fs.get_file_info(s3_manifest_key)
            if m_info.type != FileType.File or m_info.size != len(manifest_bytes):
                raise IOError("S3 manifest file verification failed.")

            with s3_fs.open_input_stream(s3_manifest_key) as m_in:
                remote_manifest_bytes = m_in.read()
            if remote_manifest_bytes != manifest_bytes:
                raise IOError("Remote manifest byte content mismatch.")

            manifest_uploaded = True
            break
        except Exception as e:
            last_err = e
            time.sleep(1.0 * (2 ** (attempt - 1)))

    if not manifest_uploaded:
        raise RuntimeError(
            f"Failed to commit _manifest.json to S3 after {max_retries} attempts: {last_err}"
        )

    # Only commit updated S3 metadata to local manifest AFTER remote manifest successfully verified
    local_manifest_path = os.path.join(batch_dir, "_manifest.json")
    write_json_atomic(local_manifest_path, manifest_data)

    print(f"[S3 PUBLISHED] Confirmed batch '{batch_id}' successfully uploaded and committed to {manifest_data['s3_confirmed_uri']}")
    return manifest_data


def default_confirmed_dir() -> str:
    """Resolves default local/mounted directory for confirmed Parquet slices."""
    env_dir = os.getenv("CONFIRMED_PARQUET_DIR")
    if env_dir:
        return env_dir
    if os.path.exists("/home/iceberg/staging_local"):
        return "/home/iceberg/staging_local/confirmed"
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../staging/confirmed")
    )


def get_catalog():
    if load_catalog is None:
        raise RuntimeError("pyiceberg is required to query Iceberg catalog.")
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


def publish_confirmed_slice(
    arrow_table: pa.Table,
    batch_id: str,
    window_start: str,
    window_end: str,
    output_base_dir: Optional[str] = None,
    s3_base_uri: Optional[str] = None,
    upload_to_s3: bool = False,
) -> Dict[str, Any]:
    """
    Writes immutable per-exchange Parquet files and atomic `_manifest.json` for a reconciled window slice.
    """
    base_dir = output_base_dir or default_confirmed_dir()
    batch_dir = os.path.join(base_dir, f"batch_id={batch_id}")
    os.makedirs(batch_dir, exist_ok=True)
    manifest_path = os.path.join(batch_dir, "_manifest.json")

    should_upload_s3 = upload_to_s3 or (
        s3_base_uri is not None
        and s3_base_uri.startswith("s3://")
        and os.getenv("CLOUD_UPLOAD_CONFIRMED", "").lower() in ("1", "true", "yes")
    )

    # Idempotency and crash-recovery check:
    # If local _manifest.json and Parquets are valid:
    # - If local handoff only (should_upload_s3=False): skip rewrite.
    # - If Cloud upload requested (should_upload_s3=True): verify remote S3 status before skipping.
    #   If remote S3 is missing or incomplete, resume/retry S3 upload without premature local skip!
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as mf:
                existing_manifest = json.load(mf)
            local_all_valid = True
            for f_entry in existing_manifest.get("files", []):
                p_file = os.path.join(batch_dir, f_entry["filename"])
                if not os.path.exists(p_file) or compute_file_sha256(p_file) != f_entry.get("sha256"):
                    local_all_valid = False
                    break

            if local_all_valid:
                if not should_upload_s3:
                    print(
                        f"[IDEMPOTENT SKIP] Confirmed batch '{batch_id}' already published "
                        f"with verified SHA-256 checksums at {batch_dir}."
                    )
                    return existing_manifest

                # Verify remote S3 batch status before skipping
                parsed = urllib.parse.urlsplit(s3_base_uri)
                bucket = parsed.netloc
                prefix = parsed.path.lstrip("/").rstrip("/")
                s3_fs = get_s3_filesystem(s3_base_uri)
                s3_manifest_key = f"{bucket}/{prefix}/batch_id={batch_id}/_manifest.json" if prefix else f"{bucket}/batch_id={batch_id}/_manifest.json"

                m_info = s3_fs.get_file_info(s3_manifest_key)
                if m_info.type == FileType.File and m_info.size > 0:
                    try:
                        # Read and verify remote manifest metadata
                        with s3_fs.open_input_stream(s3_manifest_key) as m_in:
                            remote_manifest = json.loads(m_in.read().decode("utf-8"))

                        remote_all_valid = (
                            remote_manifest.get("batch_id") == batch_id
                            and remote_manifest.get("total_records") == existing_manifest.get("total_records")
                            and len(remote_manifest.get("files", [])) == len(existing_manifest.get("files", []))
                        )

                        if remote_all_valid:
                            # Map remote manifest files by filename for SHA-256 metadata check
                            remote_file_map = {f["filename"]: f for f in remote_manifest.get("files", [])}

                            # Cryptographically verify each remote file: size, metadata SHA-256, and remote object byte hash
                            for f_entry in existing_manifest.get("files", []):
                                fn = f_entry["filename"]
                                local_p = os.path.join(batch_dir, fn)
                                k_path = f"{bucket}/{prefix}/batch_id={batch_id}/{fn}" if prefix else f"{bucket}/batch_id={batch_id}/{fn}"
                                k_info = s3_fs.get_file_info(k_path)

                                # 1. Exact byte size check
                                if k_info.type != FileType.File or k_info.size != os.path.getsize(local_p):
                                    remote_all_valid = False
                                    break

                                # 2. Manifest metadata SHA-256 check
                                r_entry = remote_file_map.get(fn)
                                if not r_entry or r_entry.get("sha256") != f_entry.get("sha256"):
                                    remote_all_valid = False
                                    break

                                # 3. Direct remote object byte SHA-256 stream verification
                                r_h = hashlib.sha256()
                                with s3_fs.open_input_stream(k_path) as s3_in:
                                    while True:
                                        chunk = s3_in.read(1024 * 1024)
                                        if not chunk:
                                            break
                                        r_h.update(chunk)
                                if r_h.hexdigest() != f_entry.get("sha256"):
                                    remote_all_valid = False
                                    break

                        if remote_all_valid:
                            print(
                                f"[IDEMPOTENT SKIP] Confirmed batch '{batch_id}' already published "
                                f"and cryptographically verified on both Local and Cloud S3 at {s3_base_uri} "
                                f"(sizes, manifest metadata, and remote SHA-256 match)."
                            )
                            return remote_manifest
                    except Exception:
                        remote_all_valid = False

                # Local valid but S3 missing/incomplete: resume upload!
                print(
                    f"[RETRY RESUME] Confirmed batch '{batch_id}' valid locally but missing/incomplete on S3. "
                    f"Resuming upload to {s3_base_uri}..."
                )
                return upload_batch_to_s3(
                    batch_dir=batch_dir,
                    manifest_data=existing_manifest,
                    s3_base_uri=s3_base_uri,
                )
        except Exception:
            pass

    if arrow_table is None or len(arrow_table) == 0:
        empty_manifest = {
            "batch_id": batch_id,
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "producer": "local_curated_silver_publisher",
            "window_start": window_start,
            "window_end": window_end,
            "total_records": 0,
            "files": [],
        }
        write_json_atomic(manifest_path, empty_manifest)
        return empty_manifest

    duck_con = duckdb.connect()
    duck_con.register("slice_arr", arrow_table)
    exchanges = [
        r[0]
        for r in duck_con.execute(
            "SELECT DISTINCT exchange FROM slice_arr ORDER BY exchange"
        ).fetchall()
    ]

    files_meta: List[Dict[str, Any]] = []
    total_records = 0

    for exc in exchanges:
        rel = duck_con.execute(
            """
            SELECT
                exchange,
                raw_symbol,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                trade_count,
                source,
                CAST(COALESCE(priority, 2) AS INTEGER) AS priority,
                base_currency,
                quote_currency,
                canonical_symbol,
                batch_id,
                ingested_at
            FROM slice_arr
            WHERE exchange = ?
            ORDER BY raw_symbol, timestamp
            """,
            [exc],
        )
        exc_arr = _to_arrow_table(rel)
        rec_count = len(exc_arr)
        if rec_count == 0:
            continue

        filename = f"{exc}_bars.parquet"
        local_file_path = os.path.join(batch_dir, filename)
        pq.write_table(exc_arr, local_file_path, compression="snappy")

        file_sha256 = compute_file_sha256(local_file_path)
        stats = duck_con.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM slice_arr WHERE exchange = ?",
            [exc],
        ).fetchone()
        min_ts = stats[0].isoformat() if hasattr(stats[0], "isoformat") else str(stats[0])
        max_ts = stats[1].isoformat() if hasattr(stats[1], "isoformat") else str(stats[1])

        # Local output path is authoritative local handoff path
        files_meta.append(
            {
                "filename": filename,
                "uri": local_file_path,
                "exchange": exc,
                "sha256": file_sha256,
                "record_count": rec_count,
                "min_timestamp": min_ts,
                "max_timestamp": max_ts,
                "priority": 2,
            }
        )
        total_records += rec_count

    manifest_data: Dict[str, Any] = {
        "batch_id": batch_id,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "producer": "local_curated_silver_publisher",
        "window_start": window_start,
        "window_end": window_end,
        "total_records": total_records,
        "files": files_meta,
    }

    # Write local _manifest.json atomically (temp file + os.replace)
    write_json_atomic(manifest_path, manifest_data)

    # Perform real S3 upload and commit if Cloud handoff is enabled
    if should_upload_s3 and s3_base_uri:
        manifest_data = upload_batch_to_s3(
            batch_dir=batch_dir,
            manifest_data=manifest_data,
            s3_base_uri=s3_base_uri,
        )

    print(
        f"[{datetime.now(timezone.utc).isoformat()}] Published {total_records:,} confirmed Curated Silver rows "
        f"across {len(files_meta)} Parquet file(s) + _manifest.json to {batch_dir}"
    )
    return manifest_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish Confirmed Parquet + _manifest.json from Local Curated Silver"
    )
    parser.add_argument("--window-start", required=True, help="ISO start timestamp")
    parser.add_argument("--window-end", required=True, help="ISO end timestamp")
    parser.add_argument("--batch-id", default=None, help="Batch ID")
    parser.add_argument(
        "--source-table",
        default="silver.bars",
        help="Source authoritative Silver table (default: silver.bars)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Base output directory for confirmed batches",
    )
    parser.add_argument(
        "--s3-uri",
        default=os.getenv("CLOUD_CONFIRMED_S3_URI"),
        help="Optional Cloud S3 prefix URI for manifest metadata",
    )
    parser.add_argument(
        "--upload-to-s3",
        action="store_true",
        default=os.getenv("CLOUD_UPLOAD_CONFIRMED", "").lower() in ("1", "true", "yes"),
        help="Upload confirmed batch and commit manifest directly to Cloud S3 bucket",
    )
    return parser.parse_args()


def _normalize_pyiceberg_ts(ts_str: str, include_tz: bool) -> str:
    s = ts_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    suffix = "+00:00" if include_tz else ""
    return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}{suffix}"


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id or str(uuid.uuid4())
    catalog = get_catalog()
    tbl = catalog.load_table(args.source_table)

    has_tz = "tz" in str(tbl.schema().find_field("timestamp").field_type).lower()
    w_start_norm = _normalize_pyiceberg_ts(args.window_start, include_tz=has_tz)
    w_end_norm = _normalize_pyiceberg_ts(args.window_end, include_tz=has_tz)

    window_expr = And(
        GreaterThanOrEqual("timestamp", w_start_norm),
        LessThan("timestamp", w_end_norm),
    )
    if args.batch_id:
        row_filter = Or(EqualTo("batch_id", args.batch_id), window_expr)
    else:
        row_filter = window_expr
    arr = tbl.scan(row_filter=row_filter).to_arrow()
    publish_confirmed_slice(
        arrow_table=arr,
        batch_id=batch_id,
        window_start=args.window_start,
        window_end=args.window_end,
        output_base_dir=args.output_dir,
        s3_base_uri=args.s3_uri,
        upload_to_s3=args.upload_to_s3,
    )


if __name__ == "__main__":
    main()
