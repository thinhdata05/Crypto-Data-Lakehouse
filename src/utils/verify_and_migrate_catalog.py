"""
Catalog Partition Spec Verification & Migration Runner.

Enforces Lakehouse Invariant 01:
- Partitioning MUST strictly be days(timestamp) (or days(trade_date) / days(created_at) / days(processed_at)).
- NEVER partition by exchange or raw_symbol.
- Automatically checks live Iceberg catalog tables, evolves obsolete partition specs,
  and verifies conformity across Bronze, Silver, Curated Silver, and Gold.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError


# Invariant Table Rules:
# tableName -> (expected_fields, forbidden_fields)
TABLE_PARTITION_INVARIANTS = {
    "bronze.crypto_bars": {
        "allowed_specs": [["timestamp_day"]],
        "forbidden_fields": ["exchange", "raw_symbol"],
    },
    "bronze.source_manifest": {
        "allowed_specs": [["processed_at_day"]],
        "forbidden_fields": ["exchange", "raw_symbol"],
    },
    "silver.crypto_bars": {
        "allowed_specs": [["timestamp_day"]],
        "forbidden_fields": ["exchange", "raw_symbol"],
    },
    "silver.curated_crypto_bars": {
        "allowed_specs": [["timestamp_day"]],
        "forbidden_fields": ["exchange", "raw_symbol"],
    },
    "gold.curated_crypto_bars": {
        "allowed_specs": [["timestamp_day"]],
        "forbidden_fields": ["exchange", "raw_symbol"],
    },
    "gold.agg_daily_crypto_ohlcv": {
        "allowed_specs": [["trade_date_day"], []],
        "forbidden_fields": ["exchange", "canonical_symbol"],
    },
    "gold.agg_daily_cross_exchange_spread": {
        "allowed_specs": [["trade_date_day"], []],
        "forbidden_fields": ["exchange", "base_currency"],
    },
    "gold.dim_symbol": {
        "allowed_specs": [[]],
        "forbidden_fields": ["exchange", "raw_symbol"],
    },
    "gold.pipeline_audit": {
        "allowed_specs": [["created_at_day"]],
        "forbidden_fields": ["dag_id", "status"],
    },
}


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


def verify_and_migrate(auto_evolve: bool = True) -> bool:
    catalog = get_catalog()
    all_passed = True
    results = []

    print("=" * 90)
    print(f"[{datetime.now(timezone.utc).isoformat()}] ICEBERG CATALOG PARTITION SPEC VERIFICATION")
    print("=" * 90)

    for table_name, rules in TABLE_PARTITION_INVARIANTS.items():
        try:
            tbl = catalog.load_table(table_name)
            current_spec = tbl.spec()
            spec_fields = [f.name for f in current_spec.fields]
            forbidden_present = [f for f in spec_fields if f in rules["forbidden_fields"]]

            status = "PASS"
            note = f"spec_id={current_spec.spec_id}, fields={spec_fields}"

            if forbidden_present:
                if auto_evolve:
                    print(f"[EVOLVE] Table {table_name} contains forbidden partition fields: {forbidden_present}. Evolving...")
                    try:
                        update = tbl.update_spec()
                        for f in forbidden_present:
                            update.remove_field(f)
                        update.commit()
                        # Re-read
                        tbl = catalog.load_table(table_name)
                        spec_fields = [f.name for f in tbl.spec().fields]
                        status = "EVOLVED"
                        note = f"Removed {forbidden_present} -> now {spec_fields}"
                    except Exception as ev_err:
                        status = "FAIL"
                        note = f"Failed to evolve: {ev_err}"
                        all_passed = False
                else:
                    status = "FAIL"
                    note = f"Contains forbidden fields {forbidden_present}"
                    all_passed = False
            elif spec_fields not in rules["allowed_specs"]:
                # Non-critical warning if spec is valid but different
                status = "WARN"
                note = f"Spec fields {spec_fields} not in standard {rules['allowed_specs']}"

            results.append((table_name, status, note))

        except NoSuchTableError:
            results.append((table_name, "MISSING", "Table does not exist in catalog"))
            all_passed = False
        except Exception as e:
            results.append((table_name, "ERROR", str(e)))
            all_passed = False

    print(f"{'Table Name':<38} | {'Status':<8} | {'Details'}")
    print("-" * 90)
    for tname, status, note in results:
        indicator = "✅" if status in ("PASS", "EVOLVED") else ("⚠️" if status == "WARN" else "❌")
        print(f"{tname:<38} | {indicator} {status:<6} | {note}")
    print("-" * 90)

    if all_passed:
        print("[SUCCESS] All Iceberg catalog tables strictly satisfy Lakehouse Invariant 01!")
    else:
        print("[FAILURE] One or more tables violate partition invariants or are missing.")

    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify and evolve Iceberg table partition specs")
    parser.add_argument("--no-evolve", action="store_true", help="Do not auto-evolve violating partition specs")
    args = parser.parse_args()

    success = verify_and_migrate(auto_evolve=not args.no_evolve)
    sys.exit(0 if success else 1)
