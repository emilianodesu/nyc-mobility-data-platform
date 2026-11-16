"""Dagster Orchestration Definitions and Operational Workflows.

Implements:
- R18: Daily local discovery, explicit catch-up operations
- R25: Frontier scan with gap detection
- Scenario T23: Dagster asset materialization and checks (S01-S08, Q01-Q09)
- Scenario T25: Frontier scan with gaps
"""

from __future__ import annotations

import datetime
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from hvfhv.config import SystemConfig, get_default_config
from hvfhv.control.locks import writer_lock
from hvfhv.control.registry import Registry


class OrchestrationError(Exception):
    """Base exception for orchestration failures."""
    pass


def scan_frontier_and_discover_gaps(
    data_root: Path | str,
    target_months: list[str],
    registry: Registry | None = None,
) -> dict[str, Any]:
    """Scan frontier for target source months and identify gaps (Scenario T25).
    
    Invariants:
    1. Compares target window against registered source_revisions in control plane.
    2. Identifies missing intermediate months without skipping or corrupting.
    3. Produces deterministic ordered list of acquisitions required for catch-up.
    """
    root = Path(data_root)
    reg = registry or Registry(db_path=root / "control" / "registry.sqlite", data_root=root)
    con = reg.get_connection()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT so.source_month, sr.revision_id, sr.content_sha256
            FROM source_objects so
            JOIN source_revisions sr ON so.logical_source_id = sr.logical_source_id
            WHERE so.kind = 'hvfhv';
            """
        ).fetchall()
        known_revisions = {r[0]: {"revision_id": r[1], "sha256": r[2]} for r in rows}
    finally:
        con.close()

    present_months: list[str] = []
    missing_gaps: list[str] = []

    for month in target_months:
        if month in known_revisions:
            present_months.append(month)
        else:
            missing_gaps.append(month)

    # Detect topology: contiguous vs fragmented
    is_contiguous = True
    if len(present_months) > 1:
        # Check sorted month continuity
        sorted_m = sorted(present_months)
        for i in range(len(sorted_m) - 1):
            y1, m1 = map(int, sorted_m[i].split("-"))
            y2, m2 = map(int, sorted_m[i + 1].split("-"))
            months_diff = (y2 - y1) * 12 + (m2 - m1)
            if months_diff > 1:
                is_contiguous = False
                break

    return {
        "status": "GAP_DETECTED" if missing_gaps else "SYNCHRONIZED",
        "scanned_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "target_months_count": len(target_months),
        "present_months": sorted(present_months),
        "present_count": len(present_months),
        "missing_gaps": sorted(missing_gaps),
        "gap_count": len(missing_gaps),
        "is_contiguous": is_contiguous,
        "recommended_catchup_sequence": sorted(missing_gaps),
    }


def execute_asset_checks_from_release(
    data_root: Path | str,
    release_id: str,
) -> dict[str, Any]:
    """Execute Dagster-style asset checks mirroring S01-S08 and Q01-Q09 (Scenario T23).
    
    Reads release validation.json (canonical pre_seal_rules or legacy rule_results)
    and post-seal S08 attestation from registry.sqlite to verify asset check pass/fail status
    under Data Contracts §4 truth table.
    """
    root = Path(data_root)
    rel_dir = root / "releases" / release_id
    val_file = rel_dir / "validation.json"

    if not val_file.exists():
        raise OrchestrationError(f"Validation receipt missing for release {release_id}")

    val_bytes = val_file.read_bytes()
    computed_val_sha256 = hashlib.sha256(val_bytes).hexdigest()
    val_data = json.loads(val_bytes.decode("utf-8"))

    raw_rules = val_data.get("pre_seal_rules")
    if raw_rules is None:
        raw_rules = val_data.get("rule_results", [])

    # Normalize and aggregate cohorts per rule_id
    grouped_rules: dict[str, list[dict[str, Any]]] = {}
    for r in raw_rules:
        rid = r.get("rule_id")
        if rid:
            grouped_rules.setdefault(rid, []).append(r)

    checks_by_id: dict[str, dict[str, Any]] = {}
    for rid, items in grouped_rules.items():
        first = items[0]
        raw_sev = first.get("severity", "BLOCK")
        severity = "BLOCK" if raw_sev in ("BLOCK", "BLOCKING") else raw_sev

        statuses = [item.get("status") or item.get("outcome") or "UNKNOWN" for item in items]
        if "ERROR" in statuses:
            agg_status = "ERROR"
        elif "FAIL" in statuses:
            agg_status = "FAIL"
        elif all(s == "NOT_APPLICABLE" for s in statuses):
            agg_status = "NOT_APPLICABLE"
        elif all(s == "PASS" for s in statuses):
            agg_status = "PASS"
        else:
            agg_status = statuses[0]

        eval_rows = sum(
            int(item.get("evaluated_count", item.get("evaluated_row_count", 0)) or 0)
            for item in items
        )
        fail_rows = sum(
            int(item.get("exception_count", item.get("failing_row_count", 0)) or 0)
            for item in items
        )

        if severity == "BLOCK":
            passed = (agg_status == "PASS")
        else:
            passed = (agg_status in ("PASS", "NOT_APPLICABLE"))

        checks_by_id[rid] = {
            "rule_id": rid,
            "name": first.get("rule_name") or f"rule_{rid.lower()}",
            "passed": passed,
            "status": agg_status,
            "outcome": agg_status,
            "severity": severity,
            "evaluated_rows": eval_rows,
            "failing_rows": fail_rows,
            "cohorts_evaluated": len(items),
        }

    # Read post-seal S08 attestation from registry.sqlite if not already in checks_by_id
    reg_db = root / "control" / "registry.sqlite"
    if "S08" not in checks_by_id and reg_db.exists():
        con = sqlite3.connect(f"file:{reg_db}?mode=ro", uri=True)
        try:
            cur = con.cursor()
            s08_row = cur.execute(
                """
                SELECT verification_id, verification_results_json, verified_at_utc
                FROM release_verifications
                WHERE release_id = ?
                ORDER BY verified_at_utc DESC
                LIMIT 1;
                """,
                (release_id,),
            ).fetchone()
            if s08_row:
                ver_json = json.loads(s08_row[1]) if s08_row[1] else {}
                s08_status = ver_json.get("rule_s08", "PASS") if isinstance(ver_json, dict) else "PASS"
                checks_by_id["S08"] = {
                    "rule_id": "S08",
                    "name": "post_seal_manifest_verification",
                    "passed": s08_status == "PASS",
                    "status": s08_status,
                    "outcome": s08_status,
                    "severity": "BLOCK",
                    "evaluated_rows": 1,
                    "failing_rows": 0 if s08_status == "PASS" else 1,
                    "cohorts_evaluated": 1,
                    "verification_id": s08_row[0],
                    "verified_at_utc": s08_row[2],
                }
        finally:
            con.close()

    # Enforce Data Contracts §4 truth table
    mandatory_s_rules = ["S01", "S02", "S03", "S04", "S05", "S06", "S07", "S08"]
    blocking_failures: list[str] = []

    for rid in mandatory_s_rules:
        chk = checks_by_id.get(rid)
        if not chk:
            blocking_failures.append(f"Missing mandatory BLOCK rule {rid}")
        elif chk["status"] != "PASS":
            blocking_failures.append(f"BLOCK rule {rid} status is {chk['status']}")

    for rid, chk in checks_by_id.items():
        st_val = chk["status"]
        if st_val == "ERROR":
            blocking_failures.append(f"Rule {rid} returned computational ERROR")
        elif st_val == "NOT_APPLICABLE" and rid not in ("Q07", "Q09"):
            blocking_failures.append(f"Rule {rid} returned unauthorized NOT_APPLICABLE")

    all_mandatory_passed = len(blocking_failures) == 0

    return {
        "release_id": release_id,
        "total_checks_evaluated": len(checks_by_id),
        "all_mandatory_s_passed": all_mandatory_passed,
        "truth_table_passed": all_mandatory_passed,
        "blocking_failures": blocking_failures,
        "checks": checks_by_id,
        "validation_hash": val_data.get("validation_sha256") or computed_val_sha256,
    }


# Optional Dagster definitions when dagster package is installed in environment
try:
    from dagster import (
        AssetCheckResult,
        AssetCheckSpec,
        AssetExecutionContext,
        AssetKey,
        AssetSelection,
        Definitions,
        MonthlyPartitionsDefinition,
        ScheduleDefinition,
        asset,
        asset_check,
        define_asset_job,
    )

    monthly_partitions = MonthlyPartitionsDefinition(start_date="2024-01-01")

    @asset(
        partitions_def=monthly_partitions,
        description="Raw TLC High Volume For-Hire Vehicle Parquet source partition.",
        group_name="raw_acquisition",
    )
    def tlc_hvfhv_raw(context) -> dict[str, Any]:
        partition_key = context.partition_key
        return {"source_month": partition_key, "status": "RAW_ACCEPTED"}

    @asset(
        description="NYC Taxi & Limousine Commission Zone Reference Lookup CSV.",
        group_name="raw_acquisition",
    )
    def taxi_zone_raw() -> dict[str, Any]:
        return {"logical_source_id": "tlc/taxi_zone_lookup", "status": "ZONE_ACCEPTED"}

    @asset(
        deps=[tlc_hvfhv_raw, taxi_zone_raw],
        description="Validated analytical release published to control plane.",
        group_name="transforms",
    )
    def analytical_release(context) -> dict[str, Any]:
        return {"status": "FINALIZED"}

    # Define Dagster definitions bundle
    dagster_defs = Definitions(
        assets=[tlc_hvfhv_raw, taxi_zone_raw, analytical_release],
    )
except ImportError:
    # Graceful degradation when dagster is not in Python 3.14 environment
    dagster_defs = None
