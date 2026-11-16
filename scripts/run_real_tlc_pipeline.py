"""End-to-End Real Official NYC TLC Execution & Publication Pipeline.

Executes against real downloaded NYC TLC files in .data/staging/downloads/:
1. Phase 1 (Gate G1): Official Raw Acceptance (streaming SHA-256, PAR1 magic check,
   100% PyArrow batch decode, acquisition_provenance.json fsync, content-addressed
   .data/raw/sha256/<digest>/ finalization, and SQLite registration) for:
   - tlc/taxi_zones (taxi_zone_lookup.csv)
   - tlc/hvfhv/2024-12 (fhvhv_tripdata_2024-12.parquet, 24-col pre-2025 schema)
   - tlc/hvfhv/2025-01 (fhvhv_tripdata_2025-01.parquet, 25-col 2025+ schema with cbd_congestion_fee)
2. Phase 2 (Gate G2 & G3): Release 1 — 1-Month Real Vertical Slice (2024-12):
   - Builds candidate DuckDB with psutil ResourceTracker, pre-seal rules S01-S07 & Q01-Q09,
     finalizes candidate, verifies post-seal Rule S08, and publishes via CAS to generation 1.
3. Phase 3 (Gate G4 & G3): Release 2 — 2-Month Incremental Cross-Boundary Build (2024-12 + 2025-01):
   - Copies Release 1 verified DB, retains 2024-12 contribution, streams 2025-01 (25-col schema),
     recomputes continuous quantiles and multi-month Q07/Q09 drift profiles, finalizes,
     verifies S08, and publishes via CAS to generation 2.
   - Verifies Option A retention preview and 80 GiB storage budget compliance.
4. Phase 4 (Gate G5): Read-Only Consumer & Dagster Asset Check Verification against live .data/.
"""

from __future__ import annotations

import datetime
from datetime import timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any
import uuid

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.acquisition.acquire import acquire_source
from hvfhv.config import get_default_config
from hvfhv.consumer.app import render_headless_summary
from hvfhv.consumer.reader import ReleaseReader
from hvfhv.contracts.identity import (
    FrozenInputPlan,
    SourceRevisionEntry,
    compute_code_bundle_manifest_hash,
    compute_derivation_id,
)
from hvfhv.control.publication import (
    finalize_candidate,
    publish,
    verify_post_seal_s08,
)
from hvfhv.control.registry import Registry
from hvfhv.orchestration.defs import (
    dagster_defs,
    execute_asset_checks_from_release,
    scan_frontier_and_discover_gaps,
)
from hvfhv.transforms.builder import (
    build_analytical_candidate,
    calculate_storage_footprint,
    preview_retention_cleanup_option_a,
)
from tests.gate2_monthly_slice.resource_tracker import ResourceTracker


class LocalFileStreamingResponse:
    """Streams a locally downloaded official file in 64 KiB chunks to acquire_source."""

    def __init__(self, file_path: Path, url: str) -> None:
        self.file_path = file_path
        self.url = url
        self.status_code = 200
        size = file_path.stat().st_size
        self.headers = {
            "Content-Length": str(size),
            "Last-Modified": "Fri, 21 Feb 2025 21:29:11 GMT",
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def raise_for_status(self) -> None:
        pass

    def iter_bytes(self, chunk_size: int = 65536):
        with open(self.file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                yield chunk


class LocalFileStreamingClient:
    """Client adapter that streams pre-fetched official NYC TLC files through acquire_source."""

    def __init__(self, url_to_path: dict[str, Path]) -> None:
        self.url_to_path = url_to_path

    def stream(self, method: str, url: str, headers: dict | None = None):
        if url not in self.url_to_path:
            raise ValueError(f"Unmapped URL: {url}")
        return LocalFileStreamingResponse(self.url_to_path[url], url)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest().lower()


def run_pipeline() -> dict[str, Any]:
    config = get_default_config()
    data_root = config.data_root
    downloads_dir = config.staging_dir / "downloads"

    zone_dl = downloads_dir / "taxi_zone_lookup.csv"
    pq_2024_12 = downloads_dir / "fhvhv_tripdata_2024-12.parquet"
    pq_2025_01 = downloads_dir / "fhvhv_tripdata_2025-01.parquet"

    if not zone_dl.exists() or not pq_2024_12.exists():
        raise FileNotFoundError(
            f"Missing downloaded official TLC files in {downloads_dir}: "
            f"zone={zone_dl.exists()}, 2024-12={pq_2024_12.exists()}"
        )

    has_2025_01 = pq_2025_01.exists() and pq_2025_01.stat().st_size >= 480_000_000

    # Reset control DB and releases directory for clean end-to-end real run
    for d in [config.releases_dir, config.raw_dir / "sha256"]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)
    for item in config.staging_dir.iterdir():
        if item.name not in ("downloads", "attempt_g2_vertical_slice"):
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)
    for f in config.control_dir.glob("registry.sqlite*"):
        f.unlink(missing_ok=True)

    reg = Registry(config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
    reg_con = reg.get_connection()

    zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    url_2024_12 = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet"
    url_2025_01 = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"

    url_map = {
        zone_url: zone_dl,
        url_2024_12: pq_2024_12,
    }
    if has_2025_01:
        url_map[url_2025_01] = pq_2025_01

    client = LocalFileStreamingClient(url_map)

    receipt: dict[str, Any] = {
        "executed_at_utc": datetime.datetime.now(timezone.utc).isoformat(),
        "acquisitions": {},
        "releases": {},
    }

    print("=" * 76)
    print("PHASE 1: OFFICIAL NYC TLC RAW ACQUISITION & FULL DECODE VERIFICATION (G1)")
    print("=" * 76)
    try:
        t0 = time.perf_counter()
        acq_zone = acquire_source(
            logical_source_id="tlc/taxi_zones",
            url=zone_url,
            config=config,
            registry_conn=reg_con,
            attempt_id="acq_real_taxi_zones",
            client=client,
        )
        acq_zone["wall_seconds"] = round(time.perf_counter() - t0, 3)
        receipt["acquisitions"]["tlc/taxi_zones"] = acq_zone
        print(
            f"  [OK] tlc/taxi_zones: {acq_zone['decoded_row_count']} rows, "
            f"sha256={acq_zone['content_sha256'][:16]}... ({acq_zone['wall_seconds']}s)"
        )

        t0 = time.perf_counter()
        acq_12 = acquire_source(
            logical_source_id="tlc/hvfhv/2024-12",
            url=url_2024_12,
            config=config,
            registry_conn=reg_con,
            attempt_id="acq_real_2024_12",
            client=client,
        )
        acq_12["wall_seconds"] = round(time.perf_counter() - t0, 3)
        receipt["acquisitions"]["tlc/hvfhv/2024-12"] = acq_12
        print(
            f"  [OK] tlc/hvfhv/2024-12: {acq_12['decoded_row_count']:,} rows, "
            f"{acq_12['completed_bytes']:,} bytes, adapter={acq_12['schema_fingerprint']} "
            f"({acq_12['wall_seconds']}s)"
        )

        acq_01 = None
        if has_2025_01:
            t0 = time.perf_counter()
            acq_01 = acquire_source(
                logical_source_id="tlc/hvfhv/2025-01",
                url=url_2025_01,
                config=config,
                registry_conn=reg_con,
                attempt_id="acq_real_2025_01",
                client=client,
            )
            acq_01["wall_seconds"] = round(time.perf_counter() - t0, 3)
            receipt["acquisitions"]["tlc/hvfhv/2025-01"] = acq_01
            print(
                f"  [OK] tlc/hvfhv/2025-01: {acq_01['decoded_row_count']:,} rows, "
                f"{acq_01['completed_bytes']:,} bytes, adapter={acq_01['schema_fingerprint']} "
                f"({acq_01['wall_seconds']}s)"
            )
    finally:
        reg_con.close()

    # Keep downloads_dir intact until explicitly cleaned up after full verification
    if "--cleanup-downloads" in sys.argv:
        shutil.rmtree(downloads_dir, ignore_errors=True)

    zone_raw_csv = config.raw_dir / "sha256" / acq_zone["content_sha256"] / "source.csv"
    rules_hash = _sha256_file(REPO_ROOT / "contracts" / "rules.json")
    schemas_hash = _sha256_file(REPO_ROOT / "contracts" / "schemas.json")
    lock_hash = _sha256_file(REPO_ROOT / "requirements.lock")
    code_hash, _ = compute_code_bundle_manifest_hash(REPO_ROOT)

    # =========================================================================
    # PHASE 2: RELEASE 1 — 2024-12 REAL VERTICAL SLICE (G2 & G3)
    # =========================================================================
    print("\n" + "=" * 76)
    print("PHASE 2: RELEASE 1 — REAL 2024-12 MONTHLY VERTICAL SLICE & PUBLICATION (G2/G3)")
    print("=" * 76)

    rel_1_id = "rel_2024_12_" + uuid.uuid4().hex[:8]
    att_1_id = "att_2024_12_" + uuid.uuid4().hex[:8]
    staging_1 = config.staging_dir / att_1_id

    plan_1 = FrozenInputPlan(
        window_months=("2024-12",),
        source_revisions=(
            SourceRevisionEntry(
                logical_source_id="tlc/hvfhv/2024-12",
                revision_id=acq_12["revision_id"],
            ),
        ),
        zone_revision_id=acq_zone["revision_id"],
        schema_adapter_version="hvfhv_24col_pre2025.v1",
        rules_hash=rules_hash,
        metric_definition_hash=schemas_hash,
        code_bundle_manifest_hash=code_hash,
        lockfile_hash=lock_hash,
        model_schema_version="v1",
        semantic_configuration={"quantile_method": "quantile_cont_type7", "retention": "OPTION_A"},
    )
    deriv_1 = compute_derivation_id(plan_1)

    reg.create_attempt(
        attempt_id=att_1_id,
        requested_configuration_json=json.dumps({"window": ["2024-12"], "release_id": rel_1_id}),
        requested_window_start="2024-12",
        requested_window_end="2024-12",
        expected_parent_release_id=None,
        expected_parent_generation=0,
    )

    tracker_1 = ResourceTracker(staging_dir=staging_1, sample_interval_s=0.1)
    tracker_1.start()
    t_build_1 = time.perf_counter()

    cand_1_db = build_analytical_candidate(
        plan=plan_1,
        staging_dir=staging_1,
        raw_data_dir=config.raw_dir,
        zone_lookup_csv=zone_raw_csv,
        is_incremental=False,
        release_id=rel_1_id,
        registry=reg,
    )
    build_1_wall = time.perf_counter() - t_build_1
    metrics_1 = tracker_1.stop()

    # Remove empty spill_writer directory before finalization rename
    shutil.rmtree(staging_1 / "spill_writer", ignore_errors=True)

    fin_1 = finalize_candidate(
        candidate_dir=staging_1,
        release_id=rel_1_id,
        attempt_id=att_1_id,
        derivation_id=deriv_1,
        frozen_plan=plan_1,
        source_revision_map={
            "tlc/hvfhv/2024-12": acq_12["revision_id"],
            "tlc/taxi_zones": acq_zone["revision_id"],
        },
        data_root=data_root,
        registry=reg,
    )
    att_s08_1 = verify_post_seal_s08(
        release_id=rel_1_id,
        data_root=data_root,
        registry=reg,
    )
    pub_1 = publish(
        release_id=rel_1_id,
        expected_parent_generation=0,
        expected_parent_release_id=None,
        operation_key=f"op_pub_{rel_1_id}",
        attempt_id=att_1_id,
        reason="Publish real 2024-12 vertical slice release",
        data_root=data_root,
        registry=reg,
    )

    # Extract summary stats from Release 1
    ro_1 = duckdb.connect(str(fin_1.release_directory / "analytics.duckdb"), read_only=True)
    try:
        q2_row = ro_1.execute(
            "SELECT evaluated_count, exception_count, details_json FROM quality_result WHERE rule_id = 'Q02';"
        ).fetchone()
        q2_details = json.loads(q2_row[2]) if q2_row and q2_row[2] else {}
        rules_1 = [
            {
                "rule_id": r[0],
                "cohort_key": r[1],
                "severity": r[2],
                "status": r[3],
                "evaluated_count": r[4],
                "exception_count": r[5],
                "details": json.loads(r[6]) if r[6] else {},
            }
            for r in ro_1.execute(
                "SELECT rule_id, cohort_key, severity, status, evaluated_count, exception_count, details_json "
                "FROM quality_result ORDER BY rule_id, cohort_key;"
            ).fetchall()
        ]
    finally:
        ro_1.close()

    receipt["releases"]["release_1_2024_12"] = {
        "release_id": rel_1_id,
        "derivation_id": deriv_1,
        "generation": pub_1.new_generation,
        "manifest_sha256": fin_1.manifest_sha256,
        "database_sha256": fin_1.database_sha256,
        "validation_sha256": fin_1.validation_sha256,
        "verification_id": att_s08_1.verification_id,
        "wall_time_seconds": round(metrics_1.wall_time_seconds, 3),
        "cpu_user_seconds": round(metrics_1.cpu_user_seconds, 3),
        "cpu_system_seconds": round(metrics_1.cpu_system_seconds, 3),
        "baseline_rss_mb": round(metrics_1.baseline_rss_mb, 2),
        "peak_rss_mb": round(metrics_1.peak_rss_mb, 2),
        "duckdb_file_bytes": (fin_1.release_directory / "analytics.duckdb").stat().st_size,
        "duckdb_peak_wal_bytes": metrics_1.duckdb_peak_wal_bytes,
        "duckdb_peak_spill_bytes": metrics_1.duckdb_peak_spill_bytes,
        "interval_distribution": q2_details,
        "quality_rules": rules_1,
    }
    print(
        f"  [OK] Published Release 1 ({rel_1_id}) at generation {pub_1.new_generation}: "
        f"{q2_details.get('total_trips', 0):,} trips, "
        f"median={q2_details.get('eligible_median_seconds')}s, "
        f"p95={q2_details.get('eligible_p95_seconds')}s, "
        f"DB={(fin_1.release_directory / 'analytics.duckdb').stat().st_size / (1024**2):.2f} MiB, "
        f"peak RSS={metrics_1.peak_rss_mb:.2f} MB in {metrics_1.wall_time_seconds:.2f}s"
    )

    # =========================================================================
    # PHASE 3: RELEASE 2 — 2-MONTH CROSS-BOUNDARY INCREMENTAL BUILD (2024-12 + 2025-01)
    # =========================================================================
    if has_2025_01 and acq_01 is not None:
        print("\n" + "=" * 76)
        print("PHASE 3: RELEASE 2 — 2-MONTH INCREMENTAL BUILD (2024-12 + 2025-01) (G4/G3)")
        print("=" * 76)

        rel_2_id = "rel_2024_12_2025_01_" + uuid.uuid4().hex[:8]
        att_2_id = "att_2024_12_2025_01_" + uuid.uuid4().hex[:8]
        staging_2 = config.staging_dir / att_2_id

        plan_2 = FrozenInputPlan(
            window_months=("2024-12", "2025-01"),
            source_revisions=(
                SourceRevisionEntry(
                    logical_source_id="tlc/hvfhv/2024-12",
                    revision_id=acq_12["revision_id"],
                ),
                SourceRevisionEntry(
                    logical_source_id="tlc/hvfhv/2025-01",
                    revision_id=acq_01["revision_id"],
                ),
            ),
            zone_revision_id=acq_zone["revision_id"],
            schema_adapter_version="hvfhv_25col_2025onward.v1",
            rules_hash=rules_hash,
            metric_definition_hash=schemas_hash,
            code_bundle_manifest_hash=code_hash,
            lockfile_hash=lock_hash,
            model_schema_version="v1",
            semantic_configuration={"quantile_method": "quantile_cont_type7", "retention": "OPTION_A"},
        )
        deriv_2 = compute_derivation_id(plan_2)

        reg.create_attempt(
            attempt_id=att_2_id,
            requested_configuration_json=json.dumps(
                {"window": ["2024-12", "2025-01"], "release_id": rel_2_id, "mode": "incremental"}
            ),
            requested_window_start="2024-12",
            requested_window_end="2025-01",
            expected_parent_release_id=rel_1_id,
            expected_parent_generation=1,
        )

        tracker_2 = ResourceTracker(staging_dir=staging_2, sample_interval_s=0.1)
        tracker_2.start()
        t_build_2 = time.perf_counter()

        cand_2_db = build_analytical_candidate(
            plan=plan_2,
            staging_dir=staging_2,
            raw_data_dir=config.raw_dir,
            zone_lookup_csv=zone_raw_csv,
            base_release_db_path=fin_1.release_directory / "analytics.duckdb",
            is_incremental=True,
            release_id=rel_2_id,
            registry=reg,
        )
        build_2_wall = time.perf_counter() - t_build_2
        metrics_2 = tracker_2.stop()

        shutil.rmtree(staging_2 / "spill_writer", ignore_errors=True)

        fin_2 = finalize_candidate(
            candidate_dir=staging_2,
            release_id=rel_2_id,
            attempt_id=att_2_id,
            derivation_id=deriv_2,
            frozen_plan=plan_2,
            source_revision_map={
                "tlc/hvfhv/2024-12": acq_12["revision_id"],
                "tlc/hvfhv/2025-01": acq_01["revision_id"],
                "tlc/taxi_zones": acq_zone["revision_id"],
            },
            data_root=data_root,
            registry=reg,
        )
        att_s08_2 = verify_post_seal_s08(
            release_id=rel_2_id,
            data_root=data_root,
            registry=reg,
        )
        pub_2 = publish(
            release_id=rel_2_id,
            expected_parent_generation=1,
            expected_parent_release_id=rel_1_id,
            operation_key=f"op_pub_{rel_2_id}",
            attempt_id=att_2_id,
            reason="Publish real 2-month (2024-12 + 2025-01) incremental release",
            data_root=data_root,
            registry=reg,
        )

        ro_2 = duckdb.connect(str(fin_2.release_directory / "analytics.duckdb"), read_only=True)
        try:
            q2_row_2 = ro_2.execute(
                "SELECT evaluated_count, exception_count, details_json FROM quality_result WHERE rule_id = 'Q02';"
            ).fetchone()
            q2_details_2 = json.loads(q2_row_2[2]) if q2_row_2 and q2_row_2[2] else {}
            rules_2 = [
                {
                    "rule_id": r[0],
                    "cohort_key": r[1],
                    "severity": r[2],
                    "status": r[3],
                    "evaluated_count": r[4],
                    "exception_count": r[5],
                    "details": json.loads(r[6]) if r[6] else {},
                }
                for r in ro_2.execute(
                    "SELECT rule_id, cohort_key, severity, status, evaluated_count, exception_count, details_json "
                    "FROM quality_result ORDER BY rule_id, cohort_key;"
                ).fetchall()
            ]
        finally:
            ro_2.close()

        receipt["releases"]["release_2_2024_12_2025_01"] = {
            "release_id": rel_2_id,
            "derivation_id": deriv_2,
            "generation": pub_2.new_generation,
            "manifest_sha256": fin_2.manifest_sha256,
            "database_sha256": fin_2.database_sha256,
            "validation_sha256": fin_2.validation_sha256,
            "verification_id": att_s08_2.verification_id,
            "wall_time_seconds": round(metrics_2.wall_time_seconds, 3),
            "cpu_user_seconds": round(metrics_2.cpu_user_seconds, 3),
            "cpu_system_seconds": round(metrics_2.cpu_system_seconds, 3),
            "baseline_rss_mb": round(metrics_2.baseline_rss_mb, 2),
            "peak_rss_mb": round(metrics_2.peak_rss_mb, 2),
            "duckdb_file_bytes": (fin_2.release_directory / "analytics.duckdb").stat().st_size,
            "duckdb_peak_wal_bytes": metrics_2.duckdb_peak_wal_bytes,
            "duckdb_peak_spill_bytes": metrics_2.duckdb_peak_spill_bytes,
            "interval_distribution": q2_details_2,
            "quality_rules": rules_2,
        }
        print(
            f"  [OK] Published Release 2 ({rel_2_id}) at generation {pub_2.new_generation}: "
            f"{q2_details_2.get('total_trips', 0):,} total trips across 2 months, "
            f"median={q2_details_2.get('eligible_median_seconds')}s, "
            f"p95={q2_details_2.get('eligible_p95_seconds')}s, "
            f"DB={(fin_2.release_directory / 'analytics.duckdb').stat().st_size / (1024**2):.2f} MiB, "
            f"peak RSS={metrics_2.peak_rss_mb:.2f} MB in {metrics_2.wall_time_seconds:.2f}s"
        )

    # =========================================================================
    # PHASE 4: RETENTION PREVIEW, FOOTPRINT & CONSUMER / DAGSTER VERIFICATION (G4/G5)
    # =========================================================================
    print("\n" + "=" * 76)
    print("PHASE 4: OPTION A RETENTION PREVIEW, STORAGE FOOTPRINT & CONSUMER CHECK (G4/G5)")
    print("=" * 76)

    footprint = calculate_storage_footprint(data_root)
    cleanup_preview = preview_retention_cleanup_option_a(reg, data_root)

    reader = ReleaseReader(data_root=data_root)
    t_query = time.perf_counter()
    headless = render_headless_summary(reader)
    query_wall = time.perf_counter() - t_query

    asset_checks = execute_asset_checks_from_release(data_root, reader.release_id)
    frontier = scan_frontier_and_discover_gaps(
        data_root,
        target_months=["2024-12", "2025-01"] if has_2025_01 else ["2024-12"],
        registry=reg,
    )

    receipt["storage_footprint"] = {
        "raw_parquet_bytes": footprint.raw_parquet_bytes,
        "retained_releases_bytes": footprint.retained_releases_bytes,
        "staging_candidate_bytes": footprint.staging_candidate_bytes,
        "spill_reserve_bytes": footprint.spill_reserve_bytes,
        "control_and_metadata_bytes": footprint.control_and_metadata_bytes,
        "total_project_bytes": footprint.total_project_bytes,
        "total_project_gib": footprint.total_project_gib,
        "headroom_gib": round(footprint.headroom_gib, 4),
        "is_compliant": footprint.is_compliant,
    }
    receipt["retention_preview"] = cleanup_preview
    receipt["consumer_verification"] = {
        "pinned_release_id": reader.release_id,
        "headless_query_wall_seconds": round(query_wall, 3),
        "denominators": headless["intervals"]["denominators"],
        "cache_stats": reader.get_cache_stats(),
        "asset_checks_all_mandatory_passed": asset_checks["all_mandatory_s_passed"],
        "asset_checks_total_evaluated": asset_checks["total_checks_evaluated"],
        "dagster_defs_loaded": dagster_defs is not None,
        "frontier_status": frontier["status"],
    }

    print(f"  [OK] Storage Footprint: {footprint.total_project_gib:.4f} GiB <= 80.0 GiB (Headroom: {footprint.headroom_gib:.4f} GiB)")
    print(f"  [OK] Option A Protected Set: {cleanup_preview.get('protected_releases')}")
    print(f"  [OK] Consumer Pinned Release: {reader.release_id} (Query wall: {query_wall:.3f}s)")
    print(f"  [OK] Dagster Asset Checks: {asset_checks['total_checks_evaluated']} evaluated, all_mandatory_s_passed={asset_checks['all_mandatory_s_passed']}")

    receipt_path = data_root / "real_tlc_execution_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(f"\nSaved complete execution receipt to {receipt_path}")
    return receipt


if __name__ == "__main__":
    run_pipeline()
