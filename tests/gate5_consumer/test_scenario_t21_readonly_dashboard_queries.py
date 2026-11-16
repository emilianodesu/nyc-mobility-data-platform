"""Gate G5 Acceptance: Scenario T21 & W04 - Read-only Dashboard Queries.

Verifies:
1. Reader performs out-of-cache eligibility check against SQLite registry before query.
2. Reader executes queries with query.lock without blocking or being blocked by background processes.
3. Concurrent reader and writer execute safely without file corruption or deadlock.
4. Post-query eligibility re-verification guarantees consistency.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import uuid
from pathlib import Path

import duckdb

from hvfhv.consumer.app import render_headless_summary
from hvfhv.consumer.reader import (
    ReleaseReader,
    IneligibleReleaseError,
    RetiredReleaseError,
    ArtifactNotFoundError,
)
from hvfhv.control.locks import writer_lock, query_lock
from hvfhv.control.publication import (
    finalize_candidate,
    verify_post_seal_s08,
    publish,
)
from hvfhv.control.registry import Registry
from hvfhv.transforms.models import (
    init_analytical_models,
    populate_release_metadata,
    populate_coverage,
    build_interval_aggregates,
)
from hvfhv.transforms.quality import (
    init_quality_models,
    QualityEngine,
)


def setup_test_published_release(data_root: Path, rel_suffix: str = "1") -> tuple[Registry, str, Path]:
    for sub in ["control", "raw", "staging", "releases", "spill"]:
        (data_root / sub).mkdir(parents=True, exist_ok=True)
    registry = Registry(db_path=data_root / "control" / "registry.sqlite", data_root=data_root)

    # Register source object, revision, and attempt
    logical_src = "tlc/hvfhv/2024-12"
    rev_id = f"rev_{rel_suffix}_" + uuid.uuid4().hex[:8]
    att_id = f"att_{rel_suffix}_" + uuid.uuid4().hex[:8]

    registry.register_source_object(logical_src, "hvfhv", "https://example.com/src.parquet", "2024-12")
    registry.create_attempt(att_id, requested_configuration_json="{}")
    registry.register_revision(
        revision_id=rev_id,
        logical_source_id=logical_src,
        content_sha256="a" * 64,
        byte_count=10000,
        relative_path=f"raw/src_{rel_suffix}.parquet",
        schema_fingerprint="fingerprint_v1",
        footer_row_count=100,
        decoded_row_count=100,
        provenance_relative_path=f"raw/prov_{rel_suffix}.json",
    )

    rel_id = f"rel_{rel_suffix}_" + uuid.uuid4().hex[:8]
    staging_dir = data_root / "staging" / att_id
    staging_dir.mkdir(parents=True, exist_ok=True)
    db_path = staging_dir / "analytics.duckdb"

    # Create real DuckDB database with canonical schemas from sql/models.sql & sql/quality_and_profiles.sql
    con = duckdb.connect(str(db_path))
    init_analytical_models(con)
    init_quality_models(con)

    # 1. Populate zone_reference (including sentinel IDs 1, 264, 265 and ordinary zones 100, 200)
    zone_rows = [
        (1, "EWR", "Newark Airport", "EWR", "zone_rev_1"),
        (100, "Manhattan", "Garment District", "Yellow Zone", "zone_rev_1"),
        (200, "Bronx", "Riverdale", "Boro Zone", "zone_rev_1"),
        (264, "Unknown", "NV", "N/A", "zone_rev_1"),
        (265, "Unknown", "NA", "N/A", "zone_rev_1"),
    ]
    con.executemany(
        "INSERT INTO zone_reference (location_id, borough, zone, service_zone, reference_revision_id) VALUES (?, ?, ?, ?, ?);",
        zone_rows,
    )

    # 2. Populate source_contribution
    con.execute(
        """
        INSERT INTO source_contribution (
            logical_source_id, revision_id, content_sha256, source_month,
            source_schema_version, cbd_fee_field_present, decoded_row_count, inserted_row_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [logical_src, rev_id, "a" * 64, "2024-12", "hvfhv_24col_pre2025.v1", False, 100, 100],
    )

    # 3. Populate 100 trips in trip_observation (0..94 eligible, 95..97 negative, 98..99 missing)
    trip_rows = []
    for rn in range(95):
        sec_offset = (rn + 1) * 10
        mins, secs = divmod(sec_offset, 60)
        pu_str = f"2024-12-15 10:{mins:02d}:{secs:02d}"
        do_str = f"2024-12-15 10:{mins + 12:02d}:{secs:02d}"
        trip_rows.append((
            rev_id, rn, "2024-12", "HV0003", "B03404", "B03404",
            "2024-12-15 10:00:00", "2024-12-15 10:00:05", pu_str, do_str,
            100, 200, 2.5, 720, 20.0, 0.0, 0.5, 1.5, 2.75, 0.0, 3.0, 15.0,
            "N", "N", "N", "N", "N", None,
        ))
    for rn in range(95, 98):
        # Negative recorded interval: pickup 5 minutes before request (-300.0s)
        trip_rows.append((
            rev_id, rn, "2024-12", "HV0003", "B03404", "B03404",
            "2024-12-15 10:05:00", "2024-12-15 10:00:00", "2024-12-15 10:00:00", "2024-12-15 10:12:00",
            100, 200, 2.5, 720, 20.0, 0.0, 0.5, 1.5, 2.75, 0.0, 3.0, 15.0,
            "N", "N", "N", "N", "N", None,
        ))
    for rn in range(98, 100):
        # Missing request_datetime (NULL)
        trip_rows.append((
            rev_id, rn, "2024-12", "HV0003", "B03404", "B03404",
            None, None, "2024-12-15 10:05:00", "2024-12-15 10:17:00",
            100, 200, 2.5, 720, 20.0, 0.0, 0.5, 1.5, 2.75, 0.0, 3.0, 15.0,
            "N", "N", "N", "N", "N", None,
        ))

    con.executemany(
        """
        INSERT INTO trip_observation (
            revision_id, source_row_number, source_month, hvfhs_license_num,
            dispatching_base_num, originating_base_num, request_datetime,
            on_scene_datetime, pickup_datetime, dropoff_datetime, pu_location_id,
            do_location_id, trip_miles, trip_time, base_passenger_fare, tolls,
            bcf, sales_tax, congestion_surcharge, airport_fee, tips, driver_pay,
            shared_request_flag, shared_match_flag, access_a_ride_flag,
            wav_request_flag, wav_match_flag, cbd_congestion_fee
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        trip_rows,
    )

    # 4. Build interval_daily and interval_monthly, coverage, release_metadata, and pre-seal rules
    build_interval_aggregates(con)
    populate_coverage(con, ["2024-11", "2024-12"], {"2024-12": (rev_id, 100)})
    populate_release_metadata(
        con,
        release_id=rel_id,
        derivation_id="deriv_1",
        model_schema_version="v1",
        rules_version="v1",
        metric_definition_version="v1",
        code_bundle_manifest_hash="c" * 64,
        lockfile_hash="l" * 64,
        zone_revision_id="zone_rev_1",
        first_source_month="2024-11",
        last_source_month="2024-12",
        built_at_utc="2026-11-11T12:00:00Z",
    )

    engine = QualityEngine(con)
    engine.run_pre_seal_rules(
        plan_months=["2024-11", "2024-12"],
        expected_derivation_id="deriv_1",
        is_earliest_month=True,
    )

    # 5. Finalize candidate, run post-seal S08 verification, and publish via CAS
    frozen_plan_dict = {
        "window_months": ["2024-11", "2024-12"],
        "source_revisions": [
            {
                "logical_source_id": logical_src,
                "revision_id": rev_id,
                "content_sha256": "a" * 64,
                "byte_count": 10000,
                "decoded_row_count": 100,
            }
        ],
        "zone_revision_id": "zone_rev_1",
        "schema_adapter_version": "hvfhv_24col_pre2025.v1",
        "rules_hash": "r" * 64,
        "metric_definition_hash": "m" * 64,
        "code_bundle_manifest_hash": "c" * 64,
        "lockfile_hash": "l" * 64,
        "model_schema_version": "v1",
        "semantic_configuration": {},
    }

    finalized = finalize_candidate(
        candidate_dir=staging_dir,
        release_id=rel_id,
        attempt_id=att_id,
        derivation_id="deriv_1",
        frozen_plan=frozen_plan_dict,
        source_revision_map={logical_src: rev_id},
        data_root=data_root,
        duckdb_writer_con=con,
        registry=registry,
    )
    verify_post_seal_s08(
        release_id=rel_id,
        data_root=data_root,
        registry=registry,
    )

    curr = registry.get_current_release()
    publish(
        release_id=rel_id,
        expected_parent_generation=curr["generation"],
        expected_parent_release_id=curr["release_id"],
        operation_key=f"op_publish_{rel_suffix}_{uuid.uuid4().hex[:6]}",
        attempt_id=att_id,
        data_root=data_root,
        registry=registry,
    )

    return registry, rel_id, finalized.release_directory


def test_t21_readonly_dashboard_queries_and_concurrency():
    """Scenario T21 & W04: Read-only Dashboard Queries and concurrent background operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        # 1. Instantiate reader and verify automatic resolution of current release
        reader = ReleaseReader(data_root=root)
        assert reader.release_id == rel_id

        # 2. Verify out-of-cache eligibility check & first-use manifest digest verification
        eligibility = reader.check_eligibility()
        assert eligibility["status"] == "VERIFIED"
        assert eligibility["database_path"].exists()
        assert reader._manifest_verified is True

        # 3. Fetch Data Reliability summary using canonical schemas
        summary = reader.get_reliability_summary()
        assert summary["release_id"] == rel_id
        assert summary["metadata"]["model_schema_version"] == "v1"
        assert summary["metadata"]["derivation_id"] == "deriv_1"
        assert len(summary["source_contributions"]) == 1
        assert summary["source_contributions"][0]["inserted_row_count"] == 100
        assert len(summary["coverage"]) == 2
        # Verify absent month has NULL row count (never 0)
        absent_month = [c for c in summary["coverage"] if c["source_month"] == "2024-11"][0]
        assert absent_month["row_count"] is None
        assert absent_month["is_available"] is False
        assert absent_month["is_included"] is False
        present_month = [c for c in summary["coverage"] if c["source_month"] == "2024-12"][0]
        assert present_month["row_count"] == 100
        assert present_month["is_included"] is True
        assert summary["s08_verification"] is not None
        assert summary["s08_verification"]["status"] == "PASS"
        assert len(summary["quality_rules"]) == 16  # S01-S07 and Q01-Q09
        assert len(summary["column_profiles"]) > 0

        # 4. Fetch Recorded Intervals view with exact denominators
        intervals = reader.query_recorded_intervals(source_month="2024-12")
        denoms = intervals["denominators"]
        assert denoms["total_population_count"] == 100
        assert denoms["eligible_count"] == 95
        assert denoms["negative_count"] == 3
        assert denoms["missing_count"] == 2
        assert abs(denoms["eligible_fraction"] - 0.95) < 1e-9
        # Verify denominator identity
        assert denoms["total_population_count"] == (
            denoms["eligible_count"] + denoms["negative_count"] + denoms["missing_count"]
        )
        assert denoms["median_seconds"] == 480.0
        assert denoms["p95_seconds"] is not None
        assert len(intervals["daily_breakdown"]) == 1
        assert intervals["daily_breakdown"][0]["borough"] == "Manhattan"
        assert len(intervals["sample_rows"]) == 100
        assert "revision_id" in intervals["sample_rows"][0]
        assert "source_row_number" in intervals["sample_rows"][0]
        assert "pu_zone_status" in intervals["sample_rows"][0]

        # Verify headless app rendering works end-to-end
        headless = render_headless_summary(reader)
        assert headless["reliability"]["release_id"] == rel_id
        assert headless["intervals"]["denominators"]["total_population_count"] == 100

        # 5. Concurrent reader and writer execution (Scenario T21 / W04)
        # Verify writer holding writer_lock does not deadlock or block reader under query_lock
        reader_completed = []
        writer_completed = []

        def run_reader_queries():
            for _ in range(5):
                res = reader.query_recorded_intervals(source_month="2024-12")
                assert res["denominators"]["total_population_count"] == 100
                time.sleep(0.01)
            reader_completed.append(True)

        def run_writer_operations():
            with writer_lock(root):
                # Simulate background acquisition/control mutation
                time.sleep(0.05)
                writer_completed.append(True)

        t_read = threading.Thread(target=run_reader_queries)
        t_write = threading.Thread(target=run_writer_operations)

        t_read.start()
        t_write.start()

        t_read.join(timeout=5.0)
        t_write.join(timeout=5.0)

        assert not t_read.is_alive(), "Reader thread timed out (deadlock detected)"
        assert not t_write.is_alive(), "Writer thread timed out (deadlock detected)"
        assert reader_completed == [True]
        assert writer_completed == [True]


def test_t21_unregistered_release_raises_ineligible():
    """Scenario T21: Attempting to query an unregistered or rejected release raises IneligibleReleaseError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        reader = ReleaseReader(data_root=root, release_id="nonexistent_rel_999")
        try:
            reader.check_eligibility()
            assert False, "Should have raised IneligibleReleaseError"
        except IneligibleReleaseError as e:
            assert "not registered" in str(e)

        # Also verify REJECTED release raises IneligibleReleaseError with REJECTED diagnostic
        registry.reject_release(rel_id, reason="Operator rejection test")
        rejected_reader = ReleaseReader(data_root=root, release_id=rel_id)
        try:
            rejected_reader.check_eligibility()
            assert False, "Should have raised IneligibleReleaseError for REJECTED release"
        except IneligibleReleaseError as e:
            assert "REJECTED" in str(e)


def test_t21_missing_database_file_raises_artifact_not_found():
    """Scenario T21: If DuckDB database file is missing, raises ArtifactNotFoundError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        # Unlink DuckDB file
        (rel_dir / "analytics.duckdb").unlink()

        reader = ReleaseReader(data_root=root, release_id=rel_id)
        try:
            reader.check_eligibility()
            assert False, "Should have raised ArtifactNotFoundError"
        except ArtifactNotFoundError as e:
            assert "missing" in str(e)
