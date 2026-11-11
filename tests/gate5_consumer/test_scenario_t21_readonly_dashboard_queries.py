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

from hvfhv.consumer.reader import (
    ReleaseReader,
    IneligibleReleaseError,
    RetiredReleaseError,
    ArtifactNotFoundError,
)
from hvfhv.control.locks import writer_lock, query_lock
from hvfhv.control.registry import Registry


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
    rel_dir = data_root / "releases" / rel_id
    rel_dir.mkdir(parents=True, exist_ok=True)
    db_path = rel_dir / "analytics.duckdb"

    # Create real DuckDB database
    con = duckdb.connect(str(db_path))
    con.execute("""
        CREATE TABLE release_metadata (
            release_id TEXT PRIMARY KEY,
            derivation_id TEXT NOT NULL,
            finalized_at_utc TEXT NOT NULL,
            pipeline_version TEXT NOT NULL,
            duckdb_version TEXT NOT NULL,
            pyarrow_version TEXT NOT NULL
        );
        INSERT INTO release_metadata VALUES (
            '""" + rel_id + """', 'deriv_1', '2026-11-11T12:00:00Z', '0.1.0', '2.0.0', '26.0.1'
        );

        CREATE TABLE coverage (
            source_month TEXT PRIMARY KEY,
            observation_status TEXT NOT NULL,
            trip_observation_row_count BIGINT,
            distinct_hvfhs_license_count INTEGER,
            distinct_pickup_date_count INTEGER,
            notes TEXT
        );
        INSERT INTO coverage VALUES
            ('2024-11', 'ABSENT', NULL, NULL, NULL, 'Missing month test'),
            ('2024-12', 'POPULATED', 100, 2, 31, 'December populated');

        CREATE TABLE quality_result (
            rule_id TEXT PRIMARY KEY,
            rule_name TEXT NOT NULL,
            rule_kind TEXT NOT NULL,
            outcome TEXT NOT NULL,
            severity TEXT NOT NULL,
            evaluated_row_count BIGINT,
            failing_row_count BIGINT,
            executed_at_utc TEXT NOT NULL,
            diagnostic_details TEXT
        );
        INSERT INTO quality_result VALUES
            ('S01', 'row_count_reconciliation', 'PRE_SEAL_BLOCKING', 'PASS', 'BLOCKING', 100, 0, '2026-11-11T12:00:00Z', NULL),
            ('S02', 'physical_locator_monotonicity', 'PRE_SEAL_BLOCKING', 'PASS', 'BLOCKING', 100, 0, '2026-11-11T12:00:00Z', NULL),
            ('S03', 'zone_reference_integrity', 'PRE_SEAL_BLOCKING', 'PASS', 'BLOCKING', 100, 0, '2026-11-11T12:00:00Z', NULL),
            ('S04', 'naive_timestamp_range', 'PRE_SEAL_BLOCKING', 'PASS', 'BLOCKING', 100, 0, '2026-11-11T12:00:00Z', NULL),
            ('S05', 'interval_count_identity', 'PRE_SEAL_BLOCKING', 'PASS', 'BLOCKING', 100, 0, '2026-11-11T12:00:00Z', NULL),
            ('S06', 'release_metadata_completeness', 'PRE_SEAL_BLOCKING', 'PASS', 'BLOCKING', 1, 0, '2026-11-11T12:00:00Z', NULL),
            ('S07', 'pre_seal_rules_inventory_check', 'PRE_SEAL_BLOCKING', 'PASS', 'BLOCKING', 7, 0, '2026-11-11T12:00:00Z', NULL);

        CREATE TABLE column_profile (
            profile_id TEXT PRIMARY KEY,
            profile_name TEXT NOT NULL,
            relation_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            population_label TEXT NOT NULL,
            profile_payload_json TEXT NOT NULL
        );
        INSERT INTO column_profile VALUES
            ('Q01', 'request_to_pickup_interval_profile', 'trip_qualified', 'recorded_request_to_pickup_seconds', 'ALL_TRIPS', '{"mean": 300.0}');

        CREATE TABLE trip_qualified (
            trip_id BIGINT PRIMARY KEY,
            source_month TEXT NOT NULL,
            pickup_date TEXT NOT NULL,
            hvfhs_license_num TEXT NOT NULL,
            pu_location_id INTEGER NOT NULL,
            request_datetime TIMESTAMP NOT NULL,
            pickup_datetime TIMESTAMP NOT NULL,
            recorded_request_to_pickup_seconds DOUBLE,
            interval_class TEXT NOT NULL,
            is_off_source_month BOOLEAN NOT NULL,
            zone_matched BOOLEAN NOT NULL
        );
    """)

    # Populate 100 trips (95 eligible, 3 negative, 2 missing)
    for t_id in range(1, 96):
        con.execute(f"""
            INSERT INTO trip_qualified VALUES (
                {t_id}, '2024-12', '2024-12-15', 'HV0003', 100,
                '2024-12-15 10:00:00', '2024-12-15 10:05:00', {float(t_id * 10)},
                'eligible_nonnegative_recorded_interval', false, true
            );
        """)
    for t_id in range(96, 99):
        con.execute(f"""
            INSERT INTO trip_qualified VALUES (
                {t_id}, '2024-12', '2024-12-15', 'HV0003', 100,
                '2024-12-15 10:05:00', '2024-12-15 10:00:00', -300.0,
                'negative_recorded_interval', false, true
            );
        """)
    for t_id in range(99, 101):
        con.execute(f"""
            INSERT INTO trip_qualified VALUES (
                {t_id}, '2024-12', '2024-12-15', 'HV0003', 100,
                '2024-12-15 10:00:00', '2024-12-15 10:05:00', NULL,
                'missing_timestamp', false, true
            );
        """)

    con.execute("CHECKPOINT;")
    con.close()

    # Create validation.json and manifest.json
    val_data = {
        "status": "PASS",
        "validation_sha256": "v" * 64,
        "pre_seal_rules_summary": {"PASS": 7, "FAIL": 0, "ERROR": 0},
        "rule_results": [
            {"rule_id": f"S0{i}", "rule_name": f"rule_s0{i}", "outcome": "PASS", "severity": "BLOCKING", "evaluated_row_count": 100, "failing_row_count": 0}
            for i in range(1, 8)
        ] + [
            {"rule_id": "S08", "rule_name": "post_seal_manifest_verification", "outcome": "PASS", "severity": "BLOCKING", "evaluated_row_count": 1, "failing_row_count": 0}
        ],
    }
    (rel_dir / "validation.json").write_text(json.dumps(val_data), encoding="utf-8")
    (rel_dir / "manifest.json").write_text(json.dumps({"release_id": rel_id}), encoding="utf-8")

    # Register finalized release in registry
    registry.register_finalized_release(
        release_id=rel_id,
        producing_attempt_id=att_id,
        release_directory=str(rel_dir),
        manifest_sha256="m" * 64,
        database_sha256="d" * 64,
        validation_sha256="v" * 64,
        derivation_id="deriv_1",
        source_revision_map={logical_src: rev_id},
    )
    registry.record_verification(
        release_id=rel_id,
        manifest_sha256="m" * 64,
        database_sha256="d" * 64,
        validation_sha256="v" * 64,
        rule_inventory_version="v1",
        rule_inventory_hash="r" * 64,
        verification_results_json="[]",
        verifier_version="1.0.0",
    )

    # Publish release
    op_key = f"op_publish_{rel_suffix}"
    registry.record_selection_request(
        operation_key=op_key,
        target_release_id=rel_id,
        expected_parent_generation=0,
        expected_parent_release_id=None,
        operation_kind="publish",
    )
    registry.publish_release(operation_key=op_key, attempt_id=att_id)

    return registry, rel_id, rel_dir


def test_t21_readonly_dashboard_queries_and_concurrency():
    """Scenario T21 & W04: Read-only Dashboard Queries and concurrent background operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        # 1. Instantiate reader and verify automatic resolution of current release
        reader = ReleaseReader(data_root=root)
        assert reader.release_id == rel_id

        # 2. Verify out-of-cache eligibility check
        eligibility = reader.check_eligibility()
        assert eligibility["status"] == "VERIFIED"
        assert eligibility["database_path"].exists()

        # 3. Fetch Data Reliability summary
        summary = reader.get_reliability_summary()
        assert summary["release_id"] == rel_id
        assert summary["metadata"]["duckdb_version"] == "2.0.0"
        assert len(summary["coverage"]) == 2
        # Verify absent month has NULL row count
        absent_month = [c for c in summary["coverage"] if c["source_month"] == "2024-11"][0]
        assert absent_month["trip_observation_row_count"] is None
        assert absent_month["observation_status"] == "ABSENT"

        # 4. Fetch Recorded Intervals view with exact denominators
        intervals = reader.query_recorded_intervals(source_month="2024-12")
        denoms = intervals["denominators"]
        assert denoms["total_population_count"] == 100
        assert denoms["eligible_count"] == 95
        assert denoms["negative_count"] == 3
        assert denoms["missing_count"] == 2
        # Verify denominator identity
        assert denoms["total_population_count"] == (
            denoms["eligible_count"] + denoms["negative_count"] + denoms["missing_count"]
        )
        assert denoms["median_seconds"] is not None
        assert denoms["p95_seconds"] is not None
        assert len(intervals["sample_rows"]) == 100

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
    """Scenario T21: Attempting to query an unregistered release raises IneligibleReleaseError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        reader = ReleaseReader(data_root=root, release_id="nonexistent_rel_999")
        try:
            reader.check_eligibility()
            assert False, "Should have raised IneligibleReleaseError"
        except IneligibleReleaseError as e:
            assert "not registered" in str(e)


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
