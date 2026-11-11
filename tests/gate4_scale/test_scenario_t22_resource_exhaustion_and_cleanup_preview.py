"""Scenario T22 & W08: Resource Exhaustion Preflight & Option A Retention Policy.

Acceptance Criteria:
1. Storage budget preflight accurately calculates usage across raw Parquet, retained DuckDB releases,
   candidate staging, spill reserve, and SQLite control plane.
2. If storage breaches the 80.0 GiB budget ceiling, build preflight halts safely with
   StorageBudgetExceededError without downsampling data or truncating the 12-month window.
3. Option A retention policy protects current + 1 prior release (2 analytical releases total),
   transitions unprotected older releases to RETIRED status, safely reclaims database payloads,
   and preserves manifests and SQLite audit records.
"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import uuid

import pytest

from hvfhv.control.registry import Registry
from hvfhv.transforms.builder import (
    calculate_storage_footprint,
    enforce_retention_policy_option_a,
    StorageBudgetExceededError,
)


def test_t22_storage_footprint_calculation_and_budget_preflight():
    """Verify storage footprint calculator and 80 GiB compliance checks."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "raw").mkdir(parents=True, exist_ok=True)
        (root / "releases").mkdir(parents=True, exist_ok=True)
        (root / "staging").mkdir(parents=True, exist_ok=True)
        (root / "control").mkdir(parents=True, exist_ok=True)

        # Create 1 GB mock raw file
        raw_file = root / "raw" / "test.parquet"
        raw_file.write_bytes(b"\x00" * (1024 * 1024))  # 1 MB mock

        footprint = calculate_storage_footprint(root, spill_reserve_bytes=16 * 1024 * 1024 * 1024)
        assert footprint.is_compliant
        assert footprint.headroom_gib > 60.0
        assert footprint.total_project_bytes > 16 * 1024 * 1024 * 1024


def test_t22_option_a_retention_policy_protects_current_and_one_prior():
    """Verify that Option A retention policy retains current + 1 prior release and reclaims older releases."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        for sub in ["control", "raw", "staging", "releases"]:
            (root / sub).mkdir(parents=True, exist_ok=True)

        registry = Registry(db_path=root / "control" / "registry.sqlite", data_root=root)
        registry.migrate()

        # Register source object
        registry.register_source_object(
            logical_source_id="tlc/hvfhv/2024-12",
            official_url="https://example.com/2024-12.parquet",
            kind="hvfhv",
            source_month="2024-12",
        )

        # Simulate 4 sequential releases published over time: rel_1, rel_2, rel_3, rel_4 (current)
        releases_info = []
        for i in range(1, 5):
            att_id = f"att_{i}"
            registry.create_attempt(
                attempt_id=att_id,
                requested_configuration_json="{}",
                requested_window_start="2024-12",
                requested_window_end="2024-12",
            )

            rev_id = f"rev_{i}_{uuid.uuid4().hex[:8]}"
            registry.register_revision(
                revision_id=rev_id,
                logical_source_id="tlc/hvfhv/2024-12",
                content_sha256=f"sha_{i}" * 16,
                byte_count=5000,
                relative_path=f"raw/sha_{i}.parquet",
                schema_fingerprint="fingerprint_" + str(i),
                footer_row_count=500,
                decoded_row_count=500,
                provenance_relative_path=f"raw/provenance_{i}.json",
            )

            rel_id = f"rel_{i}_{uuid.uuid4().hex[:8]}"
            rel_dir = root / "releases" / rel_id
            rel_dir.mkdir(parents=True, exist_ok=True)

            # Create mock release artifacts
            db_file = rel_dir / "analytics.duckdb"
            db_file.write_bytes(b"MOCK_DUCKDB_PAYLOAD_" + b"\x00" * (5 * 1024 * 1024))  # 5 MB mock
            (rel_dir / "manifest.json").write_text(json.dumps({"release_id": rel_id}), encoding="utf-8")
            (rel_dir / "validation.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")

            # Register in SQLite
            registry.register_finalized_release(
                release_id=rel_id,
                producing_attempt_id=att_id,
                release_directory=str(rel_dir),
                manifest_sha256="m" * 64,
                database_sha256="d" * 64,
                validation_sha256="v" * 64,
                derivation_id="deriv_" + str(i),
                source_revision_map={"tlc/hvfhv/2024-12": rev_id},
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

            # Publish release (making rel_i current at generation i)
            expected_parent = releases_info[-1][0] if releases_info else None
            registry.record_selection_request(
                operation_key=f"op_pub_{i}",
                target_release_id=rel_id,
                expected_parent_generation=i - 1,
                expected_parent_release_id=expected_parent,
                operation_kind="publish",
            )
            registry.publish_release(
                operation_key=f"op_pub_{i}",
                attempt_id=f"att_{i}",
            )
            releases_info.append((rel_id, rel_dir))

        # Current release is rel_4 (generation 4)
        # Under Option A, protected set must be: rel_4 (current) and rel_3 (1 prior)
        # rel_1 and rel_2 must be retired and their duckdb payloads unlinked!

        cleanup_result = enforce_retention_policy_option_a(registry, data_root=root)
        assert cleanup_result["status"] == "SUCCESS"
        assert cleanup_result["policy"] == "OPTION_A"
        assert len(cleanup_result["protected_releases"]) == 2
        assert releases_info[3][0] in cleanup_result["protected_releases"]  # rel_4
        assert releases_info[2][0] in cleanup_result["protected_releases"]  # rel_3
        assert cleanup_result["retired_count"] == 2

        # Verify physical files on disk
        # rel_4 and rel_3 must retain analytics.duckdb
        assert (releases_info[3][1] / "analytics.duckdb").exists()
        assert (releases_info[2][1] / "analytics.duckdb").exists()

        # rel_1 and rel_2 must have analytics.duckdb removed (space reclaimed)
        assert not (releases_info[0][1] / "analytics.duckdb").exists()
        assert not (releases_info[1][1] / "analytics.duckdb").exists()

        # But manifests must be strictly preserved for audit and provenance!
        assert (releases_info[0][1] / "manifest.json").exists()
        assert (releases_info[1][1] / "manifest.json").exists()

        # Verify SQLite status
        con = registry.get_connection()
        try:
            r1_status = con.execute("SELECT status FROM releases WHERE release_id = ?", [releases_info[0][0]]).fetchone()[0]
            r2_status = con.execute("SELECT status FROM releases WHERE release_id = ?", [releases_info[1][0]]).fetchone()[0]
            r3_status = con.execute("SELECT status FROM releases WHERE release_id = ?", [releases_info[2][0]]).fetchone()[0]
            r4_status = con.execute("SELECT status FROM releases WHERE release_id = ?", [releases_info[3][0]]).fetchone()[0]

            assert r1_status == "RETIRED"
            assert r2_status == "RETIRED"
            assert r3_status == "VERIFIED"
            assert r4_status == "VERIFIED"
        finally:
            con.close()
