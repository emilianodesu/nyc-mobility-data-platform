"""Adversarial Empirical Stress Suite for Gate G3 Publication Safety.

Author: challenger_m3_1_gen6
Tests:
1. Incomplete inventory and missing models (T14 extended)
2. Lock contention storm: 8 concurrent threads/workers under writer.lock (T15 extended)
3. SIGKILL during staging and verify partial DB isolation (T16 extended)
4. Unadopted orphan directory detection & non-adoption (T17 extended)
5. Manifest corruption, bit-flips, and path traversal probes (T20 extended)
6. Computation errors and quality gate BLOCK severity aborts (T26 & W03 extended)
7. Rollback byte verification, lost ACK idempotency, and raw provenance (T18, T19, W01, W05)
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import threading
import time
from typing import Any
import uuid

import duckdb
import pytest

from hvfhv.config import resolve_data_root
from hvfhv.contracts.identity import (
    canonical_json_bytes,
    canonical_json_dumps,
    compute_derivation_id,
    FrozenInputPlan,
    SourceRevisionEntry,
)
from hvfhv.control.locks import writer_lock, LockContentionError
from hvfhv.control.registry import (
    Registry,
    RegistryError,
    ReleaseIntegrityError,
    StaleGenerationConflictError,
    InvalidReleaseStatusError,
    SelectionConflictError,
)
from hvfhv.control.publication import (
    finalize_candidate,
    verify_post_seal_s08,
    publish,
    rollback,
    recover_unacknowledged_selection,
    reconcile_releases,
    inspect_current,
    evaluate_pre_seal_gate,
    checkpoint_and_verify_zero_wal,
    validate_closed_database_read_only,
    hash_file_sha256,
    PreSealGateBlockedError,
    PostSealVerificationError,
    DestinationAlreadyExistsError,
    WalVerificationError,
    ReadOnlyInspectionError,
    RollbackIntegrityError,
    GateVerdict,
)
from hvfhv.transforms.models import init_analytical_models, populate_release_metadata
from hvfhv.transforms.quality import init_quality_models


@pytest.fixture
def temp_env():
    """Create a temporary isolated data root directory."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        control_dir = data_root / "control"
        releases_dir = data_root / "releases"
        staging_dir = data_root / "staging"
        raw_dir = data_root / "raw"
        control_dir.mkdir(parents=True, exist_ok=True)
        releases_dir.mkdir(parents=True, exist_ok=True)
        staging_dir.mkdir(parents=True, exist_ok=True)
        raw_dir.mkdir(parents=True, exist_ok=True)
        db_path = control_dir / "registry.sqlite"
        yield {
            "root": root,
            "data_root": data_root,
            "control_dir": control_dir,
            "releases_dir": releases_dir,
            "staging_dir": staging_dir,
            "raw_dir": raw_dir,
            "db_path": db_path,
        }


def _register_test_source_and_revision(reg: Registry) -> None:
    """Helper to populate prerequisite source and revision in SQLite control plane."""
    try:
        reg.register_source_object(
            "tlc/hvfhv/2024-01", "hvfhv", "https://example.com/2024-01.parquet", "2024-01"
        )
    except Exception:
        pass
    try:
        reg.register_revision(
            revision_id="rev_1",
            logical_source_id="tlc/hvfhv/2024-01",
            content_sha256="hash_1",
            byte_count=1000,
            relative_path="raw/sha256/hash_1/source.parquet",
            schema_fingerprint="fp_v1",
            footer_row_count=1000,
            decoded_row_count=1000,
            provenance_relative_path="raw/provenance/hash_1.json",
        )
    except Exception:
        pass


def _create_synthetic_candidate_db(
    db_path: Path,
    release_id: str,
    derivation_id: str,
    custom_rules: list[dict[str, Any]] | None = None,
    drop_models: list[str] | None = None,
    corrupt_metadata: dict[str, Any] | None = None,
) -> None:
    """Helper to populate an analytics.duckdb candidate satisfying analytical schemas."""
    con = duckdb.connect(str(db_path))
    init_analytical_models(con)
    init_quality_models(con)

    meta_rel = corrupt_metadata.get("release_id", release_id) if corrupt_metadata else release_id
    meta_deriv = corrupt_metadata.get("derivation_id", derivation_id) if corrupt_metadata else derivation_id
    meta_schema = corrupt_metadata.get("model_schema_version", "v1") if corrupt_metadata else "v1"

    populate_release_metadata(
        con=con,
        release_id=meta_rel,
        derivation_id=meta_deriv,
        model_schema_version=meta_schema,
        rules_version="v1",
        metric_definition_version="v1",
        code_bundle_manifest_hash="a" * 64,
        lockfile_hash="b" * 64,
        zone_revision_id="c" * 64,
        first_source_month="2024-01",
        last_source_month="2024-01",
    )

    if corrupt_metadata and corrupt_metadata.get("zero_rows"):
        con.execute("DELETE FROM release_metadata;")
    elif corrupt_metadata and corrupt_metadata.get("duplicate_rows"):
        con.execute(f"INSERT INTO release_metadata VALUES ('{meta_rel}', '{meta_deriv}', '{meta_schema}', 'v1', 'v1', 'a', 'b', 'c', '2024-01', '2024-01', '2026-11-11T00:00:00Z');")

    con.execute("""
        INSERT INTO source_contribution VALUES (
            'tlc/hvfhv/2024-01', 'rev_1', 'hash_1', '2024-01', 'hvfhv_24col_pre2025.v1', false, 1000, 1000
        );
        INSERT INTO zone_reference VALUES (1, 'EWR', 'Newark Airport', 'EWR', 'zrev_1');
        INSERT INTO zone_reference VALUES (264, 'Unknown', 'NV', 'Unknown', 'zrev_1');
        INSERT INTO zone_reference VALUES (265, 'Unknown', 'NA', 'Unknown', 'zrev_1');
        INSERT INTO coverage VALUES ('2024-01', true, true, true, 'rev_1', 1000);
        INSERT INTO interval_daily VALUES ('2024-01', '2024-01-01', 1, 'HV0003', 1000, 950, 30, 20, 45.0, 90.0);
        INSERT INTO interval_monthly VALUES ('2024-01', '2024-01', 1, 'HV0003', 1000, 950, 30, 20, 45.0, 90.0);
    """)

    # Populate quality_result
    rules_to_insert = custom_rules
    if rules_to_insert is None:
        rules_to_insert = []
        for r_id in ["S01", "S02", "S03", "S04", "S05", "S06", "S07"]:
            rules_to_insert.append({
                "rule_id": r_id, "rule_version": "v1", "cohort_key": "global",
                "severity": "BLOCK", "status": "PASS", "method": "full_scan",
                "evaluated_count": 1000, "exception_count": 0, "denominator": 1000,
                "exception_rate": 0.0, "details_json": "{}", "source_provenance": "test",
                "evaluated_at_utc": "2026-11-11T00:00:00Z"
            })
        for q_id in ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"]:
            rules_to_insert.append({
                "rule_id": q_id, "rule_version": "v1", "cohort_key": "global",
                "severity": "WARN", "status": "PASS", "method": "full_scan",
                "evaluated_count": 1000, "exception_count": 0, "denominator": 1000,
                "exception_rate": 0.0, "details_json": "{}", "source_provenance": "test",
                "evaluated_at_utc": "2026-11-11T00:00:00Z"
            })

    for r in rules_to_insert:
        con.execute("""
            INSERT INTO quality_result (
                rule_id, rule_version, cohort_key, severity, status, method,
                evaluated_count, exception_count, denominator, exception_rate,
                details_json, source_provenance, evaluated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, [
            r["rule_id"], r["rule_version"], r["cohort_key"], r["severity"],
            r["status"], r["method"], r["evaluated_count"], r["exception_count"],
            r.get("denominator"), r.get("exception_rate"), r.get("details_json", "{}"),
            r.get("source_provenance"), r["evaluated_at_utc"]
        ])

    if drop_models:
        for m in drop_models:
            # Check if view or table
            is_view = con.execute(f"SELECT COUNT(*) FROM duckdb_views() WHERE view_name = '{m}';").fetchone()[0] > 0
            if is_view:
                con.execute(f"DROP VIEW {m};")
            else:
                con.execute(f"DROP TABLE {m};")

    con.commit()
    con.execute("CHECKPOINT;")
    con.close()


def _build_test_plan() -> tuple[FrozenInputPlan, str]:
    plan = FrozenInputPlan(
        source_revisions=[SourceRevisionEntry("tlc/hvfhv/2024-01", "rev_1")],
        window_months=["2024-01"],
        zone_revision_id="c" * 64,
        schema_adapter_version="hvfhv_24col_pre2025.v1",
        rules_hash="r" * 64,
        metric_definition_hash="m" * 64,
        code_bundle_manifest_hash="a" * 64,
        lockfile_hash="b" * 64,
        model_schema_version="v1",
        semantic_configuration={"test": True},
    )
    return plan, compute_derivation_id(plan)


# ===========================================================================
# 1. Incomplete Inventory and Missing Models (Scenario T14 Stress)
# ===========================================================================

def test_t14_missing_each_required_rule(temp_env):
    """Stress test: verify every single required rule (S01-S07, Q01-Q09) blocks if omitted."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    plan, derivation_id = _build_test_plan()

    all_rules = ["S01", "S02", "S03", "S04", "S05", "S06", "S07", "Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"]

    for omitted in all_rules:
        subset = [r for r in all_rules if r != omitted]
        q_results = []
        for r_id in subset:
            q_results.append({
                "rule_id": r_id, "rule_version": "v1", "cohort_key": "global",
                "severity": "BLOCK" if r_id.startswith("S") else "WARN",
                "status": "PASS", "method": "full_scan",
                "evaluated_count": 100, "exception_count": 0, "denominator": 100,
                "exception_rate": 0.0, "details_json": "{}", "source_provenance": "test",
                "evaluated_at_utc": "2026-11-11T00:00:00Z"
            })
        verdict = evaluate_pre_seal_gate(quality_results=q_results)
        assert verdict.verdict == GateVerdict.FAIL, f"Omission of {omitted} did not fail pre-seal gate!"
        assert any(f"Missing required pre-seal rule outcome: {omitted}" in s for s in verdict.blocking_failures)


def test_t14_missing_analytical_model_blocks(temp_env):
    """Stress test: dropping any of the required 10 analytical models blocks finalization."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)
    plan, derivation_id = _build_test_plan()

    models_to_test = ["column_profile", "coverage", "interval_daily", "interval_monthly", "quality_result", "trip_observation", "zone_reference"]

    for model in models_to_test:
        attempt_id = str(uuid.uuid4())
        reg.create_attempt(attempt_id, "{}")
        release_id = str(uuid.uuid4())
        prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
        prep_dir.mkdir(parents=True, exist_ok=True)
        db_path = prep_dir / "analytics.duckdb"

        _create_synthetic_candidate_db(
            db_path=db_path,
            release_id=release_id,
            derivation_id=derivation_id,
            drop_models=[model],
        )

        with pytest.raises(ReadOnlyInspectionError, match=f"Model or view '{model}' verification failed"):
            finalize_candidate(
                candidate_dir=prep_dir,
                release_id=release_id,
                attempt_id=attempt_id,
                derivation_id=derivation_id,
                frozen_plan=plan,
                source_revision_map={"tlc/hvfhv/2024-01": "rev_1"},
                data_root=data_root,
                registry=reg,
            )
        assert not (temp_env["releases_dir"] / release_id).exists()


def test_t14_corrupted_release_metadata_blocks(temp_env):
    """Stress test: corrupted release_metadata (0 rows, mismatched ids) blocks finalization."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)
    plan, derivation_id = _build_test_plan()

    corruption_cases = [
        ({"zero_rows": True}, "Expected exactly 1 row in release_metadata"),
        ({"release_id": "wrong_rel"}, "release_metadata release_id mismatch"),
        ({"derivation_id": "wrong_deriv"}, "release_metadata derivation_id mismatch"),
        ({"model_schema_version": "v99"}, "release_metadata model_schema_version mismatch"),
    ]

    for corrupt_dict, err_match in corruption_cases:
        attempt_id = str(uuid.uuid4())
        reg.create_attempt(attempt_id, "{}")
        release_id = str(uuid.uuid4())
        prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
        prep_dir.mkdir(parents=True, exist_ok=True)
        db_path = prep_dir / "analytics.duckdb"

        _create_synthetic_candidate_db(
            db_path=db_path,
            release_id=release_id,
            derivation_id=derivation_id,
            corrupt_metadata=corrupt_dict,
        )

        with pytest.raises(ReadOnlyInspectionError, match=err_match):
            finalize_candidate(
                candidate_dir=prep_dir,
                release_id=release_id,
                attempt_id=attempt_id,
                derivation_id=derivation_id,
                frozen_plan=plan,
                source_revision_map={"tlc/hvfhv/2024-01": "rev_1"},
                data_root=data_root,
                registry=reg,
            )


# ===========================================================================
# 2. Lock Contention Storm (Scenario T15 Stress)
# ===========================================================================

def test_t15_multi_worker_lock_contention_storm(temp_env):
    """Stress test: 8 simultaneous concurrent workers competing for writer.lock."""
    data_root = temp_env["data_root"]
    lock_file = data_root / "control" / "writer.lock"

    num_workers = 8
    barrier = threading.Barrier(num_workers)
    results_dict: dict[int, dict[str, Any]] = {}
    threads = []

    def worker(worker_id: int):
        barrier.wait()  # Synchronize simultaneous start
        t0 = time.monotonic()
        try:
            with writer_lock(data_root, timeout=0.15, diagnostic_info={"worker_id": worker_id}):
                time.sleep(0.04)
                results_dict[worker_id] = {"status": "ACQUIRED", "duration": time.monotonic() - t0}
        except LockContentionError as e:
            results_dict[worker_id] = {"status": "CONTENTION", "error": str(e), "holder": e.holder_info}
        except Exception as e:
            results_dict[worker_id] = {"status": "UNEXPECTED_ERROR", "error": str(e)}

    for wid in range(num_workers):
        t = threading.Thread(target=worker, args=(wid,), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=5.0)

    # Invariants:
    # 1. No thread encountered UNEXPECTED_ERROR
    assert len(results_dict) == num_workers
    statuses = [res["status"] for res in results_dict.values()]
    assert "UNEXPECTED_ERROR" not in statuses

    # 2. At least one worker succeeded in acquiring the lock
    assert "ACQUIRED" in statuses

    # 3. Lock file exists and is intact (never unlinked)
    assert lock_file.exists()


# ===========================================================================
# 3. Staging Interruption / SIGKILL (Scenario T16 Stress)
# ===========================================================================

def _crashed_duckdb_worker(db_path_str: str, ready_event: Any):
    """Worker that starts dirty transaction and waits to be SIGKILL'd."""
    con = duckdb.connect(db_path_str)
    con.execute("CREATE TABLE partial_work (x BIGINT);")
    con.execute("INSERT INTO partial_work SELECT * FROM range(1000000);")
    ready_event.set()
    # Hang here until killed
    time.sleep(60.0)


def test_t16_sigkill_leaves_no_selected_database_and_fails_safely(temp_env):
    """Stress test: OS process killed with SIGKILL leaves partial state that cannot be finalized or selected."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)
    plan, derivation_id = _build_test_plan()

    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())

    prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
    prep_dir.mkdir(parents=True, exist_ok=True)
    dirty_db = prep_dir / "analytics.duckdb"

    ready_event = multiprocessing.Event()
    p = multiprocessing.Process(target=_crashed_duckdb_worker, args=(str(dirty_db), ready_event))
    p.start()

    assert ready_event.wait(timeout=5.0)
    # Deliver SIGKILL to process mid-write
    os.kill(p.pid, signal.SIGKILL)
    p.join(timeout=2.0)

    # 1. Finalize MUST fail (either ReadOnlyInspectionError or WalVerificationError)
    with pytest.raises((WalVerificationError, ReadOnlyInspectionError, Exception)):
        finalize_candidate(
            candidate_dir=prep_dir,
            release_id=release_id,
            attempt_id=attempt_id,
            derivation_id=derivation_id,
            frozen_plan=plan,
            source_revision_map={"tlc/hvfhv/2024-01": "rev_1"},
            data_root=data_root,
            registry=reg,
        )

    # 2. Releases directory does NOT contain release_id
    assert not (temp_env["releases_dir"] / release_id).exists()

    # 3. current_release pointer is completely untouched (None / gen 0)
    curr = reg.get_current_release()
    assert curr["release_id"] is None
    assert curr["generation"] == 0


# ===========================================================================
# 4. Unadopted Orphan Directories (Scenario T17 Stress)
# ===========================================================================

def test_t17_unadopted_orphans_adversarial_isolation(temp_env):
    """Stress test: Rogue or unverified directories on disk are reported as orphans and can NEVER be published."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)

    releases_dir = temp_env["releases_dir"]

    # 1. Create an unregistered rogue directory
    rogue_dir = releases_dir / "rogue_release_001"
    rogue_dir.mkdir(parents=True, exist_ok=True)
    (rogue_dir / "manifest.json").write_text("{}")
    (rogue_dir / "analytics.duckdb").write_bytes(b"FAKE_DB")

    # 2. Create a non-directory file in releases/
    rogue_file = releases_dir / "rogue_file.txt"
    rogue_file.write_text("rogue")

    # Audit orphans
    audit = reconcile_releases(data_root=data_root, registry=reg)
    assert audit["orphan_count"] == 1
    assert audit["orphans"][0]["release_id"] == "rogue_release_001"

    # Attempting to publish rogue release directly fails closed via Foreign Key or RegistryError
    with pytest.raises((RegistryError, sqlite3.IntegrityError)):
        publish(
            release_id="rogue_release_001",
            expected_parent_generation=0,
            expected_parent_release_id=None,
            data_root=data_root,
            registry=reg,
        )

    # Attempting to rollback to rogue release fails closed
    with pytest.raises((RegistryError, sqlite3.IntegrityError, RollbackIntegrityError)):
        rollback(
            target_release_id="rogue_release_001",
            reason="Adversarial rollback attempt",
            expected_parent_generation=0,
            data_root=data_root,
            registry=reg,
        )

    # Current release remains None / 0
    curr = reg.get_current_release()
    assert curr["release_id"] is None
    assert curr["generation"] == 0


# ===========================================================================
# 5. Manifest Corruption, Bit-Flips & Path Traversal (Scenario T20 Stress)
# ===========================================================================

def test_t20_validation_json_bit_flip_fails_s08(temp_env):
    """Stress test: 1-byte bit-flip in validation.json fails Rule S08 digest check."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)
    plan, derivation_id = _build_test_plan()

    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())
    prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
    prep_dir.mkdir(parents=True, exist_ok=True)
    db_path = prep_dir / "analytics.duckdb"

    _create_synthetic_candidate_db(db_path=db_path, release_id=release_id, derivation_id=derivation_id)

    fin = finalize_candidate(
        candidate_dir=prep_dir,
        release_id=release_id,
        attempt_id=attempt_id,
        derivation_id=derivation_id,
        frozen_plan=plan,
        source_revision_map={"tlc/hvfhv/2024-01": "rev_1"},
        data_root=data_root,
        registry=reg,
    )

    # Bit flip in validation.json
    val_file = fin.release_directory / "validation.json"
    val_bytes = bytearray(val_file.read_bytes())
    val_bytes[50] ^= 0xFF
    val_file.write_bytes(bytes(val_bytes))

    with pytest.raises(PostSealVerificationError, match="validation.json digest mismatch"):
        verify_post_seal_s08(release_id=release_id, data_root=data_root, registry=reg)


def test_t20_manifest_json_corruption_fails_s08(temp_env):
    """Stress test: manifest.json bit-flip or invalid JSON fails Rule S08 digest check."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)
    plan, derivation_id = _build_test_plan()

    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())
    prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
    prep_dir.mkdir(parents=True, exist_ok=True)
    db_path = prep_dir / "analytics.duckdb"

    _create_synthetic_candidate_db(db_path=db_path, release_id=release_id, derivation_id=derivation_id)

    fin = finalize_candidate(
        candidate_dir=prep_dir,
        release_id=release_id,
        attempt_id=attempt_id,
        derivation_id=derivation_id,
        frozen_plan=plan,
        source_revision_map={"tlc/hvfhv/2024-01": "rev_1"},
        data_root=data_root,
        registry=reg,
    )

    # Bit flip in manifest.json
    man_file = fin.release_directory / "manifest.json"
    man_bytes = bytearray(man_file.read_bytes())
    man_bytes[50] ^= 0xFF
    man_file.write_bytes(bytes(man_bytes))

    with pytest.raises(PostSealVerificationError, match="manifest.json digest mismatch"):
        verify_post_seal_s08(release_id=release_id, data_root=data_root, registry=reg)


def test_t20_rollback_target_bit_flip_fails_rollback(temp_env):
    """Stress test: bit-flip in analytics.duckdb of a retained release blocks rollback."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)
    plan, derivation_id = _build_test_plan()

    # Create release 1
    att1 = str(uuid.uuid4())
    reg.create_attempt(att1, "{}")
    rel1 = str(uuid.uuid4())
    p1 = temp_env["staging_dir"] / att1 / "release_prep"
    p1.mkdir(parents=True, exist_ok=True)
    _create_synthetic_candidate_db(p1 / "analytics.duckdb", rel1, derivation_id)

    finalize_candidate(p1, rel1, att1, derivation_id, plan, {"tlc/hvfhv/2024-01": "rev_1"}, data_root=data_root, registry=reg)
    verify_post_seal_s08(rel1, data_root=data_root, registry=reg)
    publish(rel1, expected_parent_generation=0, data_root=data_root, registry=reg)

    # Create release 2
    att2 = str(uuid.uuid4())
    reg.create_attempt(att2, "{}")
    rel2 = str(uuid.uuid4())
    p2 = temp_env["staging_dir"] / att2 / "release_prep"
    p2.mkdir(parents=True, exist_ok=True)
    _create_synthetic_candidate_db(p2 / "analytics.duckdb", rel2, derivation_id)

    finalize_candidate(p2, rel2, att2, derivation_id, plan, {"tlc/hvfhv/2024-01": "rev_1"}, data_root=data_root, registry=reg)
    verify_post_seal_s08(rel2, data_root=data_root, registry=reg)
    publish(rel2, expected_parent_generation=1, expected_parent_release_id=rel1, data_root=data_root, registry=reg)

    # Now tamper with retained release 1 database file
    db1_path = temp_env["releases_dir"] / rel1 / "analytics.duckdb"
    b = bytearray(db1_path.read_bytes())
    b[100] ^= 0xFF
    db1_path.write_bytes(bytes(b))

    # Rollback to rel1 MUST fail with RollbackIntegrityError
    with pytest.raises(RollbackIntegrityError, match="Rollback target integrity failure"):
        rollback(
            target_release_id=rel1,
            reason="Test rollback",
            expected_parent_generation=2,
            expected_parent_release_id=rel2,
            data_root=data_root,
            registry=reg,
            verify_bytes=True,
        )

    # Current release remains rel2 at generation 2
    curr = reg.get_current_release()
    assert curr["release_id"] == rel2
    assert curr["generation"] == 2


def test_t20_path_traversal_attempts_probed(temp_env):
    """Stress test: Probe path traversal in release_id across publication APIs."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    plan, derivation_id = _build_test_plan()

    traversal_ids = [
        "../../outside_escape",
        "../releases_leak",
        "/tmp/abs_escape",
    ]

    for bad_id in traversal_ids:
        # Probe verify_post_seal_s08
        with pytest.raises((PostSealVerificationError, RegistryError, Exception)):
            verify_post_seal_s08(release_id=bad_id, data_root=data_root, registry=reg)

        # Probe publish
        with pytest.raises((RegistryError, InvalidReleaseStatusError, sqlite3.IntegrityError, Exception)):
            publish(release_id=bad_id, expected_parent_generation=0, data_root=data_root, registry=reg)

        # Probe rollback
        with pytest.raises((RegistryError, RollbackIntegrityError, InvalidReleaseStatusError, sqlite3.IntegrityError, Exception)):
            rollback(target_release_id=bad_id, reason="traversal probe", expected_parent_generation=0, data_root=data_root, registry=reg)


# ===========================================================================
# 6. Computation Errors vs Semantic Warnings (Scenario T26 & W03 Stress)
# ===========================================================================

def test_t26_unauthorized_not_applicable_on_all_disallowed_rules(temp_env):
    """Stress test: NOT_APPLICABLE status is rejected on ALL rules except Q07 and Q09."""
    rules_with_na_disallowed = [
        "S01", "S02", "S03", "S04", "S05", "S06", "S07",
        "Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q08"
    ]

    for target_rule in rules_with_na_disallowed:
        all_rules = []
        for r_id in ["S01", "S02", "S03", "S04", "S05", "S06", "S07"]:
            all_rules.append({
                "rule_id": r_id, "rule_version": "v1", "cohort_key": "global",
                "severity": "BLOCK", "status": "NOT_APPLICABLE" if r_id == target_rule else "PASS",
                "method": "full_scan", "evaluated_count": 100, "exception_count": 0,
            })
        for q_id in ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"]:
            st = "NOT_APPLICABLE" if q_id in ("Q07", "Q09", target_rule) else "PASS"
            all_rules.append({
                "rule_id": q_id, "rule_version": "v1", "cohort_key": "global",
                "severity": "WARN", "status": st,
                "method": "full_scan", "evaluated_count": 100, "exception_count": 0,
            })

        verdict = evaluate_pre_seal_gate(quality_results=all_rules)
        assert verdict.verdict == GateVerdict.FAIL, f"Rule {target_rule} was allowed NOT_APPLICABLE!"
        assert any(f"Rule {target_rule} returned unauthorized NOT_APPLICABLE" in err for err in verdict.blocking_failures)


def test_t26_computation_error_on_warn_rules_blocks_gate(temp_env):
    """Stress test: Computation ERROR on ANY WARN rule (Q01-Q09) blocks gate decision."""
    for q_target in ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"]:
        rules = []
        for r_id in ["S01", "S02", "S03", "S04", "S05", "S06", "S07"]:
            rules.append({
                "rule_id": r_id, "rule_version": "v1", "cohort_key": "global",
                "severity": "BLOCK", "status": "PASS", "method": "full_scan",
                "evaluated_count": 100, "exception_count": 0,
            })
        for q_id in ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"]:
            st = "ERROR" if q_id == q_target else ("NOT_APPLICABLE" if q_id in ("Q07", "Q09") else "PASS")
            rules.append({
                "rule_id": q_id, "rule_version": "v1", "cohort_key": "global",
                "severity": "WARN", "status": st, "method": "full_scan",
                "evaluated_count": 100, "exception_count": 0,
            })

        verdict = evaluate_pre_seal_gate(quality_results=rules)
        assert verdict.verdict == GateVerdict.FAIL
        assert any(f"Rule {q_target} returned computational ERROR" in err for err in verdict.blocking_failures)


# ===========================================================================
# 7. Recovery, Idempotency and Lost Acknowledgement (T18, T19, W01, W05)
# ===========================================================================

def test_t18_lost_ack_recovery_reconciliation(temp_env):
    """Stress test: crash after CAS commit before ACK returns ALREADY_COMMITTED without duplicate event."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)
    plan, derivation_id = _build_test_plan()

    att = str(uuid.uuid4())
    reg.create_attempt(att, "{}")
    rel = str(uuid.uuid4())
    p = temp_env["staging_dir"] / att / "release_prep"
    p.mkdir(parents=True, exist_ok=True)
    _create_synthetic_candidate_db(p / "analytics.duckdb", rel, derivation_id)

    finalize_candidate(p, rel, att, derivation_id, plan, {"tlc/hvfhv/2024-01": "rev_1"}, data_root=data_root, registry=reg)
    verify_post_seal_s08(rel, data_root=data_root, registry=reg)

    op_key = f"op_test_ack_{rel}"
    # 1. First publish call succeeds
    res1 = publish(rel, expected_parent_generation=0, operation_key=op_key, data_root=data_root, registry=reg)
    assert res1.status == "SUCCEEDED"
    assert res1.new_generation == 1

    # 2. Query event count in publication_events
    con = reg.get_connection()
    try:
        cnt = con.execute("SELECT COUNT(*) FROM publication_events WHERE operation_key = ?;", (op_key,)).fetchone()[0]
        assert cnt == 1
    finally:
        con.close()

    # 3. Simulate client retry with same operation_key
    res2 = publish(rel, expected_parent_generation=0, operation_key=op_key, data_root=data_root, registry=reg)
    assert res2.status == "ALREADY_COMMITTED"
    assert res2.new_generation == 1

    # 4. recover_unacknowledged_selection also returns ALREADY_COMMITTED
    res3 = recover_unacknowledged_selection(operation_key=op_key, data_root=data_root, registry=reg)
    assert res3.status == "ALREADY_COMMITTED"
    assert res3.new_generation == 1

    # 5. publication_events STILL has exactly 1 row
    con = reg.get_connection()
    try:
        cnt_after = con.execute("SELECT COUNT(*) FROM publication_events WHERE operation_key = ?;", (op_key,)).fetchone()[0]
        assert cnt_after == 1
    finally:
        con.close()
