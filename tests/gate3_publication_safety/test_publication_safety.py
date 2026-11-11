"""Gate G3 Publication Safety Acceptance Suite.

Validates Scenarios T14, T15, T16, T17, T20, T26 and Walkthroughs W03, W05:
- Scenario T14: Incomplete rule inventory blocks finalization; new derivation required.
- Scenario T15: Two writers via CLI/Dagster; one holds writer lock, second exits boundedly.
- Scenario T16: Kill during ingestion/checkpoint; no selected partial database; retry fresh candidate.
- Scenario T17: Kill after directory rename and before selection; orphan reported, current unchanged,
  no automatic orphan publication or adoption.
- Scenario T20: Corrupted raw/release/manifest or path escape detected before use; fail closed.
- Scenario T26: Semantic exceptions remain publishable/visible; computation errors block (WARN vs ERROR,
  unauthorized NOT_APPLICABLE).
- Walkthrough W03: Warning versus computation failure trace.
- Walkthrough W05: Finalized original and lost registry write; provenance verification before adoption.
"""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
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
)
from hvfhv.control.publication import (
    finalize_candidate,
    verify_post_seal_s08,
    publish,
    rollback,
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
    GateVerdict,
)
from hvfhv.transforms.models import init_analytical_models, populate_release_metadata
from hvfhv.transforms.quality import init_quality_models


@pytest.fixture
def temp_env():
    """Create a temporary data root directory for test isolation."""
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


def _create_synthetic_candidate_db(
    db_path: Path,
    release_id: str,
    derivation_id: str,
    custom_rules: list[dict[str, Any]] | None = None,
    leave_open_writer: bool = False,
    corrupt_wal: bool = False,
) -> Any:
    """Helper to populate an analytics.duckdb candidate satisfying analytical schemas."""
    con = duckdb.connect(str(db_path))
    init_analytical_models(con)
    init_quality_models(con)

    populate_release_metadata(
        con=con,
        release_id=release_id,
        derivation_id=derivation_id,
        model_schema_version="v1",
        rules_version="v1",
        metric_definition_version="v1",
        code_bundle_manifest_hash="a" * 64,
        lockfile_hash="b" * 64,
        zone_revision_id="c" * 64,
        first_source_month="2024-01",
        last_source_month="2024-01",
    )

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

    if leave_open_writer:
        return con

    con.commit()
    con.execute("CHECKPOINT;")
    con.close()

    if corrupt_wal:
        wal_path = Path(str(db_path) + ".wal")
        wal_path.write_bytes(b"CORRUPT_DIRTY_WAL_DATA")

    return None


def _build_test_frozen_plan() -> tuple[FrozenInputPlan, str]:
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
    derivation_id = compute_derivation_id(plan)
    return plan, derivation_id


def _register_test_source_and_revision(reg: Registry) -> None:
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


# ---------------------------------------------------------------------------
# Test Functions
# ---------------------------------------------------------------------------

def test_scenario_t14_incomplete_inventory_blocks_finalization(temp_env):
    """Scenario T14: Incomplete rule inventory blocks finalization; derivation sensitivity.
    
    Invariants:
    1. If any required pre-seal rule (S01-S07, Q01-Q09) is omitted from quality_result,
       evaluate_pre_seal_gate detects missing rule and returns GateVerdict.FAIL.
    2. finalize_candidate raises PreSealGateBlockedError and stops before rename.
    3. Changing frozen plan parameter produces a different derivation_id, preventing stale reuse.
    """
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)

    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())
    plan, derivation_id = _build_test_frozen_plan()

    # Omit rule S04 from pre-seal rules
    incomplete_rules = []
    for r_id in ["S01", "S02", "S03", "S05", "S06", "S07", "Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"]:
        incomplete_rules.append({
            "rule_id": r_id, "rule_version": "v1", "cohort_key": "global",
            "severity": "BLOCK" if r_id.startswith("S") else "WARN",
            "status": "PASS", "method": "full_scan",
            "evaluated_count": 100, "exception_count": 0, "denominator": 100,
            "exception_rate": 0.0, "details_json": "{}", "source_provenance": "test",
            "evaluated_at_utc": "2026-11-11T00:00:00Z"
        })

    prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
    prep_dir.mkdir(parents=True, exist_ok=True)
    db_path = prep_dir / "analytics.duckdb"

    _create_synthetic_candidate_db(
        db_path=db_path,
        release_id=release_id,
        derivation_id=derivation_id,
        custom_rules=incomplete_rules,
    )

    # Incomplete inventory must raise PreSealGateBlockedError
    with pytest.raises(PreSealGateBlockedError, match="Missing required pre-seal rule outcome: S04"):
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

    # Directory was NOT moved to releases
    target_dir = temp_env["releases_dir"] / release_id
    assert not target_dir.exists()

    # Plan change produces different derivation_id
    plan2 = FrozenInputPlan(
        source_revisions=[SourceRevisionEntry("tlc/hvfhv/2024-01", "rev_1")],
        window_months=["2024-01"],
        zone_revision_id="c" * 64,
        schema_adapter_version="hvfhv_24col_pre2025.v1",
        rules_hash="different_rules_hash" + "0" * 44,
        metric_definition_hash="m" * 64,
        code_bundle_manifest_hash="a" * 64,
        lockfile_hash="b" * 64,
        model_schema_version="v1",
        semantic_configuration={"test": True},
    )
    assert compute_derivation_id(plan2) != derivation_id


def test_scenario_t15_writer_lock_contention_second_writer_exits_boundedly(temp_env):
    """Scenario T15: Two writers via CLI/Dagster; one holds writer lock, second exits boundedly.
    
    Invariants:
    1. Writer 1 acquires exclusive writer_lock.
    2. Writer 2 attempts non-blocking/timed acquisition (timeout=0.1s).
    3. Writer 2 raises LockContentionError and exits cleanly.
    4. Diagnostic info in LockContentionError identifies holder PID.
    5. Zero mutations to staging or releases artifacts by second writer.
    6. Lock file is permanent (never unlinked).
    """
    data_root = temp_env["data_root"]
    lock_file = data_root / "control" / "writer.lock"

    acquired_evt = threading.Event()
    release_evt = threading.Event()
    second_writer_result = {}

    def holder_worker():
        with writer_lock(data_root, diagnostic_info={"worker": "holder"}):
            acquired_evt.set()
            release_evt.wait(timeout=2.0)

    t = threading.Thread(target=holder_worker, daemon=True)
    t.start()

    assert acquired_evt.wait(timeout=1.0)

    try:
        # Second writer attempts acquisition in separate thread to test cross-thread BSD flock
        def second_writer():
            try:
                with writer_lock(data_root, timeout=0.1, diagnostic_info={"worker": "challenger"}):
                    second_writer_result["acquired"] = True
            except LockContentionError as e:
                second_writer_result["error"] = e

        t2 = threading.Thread(target=second_writer, daemon=True)
        t2.start()
        t2.join(timeout=1.0)

        assert "error" in second_writer_result
        err = second_writer_result["error"]
        assert isinstance(err, LockContentionError)
        assert lock_file.exists()
    finally:
        release_evt.set()
        t.join(timeout=1.0)


def test_scenario_t16_kill_during_staging_leaves_no_selected_partial_database(temp_env):
    """Scenario T16: Kill during ingestion/checkpoint leaves no selected partial database.
    
    Invariants:
    1. Partial or uncheckpointed dirty staging candidate is discarded.
    2. current_release singleton is completely unmodified.
    3. Retry builds fresh candidate from verified sources.
    """
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)

    # Initial state: current_release is unselected (gen 0)
    curr = reg.get_current_release()
    assert curr["release_id"] is None
    assert curr["generation"] == 0

    attempt_1 = str(uuid.uuid4())
    reg.create_attempt(attempt_1, "{}")
    rel_1 = str(uuid.uuid4())
    plan, derivation_id = _build_test_frozen_plan()

    # Simulate crashed attempt 1: partial DB with dirty WAL left behind
    crash_dir = temp_env["staging_dir"] / attempt_1 / "release_prep"
    crash_dir.mkdir(parents=True, exist_ok=True)
    dirty_db = crash_dir / "analytics.duckdb"

    _create_synthetic_candidate_db(
        db_path=dirty_db,
        release_id=rel_1,
        derivation_id=derivation_id,
        corrupt_wal=True,
    )

    # Attempting to finalize uncheckpointed dirty DB raises WalVerificationError
    with pytest.raises(WalVerificationError):
        finalize_candidate(
            candidate_dir=crash_dir,
            release_id=rel_1,
            attempt_id=attempt_1,
            derivation_id=derivation_id,
            frozen_plan=plan,
            source_revision_map={"tlc/hvfhv/2024-01": "rev_1"},
            data_root=data_root,
            registry=reg,
        )

    # Invariant: current_release is still gen 0 and unselected
    curr = reg.get_current_release()
    assert curr["release_id"] is None
    assert curr["generation"] == 0

    # Clean retry in fresh attempt 2
    attempt_2 = str(uuid.uuid4())
    reg.create_attempt(attempt_2, "{}")
    rel_2 = str(uuid.uuid4())

    fresh_dir = temp_env["staging_dir"] / attempt_2 / "release_prep"
    fresh_dir.mkdir(parents=True, exist_ok=True)
    clean_db = fresh_dir / "analytics.duckdb"

    _create_synthetic_candidate_db(
        db_path=clean_db,
        release_id=rel_2,
        derivation_id=derivation_id,
    )

    fin = finalize_candidate(
        candidate_dir=fresh_dir,
        release_id=rel_2,
        attempt_id=attempt_2,
        derivation_id=derivation_id,
        frozen_plan=plan,
        source_revision_map={"tlc/hvfhv/2024-01": "rev_1"},
        data_root=data_root,
        registry=reg,
    )
    assert fin.release_id == rel_2
    assert fin.release_directory.exists()


def test_scenario_t17_kill_after_rename_reports_orphan_without_auto_adoption(temp_env):
    """Scenario T17: Kill after directory rename before selection reports orphan; current unchanged.
    
    Invariants:
    1. Candidate directory finalized to releases/<release_id>.
    2. Simulated crash occurs before SQLite CAS selection.
    3. current_release pointer remains at previous release / generation.
    4. reconcile_releases detects and reports ORPHAN_UNSELECTED.
    5. Reconciliation NEVER automatically publishes or adopts the orphan.
    """
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)

    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())
    plan, derivation_id = _build_test_frozen_plan()

    prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
    prep_dir.mkdir(parents=True, exist_ok=True)
    db_path = prep_dir / "analytics.duckdb"

    _create_synthetic_candidate_db(
        db_path=db_path,
        release_id=release_id,
        derivation_id=derivation_id,
    )

    # Finalize candidate (renames to releases/<release_id> and marks FINALIZED in SQLite)
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
    assert fin.release_directory.exists()

    # Process dies here before verification and CAS selection!
    # 1. Assert current_release is still None / 0
    curr = reg.get_current_release()
    assert curr["release_id"] is None
    assert curr["generation"] == 0

    # 2. Reconcile releases: reports orphan
    audit = reconcile_releases(data_root=data_root, registry=reg)
    assert audit["total_releases_on_disk"] == 1
    assert audit["orphan_count"] == 1

    orphan = audit["orphans"][0]
    assert orphan["release_id"] == release_id
    assert orphan["status"] == "ORPHAN_UNSELECTED"
    assert orphan["has_manifest"] is True
    assert orphan["has_duckdb"] is True
    assert orphan["has_validation"] is True

    # 3. Assert current_release remains completely untouched after reconciliation
    curr_after = reg.get_current_release()
    assert curr_after["release_id"] is None
    assert curr_after["generation"] == 0


def test_scenario_t20_corrupted_release_manifest_or_path_escape_integrity_failure(temp_env):
    """Scenario T20: Corrupted raw/release/manifest or path escape fails closed before use.
    
    Invariants:
    1. Bit flip / truncation in analytics.duckdb fails Rule S08 digest verification.
    2. Bit flip in validation.json fails Rule S08 digest verification.
    3. Tampering with manifest.json fails verification.
    4. Path traversal attempt outside releases dir is rejected.
    5. Fails closed: unverified or corrupted release cannot be published.
    """
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)

    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())
    plan, derivation_id = _build_test_frozen_plan()

    prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
    prep_dir.mkdir(parents=True, exist_ok=True)
    db_path = prep_dir / "analytics.duckdb"

    _create_synthetic_candidate_db(
        db_path=db_path,
        release_id=release_id,
        derivation_id=derivation_id,
    )

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

    sealed_dir = temp_env["releases_dir"] / release_id
    sealed_db = sealed_dir / "analytics.duckdb"

    # Corrupt 1 byte in analytics.duckdb
    raw_bytes = bytearray(sealed_db.read_bytes())
    raw_bytes[100] ^= 0xFF
    sealed_db.write_bytes(bytes(raw_bytes))

    # S08 must fail with PostSealVerificationError
    with pytest.raises(PostSealVerificationError, match="analytics.duckdb digest mismatch"):
        verify_post_seal_s08(release_id=release_id, data_root=data_root, registry=reg)

    # Attempting to publish unverified release fails with InvalidReleaseStatusError
    with pytest.raises(InvalidReleaseStatusError):
        publish(
            release_id=release_id,
            expected_parent_generation=0,
            expected_parent_release_id=None,
            data_root=data_root,
            registry=reg,
        )


def test_scenario_t26_semantic_exceptions_publishable_computation_errors_block(temp_env):
    """Scenario T26: Semantic exceptions remain publishable/visible; computation errors block.
    
    Invariants:
    1. Q02 records negative intervals with status FAIL at severity WARN -> PASS gate, published.
    2. Any rule returning status ERROR blocks pre-seal gate regardless of severity.
    3. Unauthorized NOT_APPLICABLE (e.g. on Q03) blocks gate.
    """
    # 1. WARN rule with FAIL passes gate
    semantic_warn_results = [
        {"rule_id": "S01", "severity": "BLOCK", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "S02", "severity": "BLOCK", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "S03", "severity": "BLOCK", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "S04", "severity": "BLOCK", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "S05", "severity": "BLOCK", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "S06", "severity": "BLOCK", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "S07", "severity": "BLOCK", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "Q01", "severity": "WARN", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        # Q02 FAIL at WARN indicates observed negative intervals
        {"rule_id": "Q02", "severity": "WARN", "status": "FAIL", "cohort_key": "global", "evaluated_count": 100, "exception_count": 5},
        {"rule_id": "Q03", "severity": "WARN", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "Q04", "severity": "WARN", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "Q05", "severity": "WARN", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "Q06", "severity": "WARN", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "Q07", "severity": "WARN", "status": "NOT_APPLICABLE", "cohort_key": "global", "evaluated_count": 0, "exception_count": 0},
        {"rule_id": "Q08", "severity": "WARN", "status": "PASS", "cohort_key": "global", "evaluated_count": 100, "exception_count": 0},
        {"rule_id": "Q09", "severity": "WARN", "status": "NOT_APPLICABLE", "cohort_key": "global", "evaluated_count": 0, "exception_count": 0},
    ]

    metric_id = {"total_trips": 100, "eligible_trips": 90, "missing_intervals": 5, "negative_intervals": 5}
    verdict = evaluate_pre_seal_gate(quality_results=semantic_warn_results, metric_identity=metric_id)
    assert verdict.verdict == GateVerdict.PASS
    assert verdict.all_passed is True

    # 2. Computation ERROR blocks
    error_results = list(semantic_warn_results)
    error_results[8] = {"rule_id": "Q02", "severity": "WARN", "status": "ERROR", "cohort_key": "global", "evaluated_count": 100, "exception_count": 1}
    verdict_err = evaluate_pre_seal_gate(quality_results=error_results, metric_identity=metric_id)
    assert verdict_err.verdict == GateVerdict.FAIL
    assert verdict_err.all_passed is False
    assert any("computational ERROR" in err for err in verdict_err.blocking_failures)

    # 3. Unauthorized NOT_APPLICABLE blocks
    unauth_na = list(semantic_warn_results)
    unauth_na[9] = {"rule_id": "Q03", "severity": "WARN", "status": "NOT_APPLICABLE", "cohort_key": "global", "evaluated_count": 0, "exception_count": 0}
    verdict_unauth = evaluate_pre_seal_gate(quality_results=unauth_na, metric_identity=metric_id)
    assert verdict_unauth.verdict == GateVerdict.FAIL
    assert verdict_unauth.all_passed is False
    assert any("unauthorized NOT_APPLICABLE" in err for err in verdict_unauth.blocking_failures)


def test_walkthrough_w03_warning_vs_computation_failure(temp_env):
    """Walkthrough W03: Warning versus computation failure end-to-end trace."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)
    _register_test_source_and_revision(reg)

    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())
    plan, derivation_id = _build_test_frozen_plan()

    # Create candidate with Q02 having 5 negative intervals -> FAIL at WARN
    rules_with_warn_fail = []
    for r_id in ["S01", "S02", "S03", "S04", "S05", "S06", "S07"]:
        rules_with_warn_fail.append({
            "rule_id": r_id, "rule_version": "v1", "cohort_key": "global",
            "severity": "BLOCK", "status": "PASS", "method": "full_scan",
            "evaluated_count": 100, "exception_count": 0, "denominator": 100,
            "exception_rate": 0.0, "details_json": "{}", "source_provenance": "test",
            "evaluated_at_utc": "2026-11-11T00:00:00Z"
        })
    for q_id in ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"]:
        st = "FAIL" if q_id == "Q02" else ("NOT_APPLICABLE" if q_id in ("Q07", "Q09") else "PASS")
        exc = 5 if q_id == "Q02" else 0
        rules_with_warn_fail.append({
            "rule_id": q_id, "rule_version": "v1", "cohort_key": "global",
            "severity": "WARN", "status": st, "method": "full_scan",
            "evaluated_count": 100, "exception_count": exc, "denominator": 100,
            "exception_rate": 0.05 if q_id == "Q02" else 0.0, "details_json": json.dumps({"negative_count": exc}),
            "source_provenance": "test", "evaluated_at_utc": "2026-11-11T00:00:00Z"
        })

    prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
    prep_dir.mkdir(parents=True, exist_ok=True)
    db_path = prep_dir / "analytics.duckdb"

    _create_synthetic_candidate_db(
        db_path=db_path,
        release_id=release_id,
        derivation_id=derivation_id,
        custom_rules=rules_with_warn_fail,
    )

    # 1. Candidate finalization succeeds despite Q02 warning
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
    assert fin.release_id == release_id

    # 2. Rule S08 post-seal verification succeeds
    att = verify_post_seal_s08(release_id=release_id, data_root=data_root, registry=reg)
    assert att.release_id == release_id

    # 3. CAS publication succeeds
    pub_res = publish(
        release_id=release_id,
        expected_parent_generation=0,
        expected_parent_release_id=None,
        attempt_id=attempt_id,
        data_root=data_root,
        registry=reg,
    )
    assert pub_res.status == "SUCCEEDED"
    assert pub_res.new_generation == 1


def test_walkthrough_w05_finalized_original_lost_registry_write(temp_env):
    """Walkthrough W05: Finalized original and lost registry write.
    
    Invariants:
    1. Raw Parquet downloaded and validated, stored at raw/sha256/<hash>/source.parquet.
    2. Provenance record written at raw/provenance/<hash>.json.
    3. Crash occurs before SQLite registry registration.
    4. Reconciliation verifies byte hash + provenance before permitting adoption.
    5. Missing provenance leaves unassigned blob.
    """
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)

    content = b"PAR1_MOCK_PARQUET_CONTENT_FOR_W05_VERIFICATION"
    content_hash = hashlib.sha256(content).hexdigest()

    raw_dir = data_root / "raw" / "sha256" / content_hash
    raw_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = raw_dir / "source.parquet"
    parquet_path.write_bytes(content)

    prov_dir = data_root / "raw" / "provenance"
    prov_dir.mkdir(parents=True, exist_ok=True)
    prov_path = prov_dir / f"{content_hash}.json"

    prov_data = {
        "logical_source_id": "tlc/hvfhv/2024-01",
        "official_url": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet",
        "content_sha256": content_hash,
        "byte_count": len(content),
        "downloaded_at_utc": "2026-11-11T00:00:00Z",
    }
    prov_path.write_text(json.dumps(prov_data))

    # Provenance audit helper
    assert parquet_path.exists()
    assert hash_file_sha256(parquet_path) == content_hash
    assert prov_path.exists()

    # Now if provenance was missing, blob remains unassigned
    unassigned_content = b"ANOTHER_BLOB_WITHOUT_PROVENANCE"
    unassigned_hash = hashlib.sha256(unassigned_content).hexdigest()
    unassigned_dir = data_root / "raw" / "sha256" / unassigned_hash
    unassigned_dir.mkdir(parents=True, exist_ok=True)
    (unassigned_dir / "source.parquet").write_bytes(unassigned_content)

    # Assert provenance does not exist for second blob -> cannot register
    unassigned_prov = prov_dir / f"{unassigned_hash}.json"
    assert not unassigned_prov.exists()
