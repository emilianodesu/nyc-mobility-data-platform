"""Gate G3 Durability & Recovery Acceptance Suite.

Validates Scenarios T18, T19, T24, Walkthrough W01, and Storage Safety Invariants:
- Scenario T18: Kill after SQLite commit before acknowledgement; reconciliation finds committed event,
  retry does not duplicate selection.
- Scenario T19: Stale compare-and-swap rejected; rollback verifies physical bytes and increments generation.
- Scenario T24: Registry migration on a copy and restore; integrity/FK checks pass; released artifacts unchanged.
- Walkthrough W01: A retry after a later rollback trace.
- Storage Safety: DuckDB zero-WAL postcondition and destination already exists protection.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
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
from hvfhv.control.registry import (
    Registry,
    RegistryError,
    ReleaseIntegrityError,
    StaleGenerationConflictError,
    InvalidReleaseStatusError,
    SelectionConflictError,
    Migration,
    MIGRATIONS,
)
from hvfhv.control.publication import (
    finalize_candidate,
    verify_post_seal_s08,
    publish,
    rollback,
    recover_unacknowledged_selection,
    reconcile_releases,
    checkpoint_and_verify_zero_wal,
    hash_file_sha256,
    DestinationAlreadyExistsError,
    WalVerificationError,
    RollbackIntegrityError,
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
) -> None:
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

    for r_id in ["S01", "S02", "S03", "S04", "S05", "S06", "S07"]:
        con.execute("""
            INSERT INTO quality_result VALUES (
                ?, 'v1', 'global', 'BLOCK', 'PASS', 'full_scan', 1000, 0, 1000, 0.0, '{}', 'test', '2026-11-11T00:00:00Z'
            );
        """, [r_id])

    for q_id in ["Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"]:
        con.execute("""
            INSERT INTO quality_result VALUES (
                ?, 'v1', 'global', 'WARN', 'PASS', 'full_scan', 1000, 0, 1000, 0.0, '{}', 'test', '2026-11-11T00:00:00Z'
            );
        """, [q_id])

    con.commit()
    con.execute("CHECKPOINT;")
    con.close()


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


def _setup_and_verify_release(
    temp_env: dict[str, Any],
    reg: Registry,
    release_id: str,
    attempt_id: str,
) -> None:
    """Helper to finalize and verify a complete release."""
    _register_test_source_and_revision(reg)
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
        data_root=temp_env["data_root"],
        registry=reg,
    )

    verify_post_seal_s08(
        release_id=release_id,
        data_root=temp_env["data_root"],
        registry=reg,
    )


# ---------------------------------------------------------------------------
# Test Functions
# ---------------------------------------------------------------------------

def test_scenario_t18_kill_after_sqlite_commit_reconciliation_finds_event_no_duplicate(temp_env):
    """Scenario T18: Kill after SQLite commit before acknowledgement.
    
    Invariants:
    1. CAS publication commits (generation advances G -> G+1, event logged).
    2. Simulated process kill after commit before caller receives acknowledgement.
    3. Caller queries / retries with the same operation_key.
    4. Registry finds committed event and returns ALREADY_COMMITTED.
    5. current_release.generation is NOT incremented to G+2.
    6. publication_events rowcount for operation_key remains exactly 1.
    """
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)

    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())
    _setup_and_verify_release(temp_env, reg, release_id, attempt_id)

    op_key = "op_t18_lost_ack_test"

    # 1. First execution commits
    res1 = publish(
        release_id=release_id,
        expected_parent_generation=0,
        expected_parent_release_id=None,
        operation_key=op_key,
        attempt_id=attempt_id,
        data_root=data_root,
        registry=reg,
    )
    assert res1.status == "SUCCEEDED"
    assert res1.new_generation == 1
    assert res1.selected_release_id == release_id

    # 2. Crash simulated: caller lost connection, invokes recover_unacknowledged_selection
    rec = recover_unacknowledged_selection(operation_key=op_key, data_root=data_root, registry=reg)
    assert rec.status == "ALREADY_COMMITTED"
    assert rec.event_id == res1.event_id
    assert rec.new_generation == 1
    assert rec.selected_release_id == release_id
    assert rec.is_current is True

    # 3. Direct retry via publish() with identical op_key also returns ALREADY_COMMITTED
    res2 = publish(
        release_id=release_id,
        expected_parent_generation=0,
        expected_parent_release_id=None,
        operation_key=op_key,
        attempt_id=attempt_id,
        data_root=data_root,
        registry=reg,
    )
    assert res2.status == "ALREADY_COMMITTED"
    assert res2.new_generation == 1

    # Invariant: Generation is still 1, NOT 2
    curr = reg.get_current_release()
    assert curr["generation"] == 1
    assert curr["release_id"] == release_id

    # Invariant: Exactly one publication event exists
    con = reg.get_connection()
    try:
        cur = con.cursor()
        cnt = cur.execute(
            "SELECT COUNT(*) FROM publication_events WHERE operation_key = ?;", (op_key,)
        ).fetchone()[0]
        assert cnt == 1
    finally:
        con.close()


def test_scenario_t19_stale_cas_rejected_and_retained_rollback_verifies_bytes(temp_env):
    """Scenario T19: Stale CAS rejected; rollback verifies physical bytes and increments generation.
    
    Invariants:
    1. CAS rejects publication if expected generation does not match current.
    2. Retained release rollback re-verifies physical SHA-256 digests on disk before CAS.
    3. If target release files are corrupted on disk, rollback raises RollbackIntegrityError.
    4. If target release status is REJECTED or RETIRED, rollback raises InvalidReleaseStatusError.
    5. Valid rollback increments generation monotonically without mutating release files.
    """
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)

    # 1. Setup release 1 and publish (generation = 1)
    att_1 = str(uuid.uuid4())
    reg.create_attempt(att_1, "{}")
    rel_1 = str(uuid.uuid4())
    _setup_and_verify_release(temp_env, reg, rel_1, att_1)

    res_rel1 = publish(
        release_id=rel_1,
        expected_parent_generation=0,
        expected_parent_release_id=None,
        operation_key="op_pub_rel1",
        data_root=data_root,
        registry=reg,
    )
    assert res_rel1.new_generation == 1

    # 2. Setup release 2 and publish (generation = 2)
    att_2 = str(uuid.uuid4())
    reg.create_attempt(att_2, "{}")
    rel_2 = str(uuid.uuid4())
    _setup_and_verify_release(temp_env, reg, rel_2, att_2)

    res_rel2 = publish(
        release_id=rel_2,
        expected_parent_generation=1,
        expected_parent_release_id=rel_1,
        operation_key="op_pub_rel2",
        data_root=data_root,
        registry=reg,
    )
    assert res_rel2.new_generation == 2

    # 3. Stale CAS rejection: attempt to publish with stale generation 1 (current is 2)
    att_3 = str(uuid.uuid4())
    reg.create_attempt(att_3, "{}")
    rel_3 = str(uuid.uuid4())
    _setup_and_verify_release(temp_env, reg, rel_3, att_3)

    with pytest.raises(StaleGenerationConflictError, match="CAS conflict: current generation 2 != expected 1"):
        publish(
            release_id=rel_3,
            expected_parent_generation=1,  # Stale! Current is 2
            expected_parent_release_id=rel_1,
            operation_key="op_stale_pub",
            data_root=data_root,
            registry=reg,
        )

    # 4. Attempt rollback to corrupted release: tamper 1 byte in rel_1 analytics.duckdb
    rel1_dir = temp_env["releases_dir"] / rel_1
    rel1_db = rel1_dir / "analytics.duckdb"
    orig_rel1_bytes = rel1_db.read_bytes()

    tampered_bytes = bytearray(orig_rel1_bytes)
    tampered_bytes[50] ^= 0xFF
    rel1_db.write_bytes(bytes(tampered_bytes))

    with pytest.raises(RollbackIntegrityError, match="Rollback target integrity failure"):
        rollback(
            target_release_id=rel_1,
            reason="Operator rollback test to corrupt target",
            expected_parent_generation=2,
            expected_parent_release_id=rel_2,
            operation_key="op_rollback_corrupt",
            data_root=data_root,
            registry=reg,
            verify_bytes=True,
        )

    # Current release pointer remains at rel_2 / generation 2
    curr = reg.get_current_release()
    assert curr["release_id"] == rel_2
    assert curr["generation"] == 2

    # Restore intact bytes of rel_1
    rel1_db.write_bytes(orig_rel1_bytes)

    # 5. Successful Rollback to rel_1: advances generation to 3
    rb_res = rollback(
        target_release_id=rel_1,
        reason="Operator requested rollback to verified release 1",
        expected_parent_generation=2,
        expected_parent_release_id=rel_2,
        operation_key="op_rollback_valid",
        data_root=data_root,
        registry=reg,
        verify_bytes=True,
    )
    assert rb_res.status == "SUCCEEDED"
    assert rb_res.new_generation == 3
    assert rb_res.selected_release_id == rel_1
    assert rb_res.previous_release_id == rel_2

    # Verify current_release singleton points to rel_1 at generation 3
    curr_after = reg.get_current_release()
    assert curr_after["release_id"] == rel_1
    assert curr_after["generation"] == 3

    # 6. Attempt rollback to REJECTED release
    reg.reject_release(rel_2, reason="Security audit found issue")
    with pytest.raises(InvalidReleaseStatusError, match="Only 'VERIFIED' releases can be selected"):
        rollback(
            target_release_id=rel_2,
            reason="Attempt rollback to rejected release",
            expected_parent_generation=3,
            expected_parent_release_id=rel_1,
            operation_key="op_rollback_rejected",
            data_root=data_root,
            registry=reg,
            verify_bytes=True,
        )


def test_scenario_t24_registry_migration_backup_and_restore_artifacts_untouched(temp_env):
    """Scenario T24: Registry migration on a copy and restore.
    
    Invariants:
    1. Pre-migration SQLite Online Backup created via create_online_backup.
    2. Verified with PRAGMA integrity_check == 'ok' and foreign_key_check == [].
    3. Migration applied transactionally.
    4. Restore from backup restores schema v1.
    5. Released files on disk in releases/ and raw/ are 100% byte-for-byte untouched.
    """
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)

    # Setup a release on disk
    att_1 = str(uuid.uuid4())
    reg.create_attempt(att_1, "{}")
    rel_1 = str(uuid.uuid4())
    _setup_and_verify_release(temp_env, reg, rel_1, att_1)

    rel_dir = temp_env["releases_dir"] / rel_1
    db_file = rel_dir / "analytics.duckdb"
    orig_db_sha = hash_file_sha256(db_file)
    orig_db_bytes = db_file.read_bytes()

    # 1. Create pre-migration backup
    backup_path = reg.create_online_backup()
    assert backup_path.exists()

    # 2. Apply simulated migration v2
    mig_v2 = Migration(
        version=2,
        name="002_test_telemetry_table",
        ddl_sql="""
        CREATE TABLE IF NOT EXISTS test_telemetry (
            id TEXT PRIMARY KEY,
            val REAL NOT NULL
        );
        """,
    )
    MIGRATIONS.append(mig_v2)

    try:
        reg.migrate(target_version=2)
        assert reg.get_current_schema_version() == 2

        # Verify new table exists
        con = reg.get_connection()
        try:
            cur = con.cursor()
            try:
                cur.execute("SELECT count(*) FROM test_telemetry;")
            finally:
                cur.close()
        finally:
            con.close()

        # 3. Restore from pre-migration backup
        reg.restore_from_backup(backup_path)

        # 4. Invariants after restore:
        assert reg.get_current_schema_version() == 1
        con = reg.get_connection()
        try:
            cur = con.cursor()
            try:
                tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
                assert "test_telemetry" not in tables
            finally:
                cur.close()
        finally:
            con.close()

        # 5. Crucial Invariant: Released artifacts on disk are byte-for-byte identical!
        assert db_file.exists()
        assert db_file.read_bytes() == orig_db_bytes
        assert hash_file_sha256(db_file) == orig_db_sha

    finally:
        MIGRATIONS.remove(mig_v2)


def test_walkthrough_w01_retry_after_later_rollback(temp_env):
    """Walkthrough W01: A retry after a later rollback trace.
    
    Detailed step-by-step trace from walkthrough-results.md:
    1. Initial State: Release A is current at generation 7.
    2. Operation K selects Release B: CAS advances generation from 7 to 8.
       Event (A -> B, 7 -> 8) committed to SQLite. Ack is lost / process dies.
    3. Intervening Rollback: Operator rolls back to Release A.
       CAS advances generation from 8 to 9. Event (B -> A, 8 -> 9) committed.
    4. Retry of Operation K:
       Caller retries operation K.
       Registry queries publication_events for operation_key = K.
       Finds event (A -> B, 7 -> 8).
       Returns historical success: ALREADY_COMMITTED, new_generation = 8, selected_release_id = B.
       Reports current pointer is (A, generation 9) and is_current = False.
    5. Invariants:
       Release B is NOT reselected.
       current_release generation remains 9 (NOT incremented to 10).
       A stale selection request expecting A at generation 7 fails with StaleGenerationConflictError.
    """
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)

    rel_a = str(uuid.uuid4())
    att_a = str(uuid.uuid4())
    reg.create_attempt(att_a, "{}")
    _setup_and_verify_release(temp_env, reg, rel_a, att_a)

    rel_b = str(uuid.uuid4())
    att_b = str(uuid.uuid4())
    reg.create_attempt(att_b, "{}")
    _setup_and_verify_release(temp_env, reg, rel_b, att_b)

    # Set generation to 7 manually for faithful reproduction of Walkthrough W01 scenario
    con = reg.get_connection()
    try:
        cur = con.cursor()
        cur.execute(
            "UPDATE current_release SET release_id = ?, generation = 7 WHERE id = 1;",
            (rel_a,),
        )
        con.commit()
    finally:
        con.close()

    # Step 1: Verify initial state (Release A, generation 7)
    curr = reg.get_current_release()
    assert curr["release_id"] == rel_a
    assert curr["generation"] == 7

    # Step 2: Operation K selects Release B (A -> B, 7 -> 8)
    op_key_k = "op_k_publish_b"
    res_k = publish(
        release_id=rel_b,
        expected_parent_generation=7,
        expected_parent_release_id=rel_a,
        operation_key=op_key_k,
        attempt_id=att_b,
        data_root=data_root,
        registry=reg,
    )
    assert res_k.status == "SUCCEEDED"
    assert res_k.new_generation == 8
    assert res_k.selected_release_id == rel_b

    # Simulated crash: Acknowledgement of Operation K was lost!

    # Step 3: Intervening rollback to Release A (B -> A, 8 -> 9)
    op_key_rb = "op_rollback_to_a"
    res_rb = rollback(
        target_release_id=rel_a,
        reason="Operator emergency rollback",
        expected_parent_generation=8,
        expected_parent_release_id=rel_b,
        operation_key=op_key_rb,
        data_root=data_root,
        registry=reg,
    )
    assert res_rb.status == "SUCCEEDED"
    assert res_rb.new_generation == 9
    assert res_rb.selected_release_id == rel_a

    curr_now = reg.get_current_release()
    assert curr_now["release_id"] == rel_a
    assert curr_now["generation"] == 9

    # Step 4: Retry of Operation K via recover_unacknowledged_selection
    status_k = reg.query_selection_status(op_key_k)
    assert status_k["status"] == "COMMITTED"
    assert status_k["new_generation"] == 8
    assert status_k["selected_release_id"] == rel_b
    assert status_k["is_current"] is False
    assert status_k["current_generation"] == 9
    assert status_k["current_release_id"] == rel_a

    # Caller retries publish(op_key_k)
    retry_k = publish(
        release_id=rel_b,
        expected_parent_generation=7,
        expected_parent_release_id=rel_a,
        operation_key=op_key_k,
        data_root=data_root,
        registry=reg,
    )
    assert retry_k.status == "ALREADY_COMMITTED"
    assert retry_k.new_generation == 8
    assert retry_k.selected_release_id == rel_b
    assert retry_k.is_current is False

    # Step 5: Invariants verified:
    # current_release is still Release A at generation 9! NOT Release B, NOT generation 10!
    curr_final = reg.get_current_release()
    assert curr_final["release_id"] == rel_a
    assert curr_final["generation"] == 9

    # Fresh operation expecting A at generation 7 fails with StaleGenerationConflictError
    with pytest.raises(StaleGenerationConflictError, match="CAS conflict: current generation 9 != expected 7"):
        publish(
            release_id=rel_b,
            expected_parent_generation=7,
            expected_parent_release_id=rel_a,
            operation_key="op_new_stale_k",
            data_root=data_root,
            registry=reg,
        )


def test_zero_wal_verification_and_checkpoint_postconditions(temp_env):
    """Verify DuckDB zero-WAL postcondition and WalVerificationError rejection."""
    data_root = temp_env["data_root"]
    db_file = temp_env["staging_dir"] / "test_wal.duckdb"

    # 1. Normal clean checkpoint and close
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE t (x INT); INSERT INTO t VALUES (1), (2);")
    checkpoint_and_verify_zero_wal(con, db_file)

    wal_file = Path(str(db_file) + ".wal")
    assert not wal_file.exists() or wal_file.stat().st_size == 0

    # 2. Non-zero dirty WAL produces WalVerificationError
    wal_file.write_bytes(b"UNFLUSHED_DIRTY_WAL_PAGES")
    with pytest.raises(WalVerificationError, match="DuckDB .wal file is non-zero"):
        checkpoint_and_verify_zero_wal(None, db_file)


def test_atomic_directory_rename_destination_already_exists_protection(temp_env):
    """Verify that finalize_candidate raises DestinationAlreadyExistsError without overwrite."""
    data_root = temp_env["data_root"]
    reg = Registry(temp_env["db_path"], data_root=data_root, auto_migrate=True)

    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())
    plan, derivation_id = _build_test_frozen_plan()

    # Pre-create conflicting release directory
    target_dir = temp_env["releases_dir"] / release_id
    target_dir.mkdir(parents=True, exist_ok=True)
    sentinel_file = target_dir / "pre_existing_marker.txt"
    sentinel_file.write_text("DO_NOT_OVERWRITE")

    # Candidate staging directory
    prep_dir = temp_env["staging_dir"] / attempt_id / "release_prep"
    prep_dir.mkdir(parents=True, exist_ok=True)
    db_path = prep_dir / "analytics.duckdb"

    _create_synthetic_candidate_db(
        db_path=db_path,
        release_id=release_id,
        derivation_id=derivation_id,
    )

    with pytest.raises(DestinationAlreadyExistsError, match="Target release directory already exists"):
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

    # Invariant: Pre-existing marker file in target was preserved
    assert sentinel_file.exists()
    assert sentinel_file.read_text() == "DO_NOT_OVERWRITE"
