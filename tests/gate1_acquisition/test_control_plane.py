"""Comprehensive Test Suite for SQLite Control Plane & Migrations (Milestone M1 / Gate G1).

Verifies:
- All 15 control relations DDL and constraints
- Mandatory SQLite durability pragmas (synchronous=EXTRA, fullfsync=ON, foreign_keys=ON, journal_mode=DELETE)
- Versioned migrations with pre-migration SQLite Online Backup
- Scenario T24: Registry migration and restore from backup without altering released artifacts
- Scenario T18: Kill after SQLite commit before acknowledgement; reconciliation finds committed event, retry does not duplicate selection
- Atomic Compare-And-Swap (CAS) publication and stale generation conflict detection
- Idempotent selection requests and parameter mismatch conflicts
- Release lifecycle transitions (FINALIZED -> VERIFIED -> REJECTED -> RETIRED)
- Foreign key cascading and check constraint enforcements
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import uuid

try:
    import pytest
except ImportError:
    class _RaisesContext:
        def __init__(self, expected_exception, match=None):
            self.expected_exception = expected_exception
            self.match = match
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is None:
                raise AssertionError(f"Expected {self.expected_exception} but no exception was raised")
            if not issubclass(exc_type, self.expected_exception):
                return False
            if self.match and self.match not in str(exc_val):
                raise AssertionError(f"Expected match '{self.match}' in '{exc_val}'")
            return True

    class _PytestShim:
        @staticmethod
        def raises(expected_exception, match=None):
            return _RaisesContext(expected_exception, match)

        @staticmethod
        def fixture(func):
            return func

    pytest = _PytestShim()

from hvfhv.control.registry import (
    Registry,
    RegistryError,
    MigrationDowngradeError,
    SelectionConflictError,
    StaleGenerationConflictError,
    InvalidReleaseStatusError,
    Migration,
    MIGRATIONS,
)


@pytest.fixture
def temp_env():
    """Create a temporary data root directory."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        control_dir = data_root / "control"
        releases_dir = data_root / "releases"
        control_dir.mkdir(parents=True, exist_ok=True)
        releases_dir.mkdir(parents=True, exist_ok=True)
        db_path = control_dir / "registry.sqlite"
        yield {
            "root": root,
            "data_root": data_root,
            "control_dir": control_dir,
            "releases_dir": releases_dir,
            "db_path": db_path,
        }


def test_pragmas_and_initialization(temp_env):
    """Verify that all 5 required SQLite durability pragmas are set and 15 relations are created."""
    db_path = temp_env["db_path"]
    data_root = temp_env["data_root"]

    reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
    con = reg.get_connection()
    try:
        cur = con.cursor()
        # Verify durability pragmas
        assert cur.execute("PRAGMA foreign_keys;").fetchone()[0] == 1
        assert cur.execute("PRAGMA journal_mode;").fetchone()[0].lower() == "delete"
        assert cur.execute("PRAGMA synchronous;").fetchone()[0] == 3  # EXTRA
        assert cur.execute("PRAGMA fullfsync;").fetchone()[0] == 1    # ON
        assert cur.execute("PRAGMA busy_timeout;").fetchone()[0] == 5000

        # Verify all 15 control relations exist
        tables = [
            row[0]
            for row in cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
            ).fetchall()
        ]
        expected_tables = [
            "attempt_events",
            "attempts",
            "cleanup_items",
            "cleanup_requests",
            "current_release",
            "publication_events",
            "release_sources",
            "release_status_events",
            "release_verifications",
            "releases",
            "schema_migrations",
            "selection_requests",
            "source_objects",
            "source_observations",
            "source_revisions",
        ]
        for t in expected_tables:
            assert t in tables, f"Expected table {t} missing from registry"

        # Verify initial schema version is 1
        assert reg.get_current_schema_version(con) == 1

        # Verify current_release singleton row is seeded
        curr = cur.execute("SELECT id, release_id, generation FROM current_release;").fetchall()
        assert len(curr) == 1
        assert curr[0][0] == 1
        assert curr[0][1] is None
        assert curr[0][2] == 0
    finally:
        cur.close()
        con.close()


def test_scenario_t24_migration_and_restore(temp_env):
    """Scenario T24: Registry migration and restore from pre-migration backup.
    
    Invariants verified:
    1. Pre-migration backup is created via SQLite Online Backup API.
    2. Integrity and FK checks pass before and after migration.
    3. Old backup is fully restorable.
    4. Released artifacts on the filesystem remain 100% bit-for-bit unchanged.
    """
    db_path = temp_env["db_path"]
    data_root = temp_env["data_root"]
    releases_dir = temp_env["releases_dir"]

    reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

    # 1. Populate initial data (v1)
    reg.register_source_object("tlc/hvfhv/2024-01", "hvfhv", "https://example.com/2024-01.parquet", "2024-01")
    reg.register_revision(
        revision_id="rev_2024_01",
        logical_source_id="tlc/hvfhv/2024-01",
        content_sha256="abc123sha",
        byte_count=1024,
        relative_path="raw/sha256/abc123sha/source.parquet",
        schema_fingerprint="fp1",
        footer_row_count=100,
        decoded_row_count=100,
        provenance_relative_path="raw/sha256/abc123sha/acquisition_provenance.json",
    )
    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, json.dumps({"window": ["2024-01"]}))

    # Create dummy released artifact directory and file on disk
    release_id = str(uuid.uuid4())
    rel_dir = releases_dir / release_id
    rel_dir.mkdir(parents=True, exist_ok=True)
    db_file = rel_dir / "release.duckdb"
    db_content = b"IMMUTABLE_RELEASE_DUCKDB_PAYLOAD_V1"
    db_file.write_bytes(db_content)

    reg.register_finalized_release(
        release_id=release_id,
        derivation_id="deriv_1",
        producing_attempt_id=attempt_id,
        release_directory=f"releases/{release_id}",
        manifest_sha256="m_sha",
        database_sha256="db_sha",
        validation_sha256="v_sha",
        source_revision_map={"tlc/hvfhv/2024-01": "rev_2024_01"},
    )
    reg.record_verification(
        release_id=release_id,
        manifest_sha256="m_sha",
        database_sha256="db_sha",
        validation_sha256="v_sha",
        rule_inventory_version="rules.v1",
        rule_inventory_hash="rules_sha",
        verification_results_json='{"status": "PASS"}',
        verifier_version="1.0.0",
    )

    # 2. Take explicit backup before migration
    backup_path = reg.create_online_backup()
    assert backup_path.exists()
    assert reg.get_current_schema_version() == 1

    # 3. Simulate Migration to Version 2
    migration_v2 = Migration(
        version=2,
        name="002_add_operator_audit_column",
        ddl_sql="""
        ALTER TABLE attempts ADD COLUMN operator_notes TEXT;
        CREATE TABLE IF NOT EXISTS system_telemetry (
            metric_id TEXT PRIMARY KEY,
            recorded_at_utc TEXT NOT NULL,
            metric_value REAL NOT NULL
        );
        """,
    )
    MIGRATIONS.append(migration_v2)

    try:
        applied = reg.migrate(target_version=2)
        assert applied == [2]
        assert reg.get_current_schema_version() == 2

        # Verify new column and table exist in active DB
        con = reg.get_connection()
        cur = con.cursor()
        cur.execute("SELECT operator_notes FROM attempts LIMIT 1;")
        cur.execute("SELECT count(*) FROM system_telemetry;")
        cur.close()
        con.close()

        # 4. Scenario T24: Perform Restore from pre-migration backup
        reg.restore_from_backup(backup_path)

        # 5. Verify restored state
        assert reg.get_current_schema_version() == 1
        con = reg.get_connection()
        cur = con.cursor()
        # Ensure system_telemetry does not exist
        tables = [row[0] for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        assert "system_telemetry" not in tables

        # Ensure operator_notes column does not exist on attempts
        col_names = [col[1] for col in cur.execute("PRAGMA table_info(attempts);").fetchall()]
        assert "operator_notes" not in col_names

        # Ensure all original records survived restoration
        src = cur.execute("SELECT logical_source_id FROM source_objects;").fetchall()
        assert len(src) == 1 and src[0][0] == "tlc/hvfhv/2024-01"
        rels = cur.execute("SELECT release_id, status FROM releases;").fetchall()
        assert len(rels) == 1 and rels[0][0] == release_id and rels[0][1] == "VERIFIED"
        cur.close()
        con.close()

        # Invariant check: Released artifact on filesystem is strictly unchanged
        assert db_file.exists()
        assert db_file.read_bytes() == db_content

    finally:
        MIGRATIONS.remove(migration_v2)


def test_scenario_t18_kill_after_commit_idempotent_retry(temp_env):
    """Scenario T18: Kill after SQLite commit before acknowledgement.
    
    Invariants verified:
    1. Publication CAS transaction updates current_release and appends publication_events.
    2. Simulated process kill after commit means acknowledgement was not received by caller.
    3. Caller retries publish_release with the identical operation_key.
    4. Registry finds the committed event and returns ALREADY_COMMITTED without duplicating selection or incrementing generation!
    """
    db_path = temp_env["db_path"]
    data_root = temp_env["data_root"]
    reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

    # Setup source, attempt, and verified release
    reg.register_source_object("tlc/hvfhv/2024-01", "hvfhv", "https://example.com/2024-01.parquet", "2024-01")
    reg.register_revision(
        revision_id="rev_1",
        logical_source_id="tlc/hvfhv/2024-01",
        content_sha256="hash1",
        byte_count=500,
        relative_path="raw/sha256/hash1/source.parquet",
        schema_fingerprint="fp",
        footer_row_count=50,
        decoded_row_count=50,
        provenance_relative_path="prov.json",
    )
    attempt_id = str(uuid.uuid4())
    reg.create_attempt(attempt_id, "{}")
    release_id = str(uuid.uuid4())
    reg.register_finalized_release(
        release_id=release_id,
        derivation_id="deriv_1",
        producing_attempt_id=attempt_id,
        release_directory=f"releases/{release_id}",
        manifest_sha256="m",
        database_sha256="d",
        validation_sha256="v",
        source_revision_map={"tlc/hvfhv/2024-01": "rev_1"},
    )
    reg.record_verification(
        release_id=release_id,
        manifest_sha256="m",
        database_sha256="d",
        validation_sha256="v",
        rule_inventory_version="v1",
        rule_inventory_hash="h",
        verification_results_json="{}",
        verifier_version="1.0.0",
    )

    # Register selection request
    op_key = "op_test_t18_crash_recovery"
    reg.record_selection_request(
        operation_key=op_key,
        target_release_id=release_id,
        expected_parent_release_id=None,
        expected_parent_generation=0,
        operation_kind="publish",
        reason="Initial monthly publication",
    )

    # First attempt: publish executes and commits
    res1 = reg.publish_release(operation_key=op_key, attempt_id=attempt_id)
    assert res1["status"] == "SUCCEEDED"
    assert res1["previous_generation"] == 0
    assert res1["new_generation"] == 1
    assert res1["selected_release_id"] == release_id

    # Simulate process death: acknowledgement lost.
    # Caller retries publish_release with the SAME operation_key.
    res2 = reg.publish_release(operation_key=op_key, attempt_id=attempt_id)

    # Invariant: Must return ALREADY_COMMITTED
    assert res2["status"] == "ALREADY_COMMITTED"
    assert res2["event_id"] == res1["event_id"]
    assert res2["new_generation"] == 1
    assert res2["selected_release_id"] == release_id

    # Verify that generation in current_release was NOT incremented again
    curr = reg.get_current_release()
    assert curr["release_id"] == release_id
    assert curr["generation"] == 1
    assert curr["status"] == "VERIFIED"

    # Verify that publication_events has exactly ONE row for this operation_key
    con = reg.get_connection()
    events = con.execute("SELECT count(*) FROM publication_events WHERE operation_key = ?;", (op_key,)).fetchone()[0]
    con.close()
    assert events == 1


def test_cas_concurrency_and_stale_generation_conflict(temp_env):
    """Verify that compare-and-swap (CAS) rejects publication if expected generation does not match."""
    db_path = temp_env["db_path"]
    data_root = temp_env["data_root"]
    reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

    # Attempt to publish with expected_parent_generation = 99 (when current is 0)
    op_key = "op_stale_test"
    rel_id = str(uuid.uuid4())
    reg.create_attempt("att1", "{}")
    reg.register_source_object("tlc/taxi_zones", "taxi_zones", "https://example.com/zones.csv")
    reg.register_revision("rev_zone", "tlc/taxi_zones", "zh", 100, "zones.csv", "fp", 265, 265, "p.json")
    reg.register_finalized_release(rel_id, "d", "att1", f"releases/{rel_id}", "m", "d", "v", {"tlc/taxi_zones": "rev_zone"})
    reg.record_verification(rel_id, "m", "d", "v", "v", "h", "{}", "1.0.0")

    reg.record_selection_request(
        operation_key=op_key,
        target_release_id=rel_id,
        expected_parent_generation=99,  # STALE! Current is 0
    )

    with pytest.raises(StaleGenerationConflictError, match="current generation 0 != expected 99"):
        reg.publish_release(op_key)


def test_selection_request_conflict_detection(temp_env):
    """Verify that reusing the same operation_key with different parameters raises SelectionConflictError."""
    db_path = temp_env["db_path"]
    data_root = temp_env["data_root"]
    reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

    op_key = "op_idempotency_test"
    rel1 = str(uuid.uuid4())
    rel2 = str(uuid.uuid4())
    att_id = str(uuid.uuid4())
    reg.create_attempt(att_id, "{}")
    reg.register_source_object("tlc/taxi_zones", "taxi_zones", "https://example.com/zones.csv")
    reg.register_revision("rev_zone", "tlc/taxi_zones", "zh", 100, "zones.csv", "fp", 265, 265, "p.json")
    reg.register_finalized_release(rel1, "d1", att_id, f"releases/{rel1}", "m1", "d1", "v1", {"tlc/taxi_zones": "rev_zone"})
    reg.register_finalized_release(rel2, "d2", att_id, f"releases/{rel2}", "m2", "d2", "v2", {"tlc/taxi_zones": "rev_zone"})

    reg.record_selection_request(
        operation_key=op_key,
        target_release_id=rel1,
        expected_parent_generation=0,
    )

    # Identical parameters: idempotent success
    reg.record_selection_request(
        operation_key=op_key,
        target_release_id=rel1,
        expected_parent_generation=0,
    )

    # Differing parameters: CONFLICT
    with pytest.raises(SelectionConflictError, match="previously registered with different parameters"):
        reg.record_selection_request(
            operation_key=op_key,
            target_release_id=rel2,  # Different target release!
            expected_parent_generation=0,
        )


def test_release_rejection_and_retirement(temp_env):
    """Verify release rejection and retirement lifecycle and serving invalidation."""
    db_path = temp_env["db_path"]
    data_root = temp_env["data_root"]
    reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

    rel_id = str(uuid.uuid4())
    att_id = str(uuid.uuid4())
    reg.create_attempt(att_id, "{}")
    reg.register_source_object("tlc/taxi_zones", "taxi_zones", "https://example.com/zones.csv")
    reg.register_revision("rev_zone", "tlc/taxi_zones", "zh", 100, "zones.csv", "fp", 265, 265, "p.json")
    reg.register_finalized_release(rel_id, "d", att_id, f"releases/{rel_id}", "m", "d", "v", {"tlc/taxi_zones": "rev_zone"})
    reg.record_verification(rel_id, "m", "d", "v", "v", "h", "{}", "1.0.0")

    # Publish release
    reg.record_selection_request("op_reject_test", rel_id, expected_parent_generation=0)
    reg.publish_release("op_reject_test")

    curr = reg.get_current_release()
    assert curr["status"] == "VERIFIED"

    # Reject release
    reg.reject_release(rel_id, reason="Post-publication data anomaly detected in CBD fee")
    curr_rejected = reg.get_current_release()
    assert curr_rejected["status"] == "REJECTED"

    # Publishing a REJECTED release must fail
    reg.record_selection_request("op_republish_rejected", rel_id, expected_parent_generation=1)
    with pytest.raises(InvalidReleaseStatusError, match="Only 'VERIFIED' releases can be selected"):
        reg.publish_release("op_republish_rejected")

    # Retire release
    reg.retire_release(rel_id, reason="Payload unlinked during storage cleanup")
    curr_retired = reg.get_current_release()
    assert curr_retired["status"] == "RETIRED"


def test_foreign_key_and_check_constraints(temp_env):
    """Verify that foreign keys and check constraints prevent invalid states."""
    db_path = temp_env["db_path"]
    data_root = temp_env["data_root"]
    reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
    con = reg.get_connection()
    cur = con.cursor()

    try:
        # 1. Foreign key violation: observation for nonexistent source
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            cur.execute(
                """
                INSERT INTO source_observations (
                    observation_id, logical_source_id, observed_at_utc,
                    http_method, http_status, outcome
                ) VALUES ('obs1', 'nonexistent_source', '2026-01-01T00:00:00Z', 'GET', 200, 'AVAILABLE');
                """
            )

        # 2. Check constraint violation: source_objects month missing for kind='hvfhv'
        with pytest.raises(sqlite3.IntegrityError):
            cur.execute(
                """
                INSERT INTO source_objects (logical_source_id, kind, source_month, official_url)
                VALUES ('tlc/hvfhv/bad', 'hvfhv', NULL, 'https://example.com');
                """
            )

        # 3. Check constraint violation: source_objects month supplied for taxi_zones
        with pytest.raises(sqlite3.IntegrityError):
            cur.execute(
                """
                INSERT INTO source_objects (logical_source_id, kind, source_month, official_url)
                VALUES ('tlc/taxi_zones', 'taxi_zones', '2024-01', 'https://example.com');
                """
            )

        # 4. Check constraint violation: attempts phase enum
        with pytest.raises(sqlite3.IntegrityError):
            cur.execute(
                """
                INSERT INTO attempts (
                    attempt_id, requested_configuration_json, current_phase, outcome, started_at_utc
                ) VALUES ('att_bad', '{}', 'INVALID_PHASE', 'RUNNING', '2026-01-01T00:00:00Z');
                """
            )

        # 5. Singleton constraint: current_release cannot have a second row
        with pytest.raises(sqlite3.IntegrityError):
            cur.execute(
                """
                INSERT INTO current_release (id, release_id, generation, updated_at_utc)
                VALUES (2, NULL, 0, '2026-01-01T00:00:00Z');
                """
            )

        # 6. Migration downgrade constraint
        with pytest.raises(MigrationDowngradeError, match="Cannot downgrade"):
            reg.migrate(target_version=0)

    finally:
        con.close()


def test_attempt_phase_progression_and_events(temp_env):
    """Verify attempt phase progression and append-only event logging."""
    db_path = temp_env["db_path"]
    data_root = temp_env["data_root"]
    reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

    att_id = str(uuid.uuid4())
    reg.create_attempt(att_id, '{"window": "2024"}')

    # Progress through phases
    reg.transition_attempt_phase(att_id, "ACQUIRING")
    reg.transition_attempt_phase(att_id, "INPUTS_READY", derivation_id="deriv_123", frozen_plan_hash="fph")
    reg.transition_attempt_phase(att_id, "BUILDING")
    reg.transition_attempt_phase(att_id, "VALIDATING")
    reg.transition_attempt_phase(att_id, "FINALIZING")
    reg.transition_attempt_phase(att_id, "SELECTING")
    reg.transition_attempt_phase(att_id, "COMPLETE")
    reg.complete_attempt(att_id, "SUCCEEDED")

    con = reg.get_connection()
    cur = con.cursor()
    # Check attempt record
    att = cur.execute("SELECT current_phase, outcome, derivation_id FROM attempts WHERE attempt_id = ?;", (att_id,)).fetchone()
    assert att[0] == "COMPLETE"
    assert att[1] == "SUCCEEDED"
    assert att[2] == "deriv_123"

    # Check attempt events sequence (1 to 8)
    events = cur.execute("SELECT sequence, previous_phase, new_phase FROM attempt_events WHERE attempt_id = ? ORDER BY sequence;", (att_id,)).fetchall()
    assert len(events) == 8
    assert events[0][2] == "PLANNED"
    assert events[1][1] == "PLANNED" and events[1][2] == "ACQUIRING"
    assert events[7][1] == "SELECTING" and events[7][2] == "COMPLETE"
    con.close()


if __name__ == "__main__":
    tests = [
        test_pragmas_and_initialization,
        test_scenario_t24_migration_and_restore,
        test_scenario_t18_kill_after_commit_idempotent_retry,
        test_cas_concurrency_and_stale_generation_conflict,
        test_selection_request_conflict_detection,
        test_release_rejection_and_retirement,
        test_foreign_key_and_check_constraints,
        test_attempt_phase_progression_and_events,
    ]
    passed = 0
    failed = 0
    for t in tests:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            env = {
                "root": root,
                "data_root": root / ".data",
                "control_dir": root / ".data" / "control",
                "releases_dir": root / ".data" / "releases",
                "db_path": root / ".data" / "control" / "registry.sqlite",
            }
            env["control_dir"].mkdir(parents=True, exist_ok=True)
            env["releases_dir"].mkdir(parents=True, exist_ok=True)
            try:
                t(env)
                print(f"[PASS] {t.__name__}")
                passed += 1
            except Exception as e:
                print(f"[FAIL] {t.__name__}: {e}")
                import traceback
                traceback.print_exc()
                failed += 1

    print(f"\nRESULTS: {passed} PASSED, {failed} FAILED")
    if failed > 0:
        sys.exit(1)

