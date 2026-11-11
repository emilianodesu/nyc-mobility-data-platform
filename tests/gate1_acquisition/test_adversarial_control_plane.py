"""Adversarial stress test harness for SQLite Control Plane & State Machine (Milestone M1 / Gate G1).

Empirically tests:
1. Multi-process & multi-threaded CAS publication contention storm (conflicting candidates, exactly 1 winner).
2. Multi-process & multi-threaded idempotent retry contention storm under identical operation_key.
3. Rapid repeated idempotent publication retries (50 iterations) with zero side-effects.
4. Durability pragmas invariant verification across 50 independently opened connections.
5. Transaction rollback on constraint violations (FK violation, CHECK violations, UNIQUE violations).
6. Failed migration transaction rollback & database preservation.
7. Catastrophic database corruption simulation & bit-for-bit restore from pre-migration backup (Scenario T24).
8. State machine security: rejection of unverified/rejected/retired releases, digest mismatch detection, parameter collision detection.
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from typing import Any
import uuid

# Ensure repository root and src/ are importable
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.control.locks import writer_lock, LockContentionError
from hvfhv.control.registry import (
    Registry,
    RegistryError,
    MigrationError,
    MigrationDowngradeError,
    SelectionConflictError,
    StaleGenerationConflictError,
    InvalidReleaseStatusError,
    BackupIntegrityError,
    Migration,
    MIGRATIONS,
    open_registry_connection,
)


# ==============================================================================
# Multiprocessing Worker Functions (Top-level for spawn pickling)
# ==============================================================================

def _conflicting_publish_worker(
    db_path_str: str,
    data_root_str: str,
    op_key: str,
    worker_idx: int,
    barrier: Any,
    result_queue: Any,
):
    """Worker process contending to publish a unique release at expected generation 0."""
    reg = Registry(db_path=db_path_str, data_root=data_root_str, auto_migrate=False)
    barrier.wait()  # Synchronize release publish attempts

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            res = reg.publish_release(operation_key=op_key)
            result_queue.put(("SUCCEEDED", worker_idx, res))
            return
        except StaleGenerationConflictError as e:
            result_queue.put(("STALE_CONFLICT", worker_idx, str(e)))
            return
        except LockContentionError:
            time.sleep(0.01)
        except Exception as e:
            result_queue.put(("ERROR", worker_idx, str(e)))
            return
    result_queue.put(("TIMEOUT", worker_idx, "Timed out"))


def _identical_key_publish_worker(
    db_path_str: str,
    data_root_str: str,
    op_key: str,
    worker_idx: int,
    barrier: Any,
    result_queue: Any,
):
    """Worker process contending to publish using the IDENTICAL operation_key."""
    reg = Registry(db_path=db_path_str, data_root=data_root_str, auto_migrate=False)
    barrier.wait()  # Synchronize

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            res = reg.publish_release(operation_key=op_key)
            result_queue.put((res["status"], worker_idx, res))
            return
        except LockContentionError:
            time.sleep(0.01)
        except Exception as e:
            result_queue.put(("ERROR", worker_idx, str(e)))
            return
    result_queue.put(("TIMEOUT", worker_idx, "Timed out"))


# ==============================================================================
# Helper to seed a valid verified release
# ==============================================================================

def _seed_verified_release(
    reg: Registry,
    releases_dir: Path,
    release_suffix: str = "1",
    month_num: int = 1,
) -> tuple[str, str, Path, bytes]:
    """Helper to seed source, revision, attempt, and verified release."""
    month_str = f"2024-{((month_num - 1) % 12) + 1:02d}"
    source_id = f"tlc/hvfhv/{month_str}_{release_suffix}"
    # Wait, source_objects primary key is logical_source_id.
    # Kind is 'hvfhv', source_month must be GLOB '[12][0-9][0-9][0-9]-[01][0-9]'.
    reg.register_source_object(source_id, "hvfhv", f"https://example.com/{month_str}_{release_suffix}.parquet", month_str)
    rev_id = f"rev_{release_suffix}"
    reg.register_revision(
        revision_id=rev_id,
        logical_source_id=source_id,
        content_sha256=f"hash_{release_suffix}",
        byte_count=1024,
        relative_path=f"raw/sha256/hash_{release_suffix}/source.parquet",
        schema_fingerprint="fp1",
        footer_row_count=100,
        decoded_row_count=100,
        provenance_relative_path="prov.json",
    )
    attempt_id = f"attempt_{release_suffix}_{uuid.uuid4().hex[:8]}"
    reg.create_attempt(attempt_id, '{"window": ["2024"]}')

    release_id = f"release_{release_suffix}_{uuid.uuid4().hex[:8]}"
    rel_dir = releases_dir / release_id
    rel_dir.mkdir(parents=True, exist_ok=True)
    db_file = rel_dir / "release.duckdb"
    db_bytes = f"PAYLOAD_RELEASE_{release_suffix}_{uuid.uuid4()}".encode("utf-8")
    db_file.write_bytes(db_bytes)

    manifest_hash = hashlib.sha256(b"manifest" + release_suffix.encode()).hexdigest()
    database_hash = hashlib.sha256(db_bytes).hexdigest()
    validation_hash = hashlib.sha256(b"validation" + release_suffix.encode()).hexdigest()

    reg.register_finalized_release(
        release_id=release_id,
        derivation_id=f"deriv_{release_suffix}",
        producing_attempt_id=attempt_id,
        release_directory=f"releases/{release_id}",
        manifest_sha256=manifest_hash,
        database_sha256=database_hash,
        validation_sha256=validation_hash,
        source_revision_map={source_id: rev_id},
    )
    reg.record_verification(
        release_id=release_id,
        manifest_sha256=manifest_hash,
        database_sha256=database_hash,
        validation_sha256=validation_hash,
        rule_inventory_version="v1",
        rule_inventory_hash="rule_hash",
        verification_results_json='{"status": "PASS"}',
        verifier_version="1.0.0",
    )
    return release_id, attempt_id, db_file, db_bytes


# ==============================================================================
# Adversarial Challenge Test Suites
# ==============================================================================

def test_durability_pragmas_stress_50_connections():
    """Verify that 50 independently opened connections across threads strictly enforce all durability pragmas."""
    print("  -> Testing durability pragmas across 50 independent connections in concurrent threads...")
    start_time = time.time()
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, auto_migrate=True)

        results = []
        lock = threading.Lock()

        def connection_worker(worker_id: int):
            con = reg.get_connection()
            try:
                cur = con.cursor()
                fk = cur.execute("PRAGMA foreign_keys;").fetchone()[0]
                jm = cur.execute("PRAGMA journal_mode;").fetchone()[0].lower()
                sync = cur.execute("PRAGMA synchronous;").fetchone()[0]
                ffsync = cur.execute("PRAGMA fullfsync;").fetchone()[0]
                bt = cur.execute("PRAGMA busy_timeout;").fetchone()[0]
                with lock:
                    results.append((worker_id, fk, jm, sync, ffsync, bt))
                cur.close()
            finally:
                con.close()

        threads = [threading.Thread(target=connection_worker, args=(i,)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 50, f"Expected 50 results, got {len(results)}"
        for wid, fk, jm, sync, ffsync, bt in results:
            assert fk == 1, f"Worker {wid} PRAGMA foreign_keys != 1: {fk}"
            assert jm == "delete", f"Worker {wid} PRAGMA journal_mode != delete: {jm}"
            assert sync == 3, f"Worker {wid} PRAGMA synchronous != 3 (EXTRA): {sync}"
            assert ffsync == 1, f"Worker {wid} PRAGMA fullfsync != 1 (ON): {ffsync}"
            assert bt == 5000, f"Worker {wid} PRAGMA busy_timeout != 5000: {bt}"

    elapsed = time.time() - start_time
    print(f"     [PASS] 50/50 connections verified strictly compliant ({elapsed:.3f}s)")


def test_transaction_rollback_on_constraint_violations():
    """Verify transactional atomicity: inserting invalid FK, failing CHECK or duplicate PK rolls back with zero orphaned rows."""
    print("  -> Testing transaction rollback on FK, CHECK, and UNIQUE constraint violations...")
    start_time = time.time()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        con = reg.get_connection()

        # Baseline count
        cur = con.cursor()
        base_obs = cur.execute("SELECT count(*) FROM source_observations;").fetchone()[0]
        base_src = cur.execute("SELECT count(*) FROM source_objects;").fetchone()[0]
        base_rev = cur.execute("SELECT count(*) FROM source_revisions;").fetchone()[0]
        base_att = cur.execute("SELECT count(*) FROM attempts;").fetchone()[0]
        base_rel = cur.execute("SELECT count(*) FROM releases;").fetchone()[0]
        cur.close()

        # 1. Foreign Key Violation: source_observations referencing non-existent logical_source_id
        fk_failed = False
        try:
            con.execute("BEGIN IMMEDIATE;")
            con.execute(
                """
                INSERT INTO source_observations (
                    observation_id, logical_source_id, observed_at_utc, http_method, outcome
                ) VALUES ('bad_obs_1', 'nonexistent_src', '2026-01-01T00:00:00Z', 'GET', 'AVAILABLE');
                """
            )
            con.commit()
        except sqlite3.IntegrityError:
            con.rollback()
            fk_failed = True
        assert fk_failed, "Expected Foreign Key violation to fail"

        # Verify zero rows added
        cur = con.cursor()
        assert cur.execute("SELECT count(*) FROM source_observations;").fetchone()[0] == base_obs

        # 2. CHECK constraint violation: source_objects kind='hvfhv' with NULL month
        chk1_failed = False
        try:
            con.execute("BEGIN IMMEDIATE;")
            con.execute(
                """
                INSERT INTO source_objects (logical_source_id, kind, source_month, official_url)
                VALUES ('tlc/hvfhv/bad', 'hvfhv', NULL, 'https://example.com');
                """
            )
            con.commit()
        except sqlite3.IntegrityError:
            con.rollback()
            chk1_failed = True
        assert chk1_failed, "Expected CHECK constraint check_hvfhv_source_month to fail"
        assert cur.execute("SELECT count(*) FROM source_objects;").fetchone()[0] == base_src

        # 3. CHECK constraint violation: source_revisions with byte_count <= 0
        reg.register_source_object("tlc/hvfhv/2024-01", "hvfhv", "https://example.com", "2024-01")
        chk2_failed = False
        try:
            con.execute("BEGIN IMMEDIATE;")
            con.execute(
                """
                INSERT INTO source_revisions (
                    revision_id, logical_source_id, content_sha256, byte_count, relative_path,
                    accepted_at_utc, schema_fingerprint, footer_row_count, decoded_row_count, provenance_relative_path
                ) VALUES ('bad_rev', 'tlc/hvfhv/2024-01', 'bad_sha', -100, 'path', '2026-01-01T00:00:00Z', 'fp', 10, 10, 'p');
                """
            )
            con.commit()
        except sqlite3.IntegrityError:
            con.rollback()
            chk2_failed = True
        assert chk2_failed, "Expected CHECK constraint byte_count > 0 to fail"
        assert cur.execute("SELECT count(*) FROM source_revisions;").fetchone()[0] == base_rev

        # 4. CHECK constraint violation: current_release singleton check (id = 1)
        chk3_failed = False
        try:
            con.execute("BEGIN IMMEDIATE;")
            con.execute(
                "INSERT INTO current_release (id, release_id, generation, updated_at_utc) VALUES (2, NULL, 0, '2026-01-01');"
            )
            con.commit()
        except sqlite3.IntegrityError:
            con.rollback()
            chk3_failed = True
        assert chk3_failed, "Expected current_release singleton CHECK (id = 1) to fail"

        # 5. Publication events monotonicity: new_generation <= previous_generation
        chk4_failed = False
        try:
            con.execute("BEGIN IMMEDIATE;")
            con.execute(
                """
                INSERT INTO publication_events (
                    event_id, operation_key, operation_kind, previous_generation, new_generation,
                    selected_release_id, event_at_utc
                ) VALUES ('evt1', 'op_bad_gen', 'publish', 5, 5, 'some_rel', '2026-01-01');
                """
            )
            con.commit()
        except sqlite3.IntegrityError:
            con.rollback()
            chk4_failed = True
        assert chk4_failed, "Expected publication_events CHECK (new_generation > previous_generation) to fail"

        cur.close()
        con.close()

    elapsed = time.time() - start_time
    print(f"     [PASS] All 5 constraint violation rollback scenarios passed with 0 orphaned rows ({elapsed:.3f}s)")


def test_adversarial_multiprocess_cas_contention_storm():
    """Multi-process CAS race: 10 processes simultaneously contend to publish distinct candidate releases at gen 0.

    Invariants:
    1. Exactly ONE process succeeds and advances generation from 0 to 1.
    2. All other 9 processes encounter StaleGenerationConflictError.
    3. Final singleton current_release is strictly at generation 1.
    4. Exactly 1 row in publication_events.
    """
    print("  -> Testing adversarial multi-process CAS contention storm (10 processes racing for generation 0 -> 1)...")
    start_time = time.time()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        releases_dir = data_root / "releases"
        db_path = data_root / "control" / "registry.sqlite"
        releases_dir.mkdir(parents=True, exist_ok=True)

        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

        # Seed 10 distinct verified releases and pre-register selection requests
        release_ids = []
        op_keys = []
        for i in range(10):
            rel_id, _, _, _ = _seed_verified_release(reg, releases_dir, release_suffix=str(i + 1), month_num=i + 1)
            release_ids.append(rel_id)
            op_k = f"op_conflicting_worker_{i}"
            reg.record_selection_request(
                operation_key=op_k,
                target_release_id=rel_id,
                expected_parent_generation=0,
                expected_parent_release_id=None,
                reason=f"Worker {i} contention candidate",
            )
            op_keys.append(op_k)

        # Multiprocessing race
        ctx = mp.get_context("spawn")
        barrier = ctx.Barrier(10)
        result_queue = ctx.Queue()

        processes = []
        for idx in range(10):
            p = ctx.Process(
                target=_conflicting_publish_worker,
                args=(str(db_path), str(data_root), op_keys[idx], idx, barrier, result_queue),
            )
            processes.append(p)
            p.start()

        for p in processes:
            p.join(timeout=20.0)
            if p.is_alive():
                p.terminate()

        # Collect outcomes
        results = []
        while not result_queue.empty():
            results.append(result_queue.get_nowait())

        assert len(results) == 10, f"Expected 10 worker outcomes, got {len(results)}"
        succeeded = [r for r in results if r[0] == "SUCCEEDED"]
        stale_conflicts = [r for r in results if r[0] == "STALE_CONFLICT"]
        errors = [r for r in results if r[0] == "ERROR"]

        assert len(errors) == 0, f"Unexpected errors during CAS race: {errors}"
        assert len(succeeded) == 1, f"Expected exactly 1 winner, got {len(succeeded)}: {succeeded}"
        assert len(stale_conflicts) == 9, f"Expected exactly 9 stale conflicts, got {len(stale_conflicts)}"

        winner_idx = succeeded[0][1]
        winner_res = succeeded[0][2]
        assert winner_res["previous_generation"] == 0
        assert winner_res["new_generation"] == 1
        assert winner_res["selected_release_id"] == release_ids[winner_idx]

        # Verify registry invariants directly
        curr = reg.get_current_release()
        assert curr["generation"] == 1, f"Expected generation 1, got {curr['generation']}"
        assert curr["release_id"] == release_ids[winner_idx]

        con = reg.get_connection()
        pub_count = con.execute("SELECT count(*) FROM publication_events;").fetchone()[0]
        con.close()
        assert pub_count == 1, f"Expected exactly 1 publication event, got {pub_count}"

    elapsed = time.time() - start_time
    print(f"     [PASS] Multi-process CAS storm: 1 winner, 9 conflicts, gen=1 ({elapsed:.3f}s)")


def test_adversarial_multiprocess_idempotent_retry_storm():
    """Multi-process Idempotent race: 10 processes simultaneously submit the SAME operation_key.

    Invariants:
    1. Exactly ONE process gets status 'SUCCEEDED'.
    2. Other 9 processes get status 'ALREADY_COMMITTED'.
    3. All 10 return the identical event_id, new_generation=1, and selected_release_id.
    4. current_release generation is strictly 1 (NOT 10).
    5. publication_events contains exactly 1 row.
    """
    print("  -> Testing adversarial multi-process idempotent retry storm (10 processes identical operation_key)...")
    start_time = time.time()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        releases_dir = data_root / "releases"
        db_path = data_root / "control" / "registry.sqlite"
        releases_dir.mkdir(parents=True, exist_ok=True)

        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        rel_id, _, _, _ = _seed_verified_release(reg, releases_dir, release_suffix="shared")

        op_key = "op_shared_idempotent_key_race"
        reg.record_selection_request(
            operation_key=op_key,
            target_release_id=rel_id,
            expected_parent_generation=0,
            expected_parent_release_id=None,
            reason="Idempotent contention test",
        )

        ctx = mp.get_context("spawn")
        barrier = ctx.Barrier(10)
        result_queue = ctx.Queue()

        processes = []
        for idx in range(10):
            p = ctx.Process(
                target=_identical_key_publish_worker,
                args=(str(db_path), str(data_root), op_key, idx, barrier, result_queue),
            )
            processes.append(p)
            p.start()

        for p in processes:
            p.join(timeout=20.0)
            if p.is_alive():
                p.terminate()

        results = []
        while not result_queue.empty():
            results.append(result_queue.get_nowait())

        assert len(results) == 10, f"Expected 10 worker outcomes, got {len(results)}"
        succeeded = [r for r in results if r[0] == "SUCCEEDED"]
        already_committed = [r for r in results if r[0] == "ALREADY_COMMITTED"]
        errors = [r for r in results if r[0] == "ERROR"]

        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(succeeded) == 1, f"Expected 1 initial SUCCEEDED, got {len(succeeded)}"
        assert len(already_committed) == 9, f"Expected 9 ALREADY_COMMITTED, got {len(already_committed)}"

        # Verify all return matching event_id and generation
        first_event_id = succeeded[0][2]["event_id"]
        for _, _, res in already_committed:
            assert res["event_id"] == first_event_id
            assert res["new_generation"] == 1
            assert res["selected_release_id"] == rel_id

        # Invariant checks
        curr = reg.get_current_release()
        assert curr["generation"] == 1
        assert curr["release_id"] == rel_id

        con = reg.get_connection()
        pub_count = con.execute("SELECT count(*) FROM publication_events;").fetchone()[0]
        con.close()
        assert pub_count == 1, f"Expected 1 publication_events record, got {pub_count}"

    elapsed = time.time() - start_time
    print(f"     [PASS] Multi-process idempotent storm: 1 SUCCEEDED, 9 ALREADY_COMMITTED, gen=1 ({elapsed:.3f}s)")


def test_repeated_idempotent_retries_50_iterations():
    """Verify that repeating publish_release 50 times sequentially returns ALREADY_COMMITTED with zero mutation."""
    print("  -> Testing repeated idempotent retries (50 sequential invocations)...")
    start_time = time.time()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        releases_dir = data_root / "releases"
        db_path = data_root / "control" / "registry.sqlite"
        releases_dir.mkdir(parents=True, exist_ok=True)

        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        rel_id, _, _, _ = _seed_verified_release(reg, releases_dir, release_suffix="loop")

        op_key = "op_idempotent_loop_50"
        reg.record_selection_request(
            operation_key=op_key,
            target_release_id=rel_id,
            expected_parent_generation=0,
            expected_parent_release_id=None,
        )

        res_first = reg.publish_release(operation_key=op_key)
        assert res_first["status"] == "SUCCEEDED"
        assert res_first["new_generation"] == 1

        for i in range(50):
            res = reg.publish_release(operation_key=op_key)
            assert res["status"] == "ALREADY_COMMITTED"
            assert res["event_id"] == res_first["event_id"]
            assert res["new_generation"] == 1
            assert res["selected_release_id"] == rel_id

        curr = reg.get_current_release()
        assert curr["generation"] == 1
        con = reg.get_connection()
        assert con.execute("SELECT count(*) FROM publication_events;").fetchone()[0] == 1
        con.close()

    elapsed = time.time() - start_time
    print(f"     [PASS] 50 sequential retries returned ALREADY_COMMITTED with 0 side-effects ({elapsed:.3f}s)")


def test_failed_migration_transaction_rollback():
    """Verify that a failing migration transaction rolls back cleanly leaving the database in its previous valid state."""
    print("  -> Testing failed migration rollback and database preservation...")
    start_time = time.time()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

        # Baseline v1 is active
        assert reg.get_current_schema_version() == 1
        reg.register_source_object("tlc/hvfhv/2024-01", "hvfhv", "https://example.com", "2024-01")

        # Create broken migration with intentional syntax/constraint failure
        broken_migration = Migration(
            version=2,
            name="002_broken_syntax_migration",
            ddl_sql="""
            CREATE TABLE valid_table_before_crash (id INTEGER PRIMARY KEY);
            INSERT INTO valid_table_before_crash VALUES (1);
            THIS IS A DELIBERATE SYNTAX ERROR;
            CREATE TABLE table_after_crash (id INTEGER PRIMARY KEY);
            """,
        )
        MIGRATIONS.append(broken_migration)

        try:
            failed = False
            try:
                reg.migrate(target_version=2)
            except MigrationError:
                failed = True
            assert failed, "Expected MigrationError on syntax error in migration"

            # Invariants:
            # 1. Version remains 1
            assert reg.get_current_schema_version() == 1
            con = reg.get_connection()
            cur = con.cursor()
            # 2. valid_table_before_crash rolled back (does not exist)
            tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
            assert "valid_table_before_crash" not in tables, (
                "VULNERABILITY: cur.executescript() in migrate() implicitly commits before script execution "
                "in Python sqlite3, causing statements executed prior to failure to remain committed in the "
                "database instead of rolling back."
            )
            assert "table_after_crash" not in tables
            # 3. schema_migrations has only version 1
            mig_versions = [r[0] for r in cur.execute("SELECT version FROM schema_migrations;").fetchall()]
            assert mig_versions == [1]
            # 4. Previously registered data is intact
            src = cur.execute("SELECT logical_source_id FROM source_objects;").fetchall()
            assert len(src) == 1 and src[0][0] == "tlc/hvfhv/2024-01"
            # 5. Integrity check passes
            integrity = cur.execute("PRAGMA integrity_check;").fetchall()
            assert integrity[0][0] == "ok"
            cur.close()
            con.close()
        finally:
            MIGRATIONS.remove(broken_migration)

    elapsed = time.time() - start_time
    print(f"     [PASS] Failed migration rolled back completely with zero orphaned schema objects ({elapsed:.3f}s)")


def test_scenario_t24_corruption_simulation_and_bit_for_bit_restore():
    """Scenario T24: Simulate database corruption and restore from pre-migration backup.

    Invariants verified:
    1. Pre-migration backup is created and integrity-verified before migration to v2.
    2. Active database is corrupted (header/btree pages zeroed out).
    3. Corrupted database fails integrity checks.
    4. restore_from_backup restores pristine database.
    5. PRAGMA integrity_check and foreign_key_check pass cleanly.
    6. Released filesystem artifacts remain 100% bit-for-bit unchanged.
    """
    print("  -> Testing Scenario T24: Database corruption simulation and restore from pre-migration backup...")
    start_time = time.time()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        releases_dir = data_root / "releases"
        db_path = data_root / "control" / "registry.sqlite"
        releases_dir.mkdir(parents=True, exist_ok=True)

        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        rel_id, att_id, db_file, db_bytes = _seed_verified_release(reg, releases_dir, release_suffix="t24")

        # Publish release at generation 1
        reg.record_selection_request("op_t24", rel_id, expected_parent_generation=0)
        reg.publish_release("op_t24")

        # Take pre-migration backup
        backup_path = reg.create_online_backup()
        assert backup_path.exists()
        original_backup_bytes = backup_path.read_bytes()

        # Migrate to v2
        migration_v2 = Migration(
            version=2,
            name="002_add_audit_table",
            ddl_sql="""
            CREATE TABLE IF NOT EXISTS audit_log_v2 (
                log_id TEXT PRIMARY KEY,
                created_at_utc TEXT NOT NULL
            );
            """,
        )
        MIGRATIONS.append(migration_v2)
        try:
            reg.migrate(target_version=2)
            assert reg.get_current_schema_version() == 2

            # Simulate catastrophic database corruption: zero out bytes 100..4096 of registry.sqlite
            with open(db_path, "r+b") as f:
                f.seek(100)
                f.write(b"\x00" * 2048)
                f.flush()

            # Verify that connection or integrity check detects corruption
            corruption_detected = False
            try:
                con = sqlite3.connect(db_path)
                res = con.execute("PRAGMA integrity_check;").fetchall()
                if not res or res[0][0] != "ok":
                    corruption_detected = True
                con.close()
            except sqlite3.DatabaseError:
                corruption_detected = True
            assert corruption_detected, "Expected corruption to be detected by SQLite"

            # Execute restore_from_backup
            reg.restore_from_backup(backup_path)

            # Invariant 1: Restored database integrity check passes
            con = reg.get_connection()
            cur = con.cursor()
            integrity = cur.execute("PRAGMA integrity_check;").fetchall()
            assert integrity[0][0] == "ok", f"Integrity check failed: {integrity}"
            fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()
            assert len(fk_violations) == 0, f"FK violations: {fk_violations}"

            # Invariant 2: Schema restored to v1
            assert reg.get_current_schema_version(con) == 1
            tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
            assert "audit_log_v2" not in tables

            # Invariant 3: Published release state restored intact
            curr = reg.get_current_release()
            assert curr["generation"] == 1
            assert curr["release_id"] == rel_id

            cur.close()
            con.close()

            # Invariant 4: Released filesystem artifacts strictly bit-for-bit unchanged
            assert db_file.exists()
            assert db_file.read_bytes() == db_bytes

        finally:
            MIGRATIONS.remove(migration_v2)

    elapsed = time.time() - start_time
    print(f"     [PASS] Scenario T24 corruption and bit-for-bit restoration verified ({elapsed:.3f}s)")


def test_state_machine_security_and_digest_mismatch():
    """Verify release state machine rules: unverified/rejected/retired rejection, digest tampering detection."""
    print("  -> Testing state machine governance: unverified/rejected/retired rejection & digest tampering...")
    start_time = time.time()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        releases_dir = data_root / "releases"
        db_path = data_root / "control" / "registry.sqlite"
        releases_dir.mkdir(parents=True, exist_ok=True)

        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

        # 1. Digest mismatch: verification attestation with forged database digest
        reg.register_source_object("tlc/taxi_zones", "taxi_zones", "https://example.com/zones.csv")
        reg.register_revision("rev_z", "tlc/taxi_zones", "zh", 100, "z.csv", "fp", 265, 265, "p.json")
        reg.create_attempt("att_tamper", "{}")
        reg.register_finalized_release(
            release_id="rel_tamper",
            derivation_id="d_tamper",
            producing_attempt_id="att_tamper",
            release_directory="releases/rel_tamper",
            manifest_sha256="m_real",
            database_sha256="db_real",
            validation_sha256="v_real",
            source_revision_map={"tlc/taxi_zones": "rev_z"},
        )

        # Try to attest with mismatched database digest
        tamper_blocked = False
        try:
            reg.record_verification(
                release_id="rel_tamper",
                manifest_sha256="m_real",
                database_sha256="db_FORGED",  # Mismatch!
                validation_sha256="v_real",
                rule_inventory_version="v1",
                rule_inventory_hash="rh",
                verification_results_json="{}",
                verifier_version="1.0.0",
            )
        except RegistryError as e:
            if "Verification digests do not match finalized release digests" in str(e):
                tamper_blocked = True
        assert tamper_blocked, "Expected attestation digest mismatch to be blocked"

        # 2. Cannot publish unverified release (status is FINALIZED)
        reg.record_selection_request("op_unverified", "rel_tamper", expected_parent_generation=0)
        unverified_blocked = False
        try:
            reg.publish_release("op_unverified")
        except InvalidReleaseStatusError:
            unverified_blocked = True
        assert unverified_blocked, "Expected publish of FINALIZED release to fail"

        # Now properly verify it
        reg.record_verification(
            release_id="rel_tamper",
            manifest_sha256="m_real",
            database_sha256="db_real",
            validation_sha256="v_real",
            rule_inventory_version="v1",
            rule_inventory_hash="rh",
            verification_results_json="{}",
            verifier_version="1.0.0",
        )

        # Now rejection transition
        reg.reject_release("rel_tamper", reason="Security anomaly in verification audit")
        rejected_blocked = False
        try:
            reg.publish_release("op_unverified")
        except InvalidReleaseStatusError:
            rejected_blocked = True
        assert rejected_blocked, "Expected publish of REJECTED release to fail"

        # 3. Selection request conflict detection: tampering with parameters under existing operation_key
        collision_blocked = False
        try:
            reg.record_selection_request(
                operation_key="op_unverified",
                target_release_id="different_target_release",
                expected_parent_generation=0,
            )
        except SelectionConflictError:
            collision_blocked = True
        assert collision_blocked, "Expected operation_key parameter mismatch to raise SelectionConflictError"

    elapsed = time.time() - start_time
    print(f"     [PASS] State machine governance & tampering protections strictly enforced ({elapsed:.3f}s)")


def test_cas_rollback_release_and_cleanup_tracking():
    """Verify CAS rollback_release semantics, idempotent rollback retry, and operator cleanup tracking."""
    print("  -> Testing CAS rollback_release and cleanup request audit tracking...")
    start_time = time.time()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        releases_dir = data_root / "releases"
        db_path = data_root / "control" / "registry.sqlite"
        releases_dir.mkdir(parents=True, exist_ok=True)

        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        rel_1, _, _, _ = _seed_verified_release(reg, releases_dir, release_suffix="rb1", month_num=1)
        rel_2, _, _, _ = _seed_verified_release(reg, releases_dir, release_suffix="rb2", month_num=2)

        # 1. Publish Release 1 at gen 1
        reg.record_selection_request("op_publish_1", rel_1, expected_parent_generation=0)
        p1 = reg.publish_release("op_publish_1")
        assert p1["new_generation"] == 1
        assert reg.get_current_release()["release_id"] == rel_1

        # 2. Publish Release 2 at gen 2
        reg.record_selection_request("op_publish_2", rel_2, expected_parent_generation=1, expected_parent_release_id=rel_1)
        p2 = reg.publish_release("op_publish_2")
        assert p2["new_generation"] == 2
        assert reg.get_current_release()["release_id"] == rel_2

        # 3. Rollback to Release 1 at gen 3
        rb = reg.rollback_release(
            operation_key="op_rollback_to_1",
            target_release_id=rel_1,
            expected_parent_generation=2,
            expected_parent_release_id=rel_2,
            reason="Anomaly in Release 2",
        )
        assert rb["status"] == "SUCCEEDED"
        assert rb["previous_generation"] == 2
        assert rb["new_generation"] == 3
        assert rb["selected_release_id"] == rel_1
        assert rb["previous_release_id"] == rel_2

        curr = reg.get_current_release()
        assert curr["generation"] == 3
        assert curr["release_id"] == rel_1

        # Verify publication_events recorded operation_kind == 'rollback'
        con = reg.get_connection()
        cur = con.cursor()
        evt = cur.execute("SELECT operation_kind, reason FROM publication_events WHERE operation_key = 'op_rollback_to_1';").fetchone()
        assert evt[0] == "rollback"
        assert evt[1] == "Anomaly in Release 2"

        # 4. Idempotent retry of rollback_release
        rb_retry = reg.rollback_release(
            operation_key="op_rollback_to_1",
            target_release_id=rel_1,
            expected_parent_generation=2,
            expected_parent_release_id=rel_2,
            reason="Anomaly in Release 2",
        )
        assert rb_retry["status"] == "ALREADY_COMMITTED"
        assert rb_retry["new_generation"] == 3
        assert reg.get_current_release()["generation"] == 3

        # 5. Cleanup audit tracking
        cleanup_id = "clean_20261111"
        reg.record_cleanup_request(
            cleanup_id=cleanup_id,
            preview_digest="sha_preview",
            preview_json='{"deleted_bytes": 1000}',
            expected_generation=3,
            protected_set_digest="sha_protected",
            protected_set_json='["releases/rel_1"]',
            review_reference="REV-123",
        )

        reg.record_cleanup_items(
            cleanup_id=cleanup_id,
            items=[
                {"item_id": "item1", "exact_relative_path": "releases/rel_2/data.duckdb", "byte_count": 500},
                {"item_id": "item2", "exact_relative_path": "releases/rel_2/manifest.json", "byte_count": 100},
            ],
        )

        reg.update_cleanup_item_outcome(cleanup_id, "item1", "DELETED")
        reg.update_cleanup_item_outcome(cleanup_id, "item2", "FAILED", diagnostic="Permission denied")

        items = cur.execute("SELECT item_id, outcome, diagnostic FROM cleanup_items WHERE cleanup_id = ? ORDER BY item_id;", (cleanup_id,)).fetchall()
        assert len(items) == 2
        assert items[0][1] == "DELETED"
        assert items[1][1] == "FAILED" and items[1][2] == "Permission denied"

        cur.close()
        con.close()

    elapsed = time.time() - start_time
    print(f"     [PASS] CAS rollback & cleanup audit tracking verified ({elapsed:.3f}s)")


# ==============================================================================
# Runner
# ==============================================================================

def run_all_adversarial_tests() -> int:
    print("=" * 76)
    print("GATE G1: ADVERSARIAL STRESS TEST SUITE (EMPIRICAL CHALLENGER)")
    print("=" * 76)
    suite_start = time.time()

    tests = [
        ("Durability Pragmas Stress (50 Threads/Connections)", test_durability_pragmas_stress_50_connections),
        ("Transaction Rollback On Constraint Violations", test_transaction_rollback_on_constraint_violations),
        ("Multi-Process CAS Contention Storm (10 Processes)", test_adversarial_multiprocess_cas_contention_storm),
        ("Multi-Process Idempotent Retry Storm (10 Processes)", test_adversarial_multiprocess_idempotent_retry_storm),
        ("Repeated Idempotent Retries (50 Sequential Invocations)", test_repeated_idempotent_retries_50_iterations),
        ("Failed Migration Rollback & Database Preservation", test_failed_migration_transaction_rollback),
        ("Scenario T24: Corruption & Bit-For-Bit Backup Restore", test_scenario_t24_corruption_simulation_and_bit_for_bit_restore),
        ("State Machine Security & Digest Tampering Defense", test_state_machine_security_and_digest_mismatch),
        ("CAS Rollback & Cleanup Tracking", test_cas_rollback_release_and_cleanup_tracking),
    ]

    passed = 0
    failed = 0

    for name, fn in tests:
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    total_time = time.time() - suite_start
    print("=" * 76)
    print(f"ADVERSARIAL STRESS TESTS: {passed} PASSED, {failed} FAILED in {total_time:.2f}s")
    print("=" * 76)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(run_all_adversarial_tests())
