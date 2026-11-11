"""Gate G1 Iteration 2 Adversarial Stress Test Suite (Challenger 1).

Covers:
1. Re-execution of Challenger 1 Adversarial Control Plane Suite (9 tests).
2. Multi-step migration cascade atomic rollback:
   - When step 1 succeeds and step 2 fails, verify that database restores to state
     prior to the migration run (step 0 / pre-migration backup) and schema_migrations
     does NOT advance to step 1.
3. Re-entrancy stress on restore_from_backup:
   - Calling restore_from_backup(bak_path) while NOT holding writer_lock.
   - Calling restore_from_backup(bak_path) while holding writer_lock (re-entrancy).
4. SQLite cursor, connection handle, and lock leak stress across 50 failed migrations.
"""

from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import traceback

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.control.locks import writer_lock, LockContentionError
from hvfhv.control.registry import (
    Registry,
    Migration,
    MIGRATIONS,
    MigrationError,
)
from tests.gate1_acquisition.test_adversarial_control_plane import (
    run_all_adversarial_tests as run_original_challenger1_tests,
)


def test_reexecute_challenger1_suite() -> None:
    """Verify all 9 tests in Challenger 1 adversarial suite pass."""
    print("============================================================================")
    print("1. RE-EXECUTING CHALLENGER 1 ADVERSARIAL CONTROL PLANE SUITE")
    print("============================================================================")
    run_original_challenger1_tests()


def test_multistep_migration_cascade_atomic_rollback() -> None:
    """Multi-step migration cascade: step 1 succeeds, step 2 fails.

    Invariants required:
    1. Entire migration run is atomic.
    2. Database restores to state prior to the migration batch (pre-migration backup / step 0).
    3. schema_migrations does NOT advance to step 1.
    4. Schema modifications from step 1 (cascade_step1_table) are NOT retained.
    5. Database passes PRAGMA integrity_check and foreign_key_check.
    """
    print("\n============================================================================")
    print("2. TESTING MULTI-STEP MIGRATION CASCADE ATOMIC ROLLBACK")
    print("============================================================================")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

        assert reg.get_current_schema_version() == 1
        reg.register_source_object("tlc/hvfhv/2024-01", "hvfhv", "https://example.com", "2024-01")

        # Step 1: Valid migration (v2)
        mig2 = Migration(
            version=2,
            name="002_cascade_step1_valid",
            ddl_sql="""
            CREATE TABLE cascade_step1_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            INSERT INTO cascade_step1_table VALUES (1, 'initial_step1');
            """,
        )

        # Step 2: Broken migration (v3)
        mig3 = Migration(
            version=3,
            name="003_cascade_step2_broken",
            ddl_sql="""
            CREATE TABLE cascade_step2_table (
                id INTEGER PRIMARY KEY
            );
            THIS IS A DELIBERATE SYNTAX ERROR CRASH;
            """,
        )

        MIGRATIONS.extend([mig2, mig3])
        try:
            failed = False
            try:
                reg.migrate(target_version=3)
            except MigrationError as exc:
                failed = True
                print(f"     [PASS] Caught expected MigrationError: {exc}")

            assert failed, "Expected MigrationError on broken migration step 2"

            con = reg.get_connection()
            cur = con.cursor()
            tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
            mig_rows = [r[0] for r in cur.execute("SELECT version FROM schema_migrations ORDER BY version;").fetchall()]
            integrity = cur.execute("PRAGMA integrity_check;").fetchall()
            fk_check = cur.execute("PRAGMA foreign_key_check;").fetchall()
            cur.close()
            con.close()

            # Empirical checks
            assert integrity[0][0] == "ok", f"Corrupt database after rollback: {integrity}"
            assert len(fk_check) == 0, f"FK violations after rollback: {fk_check}"

            # Invariant check: Did the cascade roll back atomically?
            assert "cascade_step1_table" not in tables, (
                "DEFECT: Multi-step migration cascade is NOT atomic across the batch. "
                "Step 1 ('cascade_step1_table') remained committed in the database after Step 2 failed."
            )
            assert mig_rows == [1], (
                f"DEFECT: schema_migrations advanced to {mig_rows} despite mid-batch failure. "
                "Expected schema_migrations to remain at [1] (pre-migration state)."
            )
            assert reg.get_current_schema_version() == 1, (
                f"DEFECT: Current schema version is {reg.get_current_schema_version()}, expected 1."
            )
            print("     [PASS] Multi-step cascade rolled back atomically to pre-migration baseline (version 1)")
        finally:
            if mig2 in MIGRATIONS:
                MIGRATIONS.remove(mig2)
            if mig3 in MIGRATIONS:
                MIGRATIONS.remove(mig3)


def test_restore_from_backup_reentrancy_stress() -> None:
    """Test re-entrancy: call restore_from_backup while holding writer_lock and while NOT holding writer_lock.

    Invariants required:
    1. Calling restore_from_backup(bak_path) when NOT holding writer_lock succeeds.
    2. Calling restore_from_backup(bak_path) when ALREADY holding writer_lock succeeds
       without raising LockContentionError (re-entrancy support).
    """
    print("\n============================================================================")
    print("3. TESTING RESTORE_FROM_BACKUP RE-ENTRANCY STRESS")
    print("============================================================================")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        reg.register_source_object("tlc/hvfhv/2024-01", "hvfhv", "https://example.com", "2024-01")

        bak_path = reg.create_online_backup()
        assert bak_path.exists()

        # Case 1: NOT holding writer_lock
        print("  -> Case 1: restore_from_backup() without holding writer_lock...")
        reg.restore_from_backup(bak_path)
        print("     [PASS] Succeeded")

        # Case 2: ALREADY holding writer_lock
        print("  -> Case 2: restore_from_backup() while holding writer_lock (re-entrancy)...")
        with writer_lock(data_root):
            try:
                reg.restore_from_backup(bak_path)
                print("     [PASS] Re-entrant restore_from_backup() while holding writer_lock succeeded")
            except LockContentionError as exc:
                raise AssertionError(
                    f"DEFECT: restore_from_backup() is NOT re-entrant under writer_lock: {exc}"
                ) from exc


def test_cursor_connection_lock_leak_stress() -> None:
    """Verify that failed migrations never leak active cursors, connections, or file locks."""
    print("\n============================================================================")
    print("4. TESTING CURSOR, CONNECTION, AND LOCK LEAK STRESS (50 ITERATIONS)")
    print("============================================================================")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        reg.register_source_object("tlc/hvfhv/2024-01", "hvfhv", "https://example.com", "2024-01")

        broken_mig = Migration(
            version=2,
            name="002_leak_stress_broken",
            ddl_sql="""
            CREATE TABLE leak_test (id INTEGER PRIMARY KEY);
            INSERT INTO leak_test VALUES (1);
            DELIBERATE SYNTAX ERROR;
            """,
        )
        MIGRATIONS.append(broken_mig)

        try:
            for i in range(50):
                try:
                    reg.migrate(target_version=2)
                except MigrationError:
                    pass

                # Verify writer lock can be acquired immediately without contention
                with writer_lock(data_root, timeout=0.05) as lock:
                    pass

                # Verify database can be connected and written to by a separate connection
                con = reg.get_connection()
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(
                    "INSERT OR REPLACE INTO source_objects (logical_source_id, kind, source_month, official_url) VALUES (?, ?, ?, ?);",
                    (f"test_{i}", "hvfhv", "2024-01", "http://test.com"),
                )
                con.commit()
                cur.close()
                con.close()

            print("     [PASS] 50 consecutive failed migrations caused ZERO cursor, connection, or lock leaks")
        finally:
            if broken_mig in MIGRATIONS:
                MIGRATIONS.remove(broken_mig)


def main() -> int:
    print("============================================================================")
    print("GATE G1 ITERATION 2: ADVERSARIAL STRESS SUITE (EMPIRICAL CHALLENGER 1)")
    print("============================================================================")
    passed = 0
    failed = 0

    tests = [
        ("Re-execution of Challenger 1 Suite", test_reexecute_challenger1_suite),
        ("Multi-step Migration Cascade Atomic Rollback", test_multistep_migration_cascade_atomic_rollback),
        ("restore_from_backup Re-entrancy Stress", test_restore_from_backup_reentrancy_stress),
        ("Cursor, Connection, and Lock Leak Stress", test_cursor_connection_lock_leak_stress),
    ]

    for name, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            print(f"\n  [FAIL] {name}: {e}")
            failed += 1
        except Exception as e:
            print(f"\n  [ERROR] {name}: {e}")
            traceback.print_exc()
            failed += 1

    print("\n============================================================================")
    print(f"GATE G1 IT2 ADVERSARIAL RESULTS: {passed} PASSED, {failed} FAILED")
    print("============================================================================")
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
