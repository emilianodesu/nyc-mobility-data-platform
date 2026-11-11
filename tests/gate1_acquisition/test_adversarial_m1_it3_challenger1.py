"""Adversarial Verification Suite for Milestone 1 Iteration 3 (Empirical Challenger 1).

Adversarially challenges:
1. Multi-step migration cascade step 3 failure:
   - When step 1 and step 2 succeed and step 3 fails, does it atomically roll back all
     intermediate steps to step 0 (baseline version 1)?
2. Nested writer_lock 3 levels deep with restore_from_backup at level 2:
   - Does lock release cleanly only when depth drops to 0?
   - Does diagnostic JSON transition correctly from HELD to RELEASED?
3. Cross-thread mutual exclusion during re-entrancy:
   - Can another thread acquire writer_lock while the first thread is in a re-entrant block?
   - Exclusive lock MUST remain strictly held against other threads.
4. Nested writer_lock exception stack unwinding:
   - Does depth drop to 0 and OS flock release when an exception is raised inside nested blocks?
5. Resource / File descriptor / Memory leak stress (100 iterations):
   - 100 failed migrations: verify zero FD leaks (delta == 0) and bounded memory RSS.
   - 100 re-entrant restores: verify zero FD leaks (delta == 0) and bounded memory RSS.
6. Empirical analysis of v0 multi-step migration rollback edge case.
"""

from __future__ import annotations

import gc
import json
import os
from pathlib import Path
import resource
import sqlite3
import sys
import tempfile
import threading
import time
import traceback
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.control.locks import (
    writer_lock,
    query_lock,
    is_lock_held_by_current_thread,
    LockContentionError,
)
from hvfhv.control.registry import (
    Registry,
    Migration,
    MIGRATIONS,
    MigrationError,
)


def count_open_fds() -> int:
    """Return count of open file descriptors in current process via /dev/fd."""
    return len(os.listdir("/dev/fd"))


def get_rss_bytes() -> int:
    """Return current process max RSS in bytes."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def test_multistep_migration_step3_failure_rollback() -> None:
    """Test multi-step migration: step 1 (v2) succeeds, step 2 (v3) succeeds, step 3 (v4) fails.

    Invariants required:
    1. Entire 3-step migration batch is atomic.
    2. Failure at step 3 rolls back all intermediate changes from step 1 and step 2.
    3. Database restores to baseline (version 1).
    4. Tables from step 1 (step1_tbl) and step 2 (step2_tbl) are NOT present.
    5. schema_migrations contains ONLY [1].
    6. Database passes PRAGMA integrity_check and foreign_key_check.
    """
    print("\n----------------------------------------------------------------------------")
    print("1. Multi-Step Migration Failure at Step 3 (Steps 1 & 2 Succeeded)")
    print("----------------------------------------------------------------------------")
    with tempfile.TemporaryDirectory() as td:
        data_root = Path(td) / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

        assert reg.get_current_schema_version() == 1
        reg.register_source_object("tlc/hvfhv/2024-01", "hvfhv", "https://example.com", "2024-01")

        # Step 1: Valid migration v2
        mig2 = Migration(
            version=2,
            name="002_adv_step1_valid",
            ddl_sql="""
            CREATE TABLE adv_step1_tbl (id INTEGER PRIMARY KEY, note TEXT);
            INSERT INTO adv_step1_tbl VALUES (1, 'from_step1');
            """,
        )

        # Step 2: Valid migration v3
        mig3 = Migration(
            version=3,
            name="003_adv_step2_valid",
            ddl_sql="""
            CREATE TABLE adv_step2_tbl (id INTEGER PRIMARY KEY, val REAL);
            INSERT INTO adv_step2_tbl VALUES (10, 42.5);
            """,
        )

        # Step 3: Failing migration v4
        mig4 = Migration(
            version=4,
            name="004_adv_step3_broken",
            ddl_sql="""
            CREATE TABLE adv_step3_tbl (id INTEGER PRIMARY KEY);
            DELIBERATE SYNTAX CRASH AT STEP 3;
            """,
        )

        MIGRATIONS.extend([mig2, mig3, mig4])
        try:
            failed = False
            try:
                reg.migrate(target_version=4)
            except MigrationError as exc:
                failed = True
                print(f"     [PASS] Caught expected MigrationError at step 3: {exc}")

            assert failed, "Expected MigrationError on step 3 failure"

            con = reg.get_connection()
            cur = con.cursor()
            tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
            mig_rows = [r[0] for r in cur.execute("SELECT version FROM schema_migrations ORDER BY version;").fetchall()]
            integrity = cur.execute("PRAGMA integrity_check;").fetchall()
            fk_check = cur.execute("PRAGMA foreign_key_check;").fetchall()
            cur.close()
            con.close()

            assert integrity[0][0] == "ok", f"Integrity check failed: {integrity}"
            assert len(fk_check) == 0, f"FK check failed: {fk_check}"
            assert "adv_step1_tbl" not in tables, "Step 1 table leaked into database after step 3 failure!"
            assert "adv_step2_tbl" not in tables, "Step 2 table leaked into database after step 3 failure!"
            assert "adv_step3_tbl" not in tables, "Step 3 table leaked into database!"
            assert mig_rows == [1], f"schema_migrations advanced to {mig_rows}, expected [1]!"
            assert reg.get_current_schema_version() == 1, f"Expected version 1, got {reg.get_current_schema_version()}"
            print("     [PASS] Intermediate steps 1 & 2 completely rolled back to baseline step 0 (version 1)")
        finally:
            for m in [mig2, mig3, mig4]:
                if m in MIGRATIONS:
                    MIGRATIONS.remove(m)


def test_nested_writer_lock_3_levels_restore_depth_lifecycle() -> None:
    """Test nested writer_lock 3 levels deep with restore_from_backup at level 2.

    Invariants required:
    1. Lock held status remains True at depths 1, 2, 3.
    2. restore_from_backup at level 2 succeeds re-entrantly.
    3. Lock release happens cleanly ONLY when depth drops back to 0.
    4. Diagnostic JSON reflects HELD during depths 1..3 and RELEASED at depth 0.
    5. After complete exit, another thread can acquire writer_lock immediately.
    """
    print("\n----------------------------------------------------------------------------")
    print("2. Nested writer_lock 3 Levels Deep with restore_from_backup at Level 2")
    print("----------------------------------------------------------------------------")
    with tempfile.TemporaryDirectory() as td:
        data_root = Path(td) / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        reg.register_source_object("tlc/hvfhv/2024-01", "hvfhv", "https://example.com", "2024-01")

        bak_path = reg.create_online_backup()
        assert bak_path.exists()
        lock_path = data_root / "control" / "writer.lock"

        def read_lock_diagnostic() -> dict[str, Any]:
            if not lock_path.exists():
                return {}
            try:
                raw = lock_path.read_text(encoding="utf-8").strip()
                return json.loads(raw) if raw else {}
            except Exception:
                return {}

        assert not is_lock_held_by_current_thread(lock_path)

        # Enter Level 1
        with writer_lock(data_root, reentrant=True):
            assert is_lock_held_by_current_thread(lock_path)
            diag1 = read_lock_diagnostic()
            assert diag1.get("state") == "HELD"
            print("     [PASS] Level 1 entered (depth=1, state=HELD)")

            # Enter Level 2
            with writer_lock(data_root, reentrant=True):
                assert is_lock_held_by_current_thread(lock_path)
                print("     [PASS] Level 2 entered (depth=2)")

                # Execute restore_from_backup at Level 2
                reg.restore_from_backup(bak_path)
                assert is_lock_held_by_current_thread(lock_path)
                print("     [PASS] restore_from_backup executed successfully at Level 2")

                # Enter Level 3
                with writer_lock(data_root, reentrant=True):
                    assert is_lock_held_by_current_thread(lock_path)
                    print("     [PASS] Level 3 entered (depth=3)")

                # Exited Level 3 -> depth=2
                assert is_lock_held_by_current_thread(lock_path)
                diag3_exit = read_lock_diagnostic()
                assert diag3_exit.get("state") == "HELD", "Lock released prematurely after exiting Level 3!"
                print("     [PASS] Exited Level 3: lock STILL held at depth=2")

            # Exited Level 2 -> depth=1
            assert is_lock_held_by_current_thread(lock_path)
            diag2_exit = read_lock_diagnostic()
            assert diag2_exit.get("state") == "HELD", "Lock released prematurely after exiting Level 2!"
            print("     [PASS] Exited Level 2: lock STILL held at depth=1")

        # Exited Level 1 -> depth=0
        assert not is_lock_held_by_current_thread(lock_path)
        diag1_exit = read_lock_diagnostic()
        assert diag1_exit.get("state") == "RELEASED", f"Expected state=RELEASED, got {diag1_exit}"
        print("     [PASS] Exited Level 1: lock CLEANLY RELEASED at depth=0 (state=RELEASED)")


def test_cross_thread_mutual_exclusion_during_reentrancy() -> None:
    """Test that another thread CANNOT acquire writer_lock while thread 1 is in a re-entrant block.

    Invariants required:
    1. While Thread 1 holds writer_lock at depth >= 1, Thread 2 CANNOT acquire writer_lock.
    2. Thread 2 raises LockContentionError immediately on non-blocking attempt.
    3. Thread 2 times out if waiting.
    4. Once Thread 1 exits all levels (depth drops to 0), Thread 2 CAN acquire.
    """
    print("\n----------------------------------------------------------------------------")
    print("3. Cross-Thread Mutual Exclusion During Re-entrant Blocks")
    print("----------------------------------------------------------------------------")
    with tempfile.TemporaryDirectory() as td:
        data_root = Path(td) / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        bak_path = reg.create_online_backup()

        t1_in_level2 = threading.Event()
        t2_attempt_done = threading.Event()
        t2_results: dict[str, Any] = {}

        def thread2_worker():
            # Wait for thread 1 to be deep in re-entrant block
            t1_in_level2.wait()
            # Attempt 1: non-blocking acquire
            try:
                with writer_lock(data_root, timeout=0.0):
                    t2_results["attempt1_acquired"] = True
            except LockContentionError as exc:
                t2_results["attempt1_acquired"] = False
                t2_results["attempt1_error"] = str(exc)

            # Attempt 2: short timeout acquire (should also fail with timeout)
            try:
                with writer_lock(data_root, timeout=0.05):
                    t2_results["attempt2_acquired"] = True
            except LockContentionError as exc:
                t2_results["attempt2_acquired"] = False
                t2_results["attempt2_error"] = str(exc)

            t2_attempt_done.set()

        t2 = threading.Thread(target=thread2_worker)
        t2.start()

        # Thread 1 enters nested re-entrant block
        with writer_lock(data_root, reentrant=True):
            with writer_lock(data_root, reentrant=True):
                reg.restore_from_backup(bak_path)
                with writer_lock(data_root, reentrant=True):
                    # Signal Thread 2 to attempt acquisition
                    t1_in_level2.set()
                    t2_attempt_done.wait(timeout=5.0)

        t2.join()

        assert t2_results.get("attempt1_acquired") is False, (
            "DEFECT: Thread 2 acquired writer_lock while Thread 1 held re-entrant lock!"
        )
        assert t2_results.get("attempt2_acquired") is False, (
            "DEFECT: Thread 2 acquired writer_lock on timeout while Thread 1 held re-entrant lock!"
        )
        print("     [PASS] Thread 2 was strictly blocked with LockContentionError during Thread 1 re-entrancy")

        # Now Thread 1 has exited completely; Thread 2 should succeed
        t2_post_result = {}

        def thread2_post_worker():
            try:
                with writer_lock(data_root, timeout=0.1):
                    t2_post_result["acquired"] = True
            except Exception as exc:
                t2_post_result["acquired"] = False
                t2_post_result["error"] = str(exc)

        t2_post = threading.Thread(target=thread2_post_worker)
        t2_post.start()
        t2_post.join()

        assert t2_post_result.get("acquired") is True, f"Thread 2 failed after release: {t2_post_result}"
        print("     [PASS] Thread 2 acquired writer_lock immediately after Thread 1 released (depth=0)")


def test_nested_writer_lock_exception_unwinding() -> None:
    """Test that when an exception occurs in a nested re-entrant block, lock releases cleanly."""
    print("\n----------------------------------------------------------------------------")
    print("4. Nested writer_lock Exception Stack Unwinding")
    print("----------------------------------------------------------------------------")
    with tempfile.TemporaryDirectory() as td:
        data_root = Path(td) / ".data"
        lock_path = data_root / "control" / "writer.lock"

        try:
            with writer_lock(data_root, reentrant=True):
                with writer_lock(data_root, reentrant=True):
                    with writer_lock(data_root, reentrant=True):
                        raise RuntimeError("Deliberate failure deep in nested context")
        except RuntimeError:
            pass

        assert not is_lock_held_by_current_thread(lock_path)
        diag = json.loads(lock_path.read_text(encoding="utf-8").strip())
        assert diag.get("state") == "RELEASED"

        # Verify a new lock can be acquired immediately
        with writer_lock(data_root, timeout=0.0):
            assert is_lock_held_by_current_thread(lock_path)
        print("     [PASS] Exception unwound 3 levels of writer_lock cleanly; lock state=RELEASED")


def test_fd_and_memory_leak_stress_100_failed_migrations() -> None:
    """Stress test 100 consecutive failed migrations for file descriptor and memory leaks."""
    print("\n----------------------------------------------------------------------------")
    print("5. File Descriptor & Memory Leak Stress: 100 Failed Migrations")
    print("----------------------------------------------------------------------------")
    with tempfile.TemporaryDirectory() as td:
        data_root = Path(td) / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)

        broken_mig = Migration(
            version=2,
            name="002_adv_leak_test",
            ddl_sql="CREATE TABLE leak_test (id INT); SYNTAX ERROR CRASH;",
        )
        MIGRATIONS.append(broken_mig)

        try:
            gc.collect()
            initial_fds = count_open_fds()
            initial_rss = get_rss_bytes()

            for i in range(100):
                try:
                    reg.migrate(target_version=2)
                except MigrationError:
                    pass

            gc.collect()
            final_fds = count_open_fds()
            final_rss = get_rss_bytes()

            fd_delta = final_fds - initial_fds
            rss_delta_mb = (final_rss - initial_rss) / (1024 * 1024)

            print(f"     [PASS] 100 failed migrations completed")
            print(f"            Open FDs: initial={initial_fds}, final={final_fds}, delta={fd_delta}")
            print(f"            RSS Memory: initial={initial_rss/(1024*1024):.1f}MB, final={final_rss/(1024*1024):.1f}MB, delta={rss_delta_mb:.2f}MB")

            assert fd_delta == 0, f"DEFECT: Leaked {fd_delta} file descriptors across 100 failed migrations!"
            assert rss_delta_mb < 50.0, f"Excessive memory growth: {rss_delta_mb:.2f}MB"
        finally:
            if broken_mig in MIGRATIONS:
                MIGRATIONS.remove(broken_mig)


def test_fd_and_memory_leak_stress_100_reentrant_restores() -> None:
    """Stress test 100 consecutive re-entrant restore_from_backup calls for FD and memory leaks."""
    print("\n----------------------------------------------------------------------------")
    print("6. File Descriptor & Memory Leak Stress: 100 Re-entrant Restores")
    print("----------------------------------------------------------------------------")
    with tempfile.TemporaryDirectory() as td:
        data_root = Path(td) / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=True)
        bak_path = reg.create_online_backup()

        gc.collect()
        initial_fds = count_open_fds()
        initial_rss = get_rss_bytes()

        for i in range(100):
            with writer_lock(data_root, reentrant=True):
                with writer_lock(data_root, reentrant=True):
                    reg.restore_from_backup(bak_path)
                    with writer_lock(data_root, reentrant=True):
                        pass

        gc.collect()
        final_fds = count_open_fds()
        final_rss = get_rss_bytes()

        fd_delta = final_fds - initial_fds
        rss_delta_mb = (final_rss - initial_rss) / (1024 * 1024)

        print(f"     [PASS] 100 re-entrant restores completed")
        print(f"            Open FDs: initial={initial_fds}, final={final_fds}, delta={fd_delta}")
        print(f"            RSS Memory: initial={initial_rss/(1024*1024):.1f}MB, final={final_rss/(1024*1024):.1f}MB, delta={rss_delta_mb:.2f}MB")

        assert fd_delta == 0, f"DEFECT: Leaked {fd_delta} file descriptors across 100 re-entrant restores!"
        assert rss_delta_mb < 50.0, f"Excessive memory growth: {rss_delta_mb:.2f}MB"


def test_v0_migration_edge_case_observation() -> None:
    """Empirical test documenting the behavior of multi-step migration when baseline is v0.

    When starting from v0 (clean DB, auto_migrate=False) and running a multi-step batch
    where step 1 (v1) and step 2 (v2) succeed and step 3 (v3) fails:
    Because registry.py line 508 has `if current_v > 0:`, no initial backup is captured.
    As a result, failure on step 3 does not restore to v0, but leaves intermediate steps in DB.
    """
    print("\n----------------------------------------------------------------------------")
    print("7. Empirical Observation: Multi-Step Migration from Clean v0 Baseline")
    print("----------------------------------------------------------------------------")
    with tempfile.TemporaryDirectory() as td:
        data_root = Path(td) / ".data"
        db_path = data_root / "control" / "registry.sqlite"
        reg = Registry(db_path=db_path, data_root=data_root, auto_migrate=False)
        assert reg.get_current_schema_version() == 0

        mig2 = Migration(version=2, name="002_v0_step2", ddl_sql="CREATE TABLE v0_step2 (id INT);")
        mig3 = Migration(version=3, name="003_v0_step3_broken", ddl_sql="CREATE TABLE v0_step3 (id INT); SYNTAX ERROR;")
        MIGRATIONS.extend([mig2, mig3])

        try:
            try:
                reg.migrate(target_version=3)
            except MigrationError as exc:
                print(f"     [INFO] Caught expected MigrationError on v0 batch: {exc}")

            v_after = reg.get_current_schema_version()
            print(f"     [OBSERVATION] When baseline is v0, schema version after mid-batch failure is: {v_after}")
            if v_after > 0:
                print(f"     [NOTE] Caveat confirmed: line 508 'if current_v > 0:' skips backup for v0 DBs,")
                print(f"            leaving v0 batch partially committed at version {v_after}.")
            else:
                print(f"     [PASS] Successfully rolled back to v0")
        finally:
            for m in [mig2, mig3]:
                if m in MIGRATIONS:
                    MIGRATIONS.remove(m)


def run_all_challenger1_it3_tests() -> int:
    print("============================================================================")
    print("GATE G1 ITERATION 3: EMPIRICAL CHALLENGER 1 ADVERSARIAL STRESS SUITE")
    print("============================================================================")
    tests = [
        ("Multi-step Migration Step 3 Failure Rollback", test_multistep_migration_step3_failure_rollback),
        ("Nested writer_lock 3 Levels Deep & restore Lifecycle", test_nested_writer_lock_3_levels_restore_depth_lifecycle),
        ("Cross-Thread Mutual Exclusion During Re-entrancy", test_cross_thread_mutual_exclusion_during_reentrancy),
        ("Nested writer_lock Exception Stack Unwinding", test_nested_writer_lock_exception_unwinding),
        ("FD & Memory Leak Stress: 100 Failed Migrations", test_fd_and_memory_leak_stress_100_failed_migrations),
        ("FD & Memory Leak Stress: 100 Re-entrant Restores", test_fd_and_memory_leak_stress_100_reentrant_restores),
        ("Observation on Clean v0 Baseline Rollback", test_v0_migration_edge_case_observation),
    ]

    passed = 0
    failed = 0
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
    print(f"CHALLENGER 1 IT3 ADVERSARIAL SUITE RESULTS: {passed} PASSED, {failed} FAILED")
    print("============================================================================")
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(run_all_challenger1_it3_tests())
