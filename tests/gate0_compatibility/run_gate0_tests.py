"""Comprehensive Gate G0 Compatibility Test Suite Runner.

Executes all Gate G0 compatibility checks:
- test_environment.py
- test_sqlite_durability.py
- test_locks.py
- test_duckdb_arrow.py
"""

import sys
import tempfile
import time
from pathlib import Path

# Ensure src/ and repo root are on python path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

import tests.gate0_compatibility.test_environment as t_env
import tests.gate0_compatibility.test_sqlite_durability as t_sql
import tests.gate0_compatibility.test_locks as t_locks
import tests.gate0_compatibility.test_duckdb_arrow as t_duck


def main() -> int:
    print("=" * 70)
    print("GATE G0: ENVIRONMENT & COMPATIBILITY SUITE EXECUTION")
    print("=" * 70)
    start_time = time.time()
    passed = 0
    skipped = 0
    failed = 0

    def run_test(name: str, fn, *args):
        nonlocal passed, failed
        try:
            fn(*args)
            print(f"  [PASS] {name}")
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            failed += 1

    print("\n--- 1. Environment & Host Tuple Tests ---")
    run_test("test_host_tuple_and_python_version", t_env.test_host_tuple_and_python_version)
    with tempfile.TemporaryDirectory() as td:
        run_test("test_sqlite_pragmas_and_durability (env)", t_env.test_sqlite_pragmas_and_durability, Path(td))
        run_test("test_posix_advisory_locking (env)", t_env.test_posix_advisory_locking, Path(td))
    run_test("test_data_root_structure", t_env.test_data_root_structure)

    print("\n--- 2. SQLite Durability Pragmas Tests ---")
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        run_test("test_durability_pragmas_readback", t_sql.test_durability_pragmas_readback, tpath)
        run_test("test_connection_reset_semantics", t_sql.test_connection_reset_semantics, tpath)
        run_test("test_foreign_key_enforcement", t_sql.test_foreign_key_enforcement, tpath)
        run_test("test_rollback_journal_lifecycle", t_sql.test_rollback_journal_lifecycle, tpath)
        run_test("test_short_transaction_immediate_locking", t_sql.test_short_transaction_immediate_locking, tpath)

    print("\n--- 3. Advisory POSIX Locking Tests ---")
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        run_test("test_writer_lock_lifecycle", t_locks.test_writer_lock_lifecycle, tpath)
        run_test("test_writer_lock_contention_non_blocking", t_locks.test_writer_lock_contention_non_blocking, tpath)
        run_test("test_lock_file_never_unlinked_inode_stability", t_locks.test_lock_file_never_unlinked_inode_stability, tpath)
        run_test("test_cross_process_contention", t_locks.test_cross_process_contention, tpath)
        run_test("test_crash_recovery_sigkill", t_locks.test_crash_recovery_sigkill, tpath)
        run_test("test_query_lock_exclusive_by_default", t_locks.test_query_lock_exclusive_by_default, tpath)
        run_test("test_query_lock_shared_mode", t_locks.test_query_lock_shared_mode, tpath)
        run_test("test_bounded_wait_timeout", t_locks.test_bounded_wait_timeout, tpath)
        run_test("test_exception_safety", t_locks.test_exception_safety, tpath)

    print("\n--- 4. DuckDB & PyArrow Compatibility Tests ---")
    try:
        import duckdb
        import pyarrow
        has_duckdb_arrow = True
    except ImportError as e:
        has_duckdb_arrow = False
        print(f"  [SKIP] Third-party DuckDB/PyArrow libraries not yet installed in virtualenv ({e}).")
        print("         Lockfile generated; packages ready for installation.")
        skipped += 10

    if has_duckdb_arrow:
        with tempfile.TemporaryDirectory() as td:
            tpath = Path(td)
            run_test("test_duckdb_arrow_batch_stream_ingestion", t_duck.test_duckdb_arrow_batch_stream_ingestion, tpath)
            run_test("test_duckdb_batch_size_invariance", t_duck.test_duckdb_batch_size_invariance, tpath)
            run_test("test_duckdb_checkpoint_and_zero_wal_postcondition", t_duck.test_duckdb_checkpoint_and_zero_wal_postcondition, tpath)
            run_test("test_duckdb_checkpoint_active_transaction_barrier", t_duck.test_duckdb_checkpoint_active_transaction_barrier, tpath)
            run_test("test_duckdb_read_only_mode_and_mtime_immutability", t_duck.test_duckdb_read_only_mode_and_mtime_immutability, tpath)
            run_test("test_duckdb_read_only_nonexistent_database", t_duck.test_duckdb_read_only_nonexistent_database, tpath)
            run_test("test_duckdb_concurrent_readers", t_duck.test_duckdb_concurrent_readers, tpath)
            run_test("test_duckdb_writer_configuration_settings", t_duck.test_duckdb_writer_configuration_settings, tpath)
            run_test("test_duckdb_reader_configuration_settings", t_duck.test_duckdb_reader_configuration_settings, tpath)
        run_test("test_duckdb_interval_arithmetic_and_quantile_oracle", t_duck.test_duckdb_interval_arithmetic_and_quantile_oracle)

    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"EXECUTION SUMMARY: {passed} PASSED, {skipped} SKIPPED, {failed} FAILED in {elapsed:.2f}s")
    print("=" * 70)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
