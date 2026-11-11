"""Comprehensive Gate G3 Acceptance Test Suite Runner.

Executes all Gate G3 Publication Safety & Durability Recovery test suites:
- test_publication_safety.py (T14, T15, T16, T17, T20, T26, W03, W05)
- test_durability_and_recovery.py (T18, T19, T24, W01, Zero-WAL, Rename Protection)
"""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import time
import traceback
from typing import Any

# Ensure src/ and repo root are on python path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

import tests.gate3_publication_safety.test_publication_safety as t_pub
import tests.gate3_publication_safety.test_durability_and_recovery as t_dur


def main() -> int:
    print("=" * 76)
    print("GATE G3: PUBLICATION SAFETY & DURABILITY RECOVERY ACCEPTANCE SUITE")
    print("=" * 76)
    start_time = time.time()
    passed = 0
    failed = 0

    def run_test(name: str, fn, *args):
        nonlocal passed, failed
        try:
            fn(*args)
            print(f"  [PASS] {name}")
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            traceback.print_exc()
            failed += 1

    def make_temp_env(root: Path) -> dict[str, Any]:
        data_root = root / ".data"
        control_dir = data_root / "control"
        releases_dir = data_root / "releases"
        staging_dir = data_root / "staging"
        raw_dir = data_root / "raw"
        control_dir.mkdir(parents=True, exist_ok=True)
        releases_dir.mkdir(parents=True, exist_ok=True)
        staging_dir.mkdir(parents=True, exist_ok=True)
        raw_dir.mkdir(parents=True, exist_ok=True)
        return {
            "root": root,
            "data_root": data_root,
            "control_dir": control_dir,
            "releases_dir": releases_dir,
            "staging_dir": staging_dir,
            "raw_dir": raw_dir,
            "db_path": control_dir / "registry.sqlite",
        }

    print("\n--- 1. Candidate Finalization & Publication Safety (T14, T15, T16, T17, T20, T26, W03, W05) ---")
    pub_tests = [
        ("test_scenario_t14_incomplete_inventory_blocks_finalization", t_pub.test_scenario_t14_incomplete_inventory_blocks_finalization),
        ("test_scenario_t15_writer_lock_contention_second_writer_exits_boundedly", t_pub.test_scenario_t15_writer_lock_contention_second_writer_exits_boundedly),
        ("test_scenario_t16_kill_during_staging_leaves_no_selected_partial_database", t_pub.test_scenario_t16_kill_during_staging_leaves_no_selected_partial_database),
        ("test_scenario_t17_kill_after_rename_reports_orphan_without_auto_adoption", t_pub.test_scenario_t17_kill_after_rename_reports_orphan_without_auto_adoption),
        ("test_scenario_t20_corrupted_release_manifest_or_path_escape_integrity_failure", t_pub.test_scenario_t20_corrupted_release_manifest_or_path_escape_integrity_failure),
        ("test_scenario_t26_semantic_exceptions_publishable_computation_errors_block", t_pub.test_scenario_t26_semantic_exceptions_publishable_computation_errors_block),
        ("test_walkthrough_w03_warning_vs_computation_failure", t_pub.test_walkthrough_w03_warning_vs_computation_failure),
        ("test_walkthrough_w05_finalized_original_lost_registry_write", t_pub.test_walkthrough_w05_finalized_original_lost_registry_write),
    ]
    for name, fn in pub_tests:
        with tempfile.TemporaryDirectory() as td:
            env = make_temp_env(Path(td))
            run_test(name, fn, env)

    print("\n--- 2. Durability, Idempotency & Recovery (T18, T19, T24, W01, Zero-WAL, Rename) ---")
    dur_tests = [
        ("test_scenario_t18_kill_after_sqlite_commit_reconciliation_finds_event_no_duplicate", t_dur.test_scenario_t18_kill_after_sqlite_commit_reconciliation_finds_event_no_duplicate),
        ("test_scenario_t19_stale_cas_rejected_and_retained_rollback_verifies_bytes", t_dur.test_scenario_t19_stale_cas_rejected_and_retained_rollback_verifies_bytes),
        ("test_scenario_t24_registry_migration_backup_and_restore_artifacts_untouched", t_dur.test_scenario_t24_registry_migration_backup_and_restore_artifacts_untouched),
        ("test_walkthrough_w01_retry_after_later_rollback", t_dur.test_walkthrough_w01_retry_after_later_rollback),
        ("test_zero_wal_verification_and_checkpoint_postconditions", t_dur.test_zero_wal_verification_and_checkpoint_postconditions),
        ("test_atomic_directory_rename_destination_already_exists_protection", t_dur.test_atomic_directory_rename_destination_already_exists_protection),
    ]
    for name, fn in dur_tests:
        with tempfile.TemporaryDirectory() as td:
            env = make_temp_env(Path(td))
            run_test(name, fn, env)

    elapsed = time.time() - start_time
    print("\n" + "=" * 76)
    print(f"GATE G3 EXECUTION SUMMARY: {passed} PASSED, 0 SKIPPED, {failed} FAILED in {elapsed:.2f}s")
    print("=" * 76)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
