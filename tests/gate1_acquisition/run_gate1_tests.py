"""Comprehensive Gate G1 Acceptance Test Suite Runner.

Executes all Gate G1 acceptance and validation test suites:
- test_contracts_and_identity.py (RFC 8785, derivation IDs, schemas.json, rules.json, T05, T10, T14)
- test_control_plane.py (15 SQLite relations, pragmas, migrations, T18, T24)
- test_acquisition_and_provenance.py (bounded streaming, provenance, T01, T02, T03, T04, T25, T29)
"""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import time
import traceback

# Ensure src/ and repo root are on python path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

import tests.gate1_acquisition.test_acquisition_and_provenance as t_acq
import tests.gate1_acquisition.test_contracts_and_identity as t_cid
import tests.gate1_acquisition.test_control_plane as t_ctrl


def main() -> int:
    print("=" * 76)
    print("GATE G1: TRUSTWORTHY ACQUISITION & SQLITE CONTROL PLANE ACCEPTANCE SUITE")
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

    print("\n--- 1. Contracts & Canonical Identity Hashing (T05, T10, T14) ---")
    run_test("test_canonical_json_determinism_and_key_sorting", t_cid.test_canonical_json_determinism_and_key_sorting)
    run_test("test_canonical_json_rejects_non_finite_floats", t_cid.test_canonical_json_rejects_non_finite_floats)
    run_test("test_canonical_json_rejects_non_string_dict_keys", t_cid.test_canonical_json_rejects_non_string_dict_keys)
    run_test("test_canonical_json_explicit_null_semantics", t_cid.test_canonical_json_explicit_null_semantics)
    run_test("test_canonical_json_unicode_preservation", t_cid.test_canonical_json_unicode_preservation)
    run_test("test_compute_revision_id_determinism", t_cid.test_compute_revision_id_determinism)
    run_test("test_compute_revision_id_sensitivity", t_cid.test_compute_revision_id_sensitivity)
    run_test("test_compute_revision_id_validation", t_cid.test_compute_revision_id_validation)
    run_test("test_compute_derivation_id_permutation_invariance", t_cid.test_compute_derivation_id_permutation_invariance)
    run_test("test_compute_derivation_id_sensitivity_scenarios", t_cid.test_compute_derivation_id_sensitivity_scenarios)
    run_test("test_compute_code_bundle_manifest_hash", t_cid.test_compute_code_bundle_manifest_hash)
    run_test("test_scenario_t05_contracts_schemas_json_structure", t_cid.test_scenario_t05_contracts_schemas_json_structure)
    run_test("test_scenario_t05_schema_validator_logic", t_cid.test_scenario_t05_schema_validator_logic)
    run_test("test_scenario_t10_taxi_zone_lookup_invariants", t_cid.test_scenario_t10_taxi_zone_lookup_invariants)
    run_test("test_scenario_t10_unmatched_facts_retained", t_cid.test_scenario_t10_unmatched_facts_retained)
    run_test("test_scenario_t14_rules_json_inventory", t_cid.test_scenario_t14_rules_json_inventory)
    run_test("test_scenario_t14_s07_blocks_on_incomplete_inventory_or_error", t_cid.test_scenario_t14_s07_blocks_on_incomplete_inventory_or_error)

    print("\n--- 2. SQLite Control Plane & State Machine (T18, T24) ---")
    control_tests = [
        ("test_pragmas_and_initialization", t_ctrl.test_pragmas_and_initialization),
        ("test_scenario_t24_migration_and_restore", t_ctrl.test_scenario_t24_migration_and_restore),
        ("test_scenario_t18_kill_after_commit_idempotent_retry", t_ctrl.test_scenario_t18_kill_after_commit_idempotent_retry),
        ("test_cas_concurrency_and_stale_generation_conflict", t_ctrl.test_cas_concurrency_and_stale_generation_conflict),
        ("test_selection_request_conflict_detection", t_ctrl.test_selection_request_conflict_detection),
        ("test_release_rejection_and_retirement", t_ctrl.test_release_rejection_and_retirement),
        ("test_foreign_key_and_check_constraints", t_ctrl.test_foreign_key_and_check_constraints),
        ("test_attempt_phase_progression_and_events", t_ctrl.test_attempt_phase_progression_and_events),
    ]
    for name, fn in control_tests:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            env = {
                "root": root,
                "data_root": root / ".data",
                "control_dir": root / ".data" / "control",
                "releases_dir": root / ".data" / "releases",
                "db_path": root / ".data" / "control" / "registry.sqlite",
            }
            run_test(name, fn, env)

    print("\n--- 3. Frontier Discovery, Bounded Acquisition & Provenance (T01, T02, T03, T04, T25, T29) ---")
    run_test("test_t01_truncated_download_midstream_abort", t_acq.test_t01_truncated_download_midstream_abort)
    run_test("test_t02_identical_bytes_acquired_twice", t_acq.test_t02_identical_bytes_acquired_twice)
    run_test("test_t03_content_changes_without_metadata_validators", t_acq.test_t03_content_changes_without_metadata_validators)
    run_test("test_t04_http_error_and_tls_classification", t_acq.test_t04_http_error_and_tls_classification)
    run_test("test_t25_frontier_scan_with_gaps_preserves_current", t_acq.test_t25_frontier_scan_with_gaps_preserves_current)
    run_test("test_t29_crash_recovery_and_orphan_reconciliation", t_acq.test_t29_crash_recovery_and_orphan_reconciliation)

    elapsed = time.time() - start_time
    print("\n" + "=" * 76)
    print(f"GATE G1 EXECUTION SUMMARY: {passed} PASSED, 0 SKIPPED, {failed} FAILED in {elapsed:.2f}s")
    print("=" * 76)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
