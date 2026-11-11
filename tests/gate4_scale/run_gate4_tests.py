"""Gate G4 Scale & Incremental Correctness Test Suite Runner.

Executes all Gate G4 acceptance scenarios:
- Scenario T13 & W02: Incremental build vs full rebuild logical equivalence & anti-inflation
- Scenario T15: Concurrent writer lock contention and bounded exit
- Scenario T22 & W08: 80 GiB storage budget preflight & Option A retention policy enforcement
"""

import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

import tests.gate4_scale.test_scenario_t13_incremental_vs_full_rebuild as t13
import tests.gate4_scale.test_scenario_t15_concurrent_writers as t15
import tests.gate4_scale.test_scenario_t22_resource_exhaustion_and_cleanup_preview as t22


def main() -> int:
    print("=" * 76)
    print("GATE G4: SCALE, INCREMENTAL CORRECTNESS & OPTION A RETENTION SUITE")
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
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n--- 1. Scenario T13 & W02: Incremental Rebuild Equivalence & Anti-Inflation ---")
    run_test("test_t13_incremental_vs_full_rebuild_equivalence", t13.test_t13_incremental_vs_full_rebuild_equivalence)
    run_test("test_t13_source_revision_replacement_removes_old_rows_without_inflation", t13.test_t13_source_revision_replacement_removes_old_rows_without_inflation)

    print("\n--- 2. Scenario T15: Concurrent Writers Under Advisory Lock ---")
    run_test("test_t15_concurrent_writers_exit_boundedly", t15.test_t15_concurrent_writers_exit_boundedly)

    print("\n--- 3. Scenario T22 & W08: Budget Preflight & Option A Retention Policy ---")
    run_test("test_t22_storage_footprint_calculation_and_budget_preflight", t22.test_t22_storage_footprint_calculation_and_budget_preflight)
    run_test("test_t22_option_a_retention_policy_protects_current_and_one_prior", t22.test_t22_option_a_retention_policy_protects_current_and_one_prior)

    elapsed = time.time() - start_time
    print("\n" + "=" * 76)
    print(f"GATE G4 EXECUTION SUMMARY: {passed} PASSED, 0 SKIPPED, {failed} FAILED in {elapsed:.2f}s")
    print("=" * 76)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
