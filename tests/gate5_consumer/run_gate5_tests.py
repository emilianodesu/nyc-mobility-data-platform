"""Gate G5 End-to-End Consumer & Operations Test Suite Runner.

Executes all Gate G5 acceptance scenarios:
- Scenario T21 & W04: Read-only Dashboard Queries, out-of-cache checks & concurrency
- Scenario T23 & R18: Orchestration asset checks mirroring S01-S08 and Q01-Q09
- Scenario T25 & R25: Frontier scan with gap detection & recommended catch-up
- Scenario T27: Retired release access rejection & audit manifest preservation
- Scenario T28: Streamlit / Consumer bounded LRU cache & memory bounds
"""

import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

import tests.gate5_consumer.test_scenario_t21_readonly_dashboard_queries as t21
import tests.gate5_consumer.test_scenario_t23_orchestration_assets_and_checks as t23
import tests.gate5_consumer.test_scenario_t25_frontier_scan_schedule as t25
import tests.gate5_consumer.test_scenario_t27_retired_release_access as t27
import tests.gate5_consumer.test_scenario_t28_bounded_memory_cache as t28


def main() -> int:
    print("=" * 76)
    print("GATE G5: END-TO-END CONSUMER & OPERATIONS ACCEPTANCE SUITE")
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

    print("\n--- 1. Scenario T21 & W04: Read-Only Queries, Out-of-Cache Checks & Concurrency ---")
    run_test("test_t21_readonly_dashboard_queries_and_concurrency", t21.test_t21_readonly_dashboard_queries_and_concurrency)
    run_test("test_t21_unregistered_release_raises_ineligible", t21.test_t21_unregistered_release_raises_ineligible)
    run_test("test_t21_missing_database_file_raises_artifact_not_found", t21.test_t21_missing_database_file_raises_artifact_not_found)

    print("\n--- 2. Scenario T23 & R18: Orchestration Assets & Checks Mirroring Rules ---")
    run_test("test_t23_asset_checks_mirror_pre_seal_rules_success", t23.test_t23_asset_checks_mirror_pre_seal_rules_success)
    run_test("test_t23_failing_rule_surfaces_as_asset_check_failure", t23.test_t23_failing_rule_surfaces_as_asset_check_failure)
    run_test("test_t23_missing_validation_receipt_raises_orchestration_error", t23.test_t23_missing_validation_receipt_raises_orchestration_error)

    print("\n--- 3. Scenario T25 & R25: Frontier Scan With Gap Detection ---")
    run_test("test_t25_frontier_scan_detects_gap_and_recommends_sequence", t25.test_t25_frontier_scan_detects_gap_and_recommends_sequence)
    run_test("test_t25_frontier_scan_synchronized_contiguous", t25.test_t25_frontier_scan_synchronized_contiguous)

    print("\n--- 4. Scenario T27: Retired Release Access Attempt ---")
    run_test("test_t27_retired_release_access_raises_error_and_preserves_manifests", t27.test_t27_retired_release_access_raises_error_and_preserves_manifests)

    print("\n--- 5. Scenario T28: Bounded LRU Cache & Memory Bounds ---")
    run_test("test_t28_lru_cache_bounded_at_32_entries", t28.test_t28_lru_cache_bounded_at_32_entries)
    run_test("test_t28_result_set_bounded_by_max_rows", t28.test_t28_result_set_bounded_by_max_rows)
    run_test("test_t28_memory_rss_remains_bounded", t28.test_t28_memory_rss_remains_bounded)

    elapsed = time.time() - start_time
    print("\n" + "=" * 76)
    print(f"GATE G5 EXECUTION SUMMARY: {passed} PASSED, 0 SKIPPED, {failed} FAILED in {elapsed:.2f}s")
    print("=" * 76)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
