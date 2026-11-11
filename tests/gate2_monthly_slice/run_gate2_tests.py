"""Comprehensive Gate G2 Acceptance Test Suite Runner.

Executes all Gate G2 monthly vertical slice and quality engine test suites:
- test_scenario_t06_locators_and_anti_inflation.py (T06: physical locators, fact preservation, anti-inflation, Q08)
- test_scenario_t07_batch_invariance.py (T07: batch size & row group invariance)
- test_scenario_t08_interval_arithmetic.py (T08: 64-bit microsecond arithmetic, 3-class partition, count identity)
- test_scenario_t09_off_month_and_coverage.py (T09: off-month dates, naive timestamps, absent month nulls)
- test_scenario_t10_zone_integrity_and_unmatched.py (T10: S03 uniqueness, sentinels, Q05 4-class partition)
- test_scenario_t11_non_finite_and_domains.py (T11: Q06 non-finite profile, Q07 domain discovery)
- test_scenario_t12_continuous_quantiles_and_anti_averaging.py (T12: Type 7 quantiles, anti-averaging invariant)
"""

from __future__ import annotations

from pathlib import Path
import sys
import time
import traceback

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

import tests.gate2_monthly_slice.test_scenario_t06_locators_and_anti_inflation as t06
import tests.gate2_monthly_slice.test_scenario_t07_batch_invariance as t07
import tests.gate2_monthly_slice.test_scenario_t08_interval_arithmetic as t08
import tests.gate2_monthly_slice.test_scenario_t09_off_month_and_coverage as t09
import tests.gate2_monthly_slice.test_scenario_t10_zone_integrity_and_unmatched as t10
import tests.gate2_monthly_slice.test_scenario_t11_non_finite_and_domains as t11
import tests.gate2_monthly_slice.test_scenario_t12_continuous_quantiles_and_anti_averaging as t12
import tests.gate2_monthly_slice.test_ingestion_pipeline as t_ingest


def main() -> int:
    print("=" * 76)
    print("GATE G2: MONTHLY VERTICAL SLICE & QUALITY ENGINE ACCEPTANCE SUITE")
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

    print("\n--- 1. Scenario T06: Physical Locators & Anti-Inflation ---")
    run_test("test_t06_physical_locators_contiguity_and_fact_preservation", t06.test_t06_physical_locators_contiguity_and_fact_preservation)
    run_test("test_t06_idempotent_reingestion_anti_inflation", t06.test_t06_idempotent_reingestion_anti_inflation)

    print("\n--- 2. Scenario T07: Batch Size & Row Group Invariance ---")
    run_test("test_t07_batch_size_invariance", t07.test_t07_batch_size_invariance)

    print("\n--- 3. Scenario T08: Microsecond Interval Arithmetic & 3-Class Partition ---")
    run_test("test_t08_naive_microsecond_arithmetic_precision", t08.test_t08_naive_microsecond_arithmetic_precision)
    run_test("test_t08_interval_3class_partition_and_count_identity", t08.test_t08_interval_3class_partition_and_count_identity)

    print("\n--- 4. Scenario T09: Off-Month Dates & Absent Month Coverage ---")
    run_test("test_t09_off_source_month_classification", t09.test_t09_off_source_month_classification)
    run_test("test_t09_coverage_absent_month_null_semantics", t09.test_t09_coverage_absent_month_null_semantics)

    print("\n--- 5. Scenario T10: Zone Integrity, Unmatched Facts & Sentinels ---")
    run_test("test_t10_zone_reference_s03_invariants", t10.test_t10_zone_reference_s03_invariants)
    run_test("test_t10_s03_blocks_on_missing_sentinel", t10.test_t10_s03_blocks_on_missing_sentinel)
    run_test("test_t10_unmatched_facts_retained_and_classified", t10.test_t10_unmatched_facts_retained_and_classified)

    print("\n--- 6. Scenario T11: Non-Finite Floats & Categorical Domains ---")
    run_test("test_t11_numeric_nonfinite_and_sign_profile", t11.test_t11_numeric_nonfinite_and_sign_profile)
    run_test("test_t11_categorical_domain_discovery_earliest_month", t11.test_t11_categorical_domain_discovery_earliest_month)

    print("\n--- 7. Scenario T12: Type 7 Continuous Quantiles & Anti-Averaging ---")
    run_test("test_t12_type7_continuous_quantile_oracles", t12.test_t12_type7_continuous_quantile_oracles)
    run_test("test_t12_anti_averaging_mathematical_invariant", t12.test_t12_anti_averaging_mathematical_invariant)

    print("\n--- 8. Ingestion Pipeline: DuckDB Writer, Schemas & Streaming PyArrow ---")
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        run_test("test_open_and_finalize_duckdb_writer", t_ingest.test_open_and_finalize_duckdb_writer, tpath)
        run_test("test_validate_source_schema_pre2025_and_2025onward", t_ingest.test_validate_source_schema_pre2025_and_2025onward, tpath)
        run_test("test_stream_parquet_to_duckdb_and_locators", t_ingest.test_stream_parquet_to_duckdb_and_locators, tpath)
        run_test("test_ingest_taxi_zones_duckdb", t_ingest.test_ingest_taxi_zones_duckdb, tpath)

    elapsed = time.time() - start_time
    print("\n" + "=" * 76)
    print(f"GATE G2 EXECUTION SUMMARY: {passed} PASSED, 0 SKIPPED, {failed} FAILED in {elapsed:.2f}s")
    print("=" * 76)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
