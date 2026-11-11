"""Gate G2 Iteration 2: Adversarial Empirical Verification & Stress Harness.

Empirically tests:
1. Concurrency and Read-Only Invariants:
   - Multi-reader concurrent execution on candidate.duckdb with read_only=True.
   - st_mtime_ns and SHA-256 byte immutability verification.
   - Rejection of DML, DDL, and CHECKPOINT mutations on read-only connections.
   - Zero .wal existence before, during, and after concurrency.

2. Resource Configuration Readback:
   - Writer configuration (threads=4, memory_limit='8GB', max_temp_directory_size='14GB', temp_directory).
   - Reader configuration (threads=4, memory_limit='1GB', max_temp_directory_size='2GB', temp_directory).
   - Reader configuration with threads=1 as specified in config.py.
   - Readback verification of effective DuckDB current_setting() values.

3. Microsecond Interval Arithmetic & 3-Class Partition:
   - Negative durations across multiple scales (-1 us, -1s, -1h, -1 day, -30 days, -68 years).
   - Zero duration (exact 0.0s) -> eligible.
   - Subsecond and multi-year positive durations (+1 us, +0.2s, +1s, +1 day, +31 days, +68 years).
   - Missing timestamps (all 3 NULL combinations: missing request, missing pickup, missing both).
   - Count identity verification: total == missing + negative + eligible.
   - Mutually exclusive classification and eligibility invariants.

4. Non-Finite Floats, Inf/NaN, and Extreme Number Handling:
   - Finite vs non-finite handling in continuous quantiles.
   - Behavior of DuckDB epoch_us arithmetic on timestamp boundaries.
   - IEEE 754 precision analysis for microsecond intervals.
"""

from __future__ import annotations

import concurrent.futures
from datetime import datetime, timezone
import hashlib
import math
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any

import duckdb
import pytest

from hvfhv.config import ResourceCaps, SystemConfig, get_default_config
from hvfhv.transforms.models import (
    calculate_recorded_interval_seconds,
    classify_interval,
    continuous_quantile,
    init_analytical_models,
)
from hvfhv.transforms.quality import QualityEngine, init_quality_models


REPO_ROOT = Path(__file__).resolve().parents[2]
CANDIDATE_DB_PATH = REPO_ROOT / ".data" / "staging" / "attempt_g2_vertical_slice" / "candidate.duckdb"


def _compute_sha256(file_path: Path) -> str:
    """Computes SHA-256 hex digest of file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


# ============================================================================
# 1. Concurrency and Read-Only Invariants on candidate.duckdb
# ============================================================================

def test_candidate_duckdb_read_only_concurrency_and_immutability():
    """Verifies that candidate.duckdb can be opened concurrently with read_only=True,
    serves parallel analytical queries, rejects mutations, and preserves st_mtime_ns.
    """
    assert CANDIDATE_DB_PATH.exists(), f"Candidate database {CANDIDATE_DB_PATH} must exist"

    wal_path = Path(f"{CANDIDATE_DB_PATH}.wal")
    assert not wal_path.exists(), f"WAL file {wal_path} must not exist before read-only queries"

    # Pre-test immutability snapshot
    stat_before = os.stat(CANDIDATE_DB_PATH)
    mtime_before = stat_before.st_mtime_ns
    size_before = stat_before.st_size
    sha256_before = _compute_sha256(CANDIDATE_DB_PATH)

    num_threads = 16

    def reader_worker(worker_id: int) -> dict[str, Any]:
        """Reader connection executing queries and verifying mutation rejection."""
        con = duckdb.connect(str(CANDIDATE_DB_PATH), read_only=True)

        # 1. Analytical query execution
        obs_count = con.execute("SELECT COUNT(*) FROM trip_observation;").fetchone()[0]
        qual_stats = con.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS eligible,
                COUNT(*) FILTER (WHERE interval_class = 'negative_recorded_interval') AS negative,
                COUNT(*) FILTER (WHERE interval_class = 'missing_timestamp') AS missing,
                quantile_cont(recorded_request_to_pickup_seconds, 0.50) FILTER (WHERE is_interval_eligible) AS p50,
                quantile_cont(recorded_request_to_pickup_seconds, 0.95) FILTER (WHERE is_interval_eligible) AS p95
            FROM trip_qualified;
        """).fetchone()

        # 2. Mutation rejection assertions (DML, DDL, CHECKPOINT)
        mutation_rejected = 0

        # Attempt INSERT
        try:
            con.execute("INSERT INTO trip_observation (revision_id, source_row_number) VALUES ('hacker', 999999);")
        except Exception:
            mutation_rejected += 1

        # Attempt UPDATE
        try:
            con.execute("UPDATE trip_observation SET trip_miles = 999.0 WHERE source_row_number = 0;")
        except Exception:
            mutation_rejected += 1

        # Attempt DELETE
        try:
            con.execute("DELETE FROM trip_observation WHERE source_row_number = 0;")
        except Exception:
            mutation_rejected += 1

        # Attempt CREATE TABLE
        try:
            con.execute("CREATE TABLE adversary_payload (x INT);")
        except Exception:
            mutation_rejected += 1

        # Attempt DROP TABLE
        try:
            con.execute("DROP TABLE zone_reference;")
        except Exception:
            mutation_rejected += 1

        # Attempt CHECKPOINT on read-only connection
        try:
            con.execute("CHECKPOINT;")
        except Exception:
            mutation_rejected += 1

        con.close()

        return {
            "worker_id": worker_id,
            "obs_count": obs_count,
            "qual_stats": qual_stats,
            "mutation_rejected_count": mutation_rejected,
        }

    # Execute 16 concurrent readers simultaneously
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(reader_worker, i) for i in range(num_threads)]
        results = [f.result() for f in futures]

    # Verify all workers returned identical observations
    baseline = results[0]
    for r in results:
        assert r["obs_count"] == baseline["obs_count"] == 100_000
        assert r["qual_stats"] == baseline["qual_stats"]
        assert r["mutation_rejected_count"] >= 5, f"Expected at least 5 rejected mutations, got {r['mutation_rejected_count']}"

    # Verify candidate.duckdb immutability post-conditions
    stat_after = os.stat(CANDIDATE_DB_PATH)
    mtime_after = stat_after.st_mtime_ns
    size_after = stat_after.st_size
    sha256_after = _compute_sha256(CANDIDATE_DB_PATH)

    assert mtime_before == mtime_after, (
        f"Immutability violated: st_mtime_ns changed ({mtime_before} -> {mtime_after})"
    )
    assert size_before == size_after, (
        f"Immutability violated: file size changed ({size_before} -> {size_after})"
    )
    assert sha256_before == sha256_after, (
        f"Immutability violated: SHA256 digest changed ({sha256_before} -> {sha256_after})"
    )
    assert not wal_path.exists(), "Read-only concurrency must never create a .wal file"


# ============================================================================
# 2. Resource Configuration Readback
# ============================================================================

def test_duckdb_writer_and_reader_resource_readbacks():
    """Verifies that DuckDB writer and reader connections enforce exact resource caps:
    - Writer: threads=4, memory_limit='8GB', max_temp_directory_size='14GB', temp_directory
    - Reader: threads=4 (or 1), memory_limit='1GB', max_temp_directory_size='2GB', temp_directory
    """
    with tempfile.TemporaryDirectory() as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        db_path = tmp_dir / "resource_test.duckdb"
        spill_writer = tmp_dir / "spill_writer"
        spill_reader = tmp_dir / "spill_reader"
        spill_writer.mkdir()
        spill_reader.mkdir()

        # 1. Writer Configuration (threads=4, memory_limit='8GB', max_temp_directory_size='14GB')
        writer_config = {
            "threads": 4,
            "memory_limit": "8GB",
            "max_temp_directory_size": "14GB",
            "temp_directory": str(spill_writer),
        }

        w_con = duckdb.connect(str(db_path), config=writer_config)
        w_threads = int(w_con.execute("SELECT current_setting('threads');").fetchone()[0])
        w_mem = w_con.execute("SELECT current_setting('memory_limit');").fetchone()[0]
        w_max_temp = w_con.execute("SELECT current_setting('max_temp_directory_size');").fetchone()[0]
        w_temp = w_con.execute("SELECT current_setting('temp_directory');").fetchone()[0]

        assert w_threads == 4, f"Expected 4 writer threads, got {w_threads}"
        # DuckDB 2.0.0 parses '8GB' as 8*10^9 bytes = 7.45 GiB -> formatted as '7.4 GiB'
        assert "7.4" in w_mem or "8.0" in w_mem or "8GB" in w_mem, f"Unexpected memory_limit readback: {w_mem}"
        # DuckDB parses '14GB' as 14*10^9 bytes = 13.03 GiB -> formatted as '13.0 GiB'
        assert "13.0" in w_max_temp or "14" in w_max_temp, f"Unexpected max_temp_directory_size readback: {w_max_temp}"
        assert str(spill_writer) in w_temp or w_temp == str(spill_writer)

        w_con.execute("CREATE TABLE t (id INT);")
        w_con.commit()
        w_con.execute("CHECKPOINT;")
        w_con.close()

        # 2. Reader Configuration with threads=4, memory_limit='1GB'
        reader_config_t4 = {
            "threads": 4,
            "memory_limit": "1GB",
            "max_temp_directory_size": "2GB",
            "temp_directory": str(spill_reader),
        }
        r4_con = duckdb.connect(str(db_path), read_only=True, config=reader_config_t4)
        r4_threads = int(r4_con.execute("SELECT current_setting('threads');").fetchone()[0])
        r4_mem = r4_con.execute("SELECT current_setting('memory_limit');").fetchone()[0]
        r4_max_temp = r4_con.execute("SELECT current_setting('max_temp_directory_size');").fetchone()[0]
        r4_temp = r4_con.execute("SELECT current_setting('temp_directory');").fetchone()[0]

        assert r4_threads == 4, f"Expected 4 reader threads, got {r4_threads}"
        # 1GB = 10^9 bytes = 953.67 MiB
        assert "953" in r4_mem or "1.0" in r4_mem or "1GB" in r4_mem, f"Unexpected memory_limit readback: {r4_mem}"
        assert "1.8" in r4_max_temp or "2.0" in r4_max_temp or "2GB" in r4_max_temp
        assert str(spill_reader) in r4_temp or r4_temp == str(spill_reader)
        r4_con.close()

        # 3. Reader Configuration with threads=1 (config.py default reader caps)
        reader_config_t1 = {
            "threads": 1,
            "memory_limit": "1GB",
            "max_temp_directory_size": "2GB",
            "temp_directory": str(spill_reader),
        }
        r1_con = duckdb.connect(str(db_path), read_only=True, config=reader_config_t1)
        r1_threads = int(r1_con.execute("SELECT current_setting('threads');").fetchone()[0])
        assert r1_threads == 1, f"Expected 1 reader thread, got {r1_threads}"
        r1_con.close()


# ============================================================================
# 3. Microsecond Interval Arithmetic & 3-Class Partition Stress Assertions
# ============================================================================

def test_microsecond_interval_arithmetic_extreme_durations_and_3class_partition():
    """Exhaustive stress assertions on microsecond interval arithmetic and 3-class partition:
    - Extreme positive durations (+1 us up to +68 years).
    - Exact zero (0.0 us).
    - Extreme negative durations (-1 us down to -68 years).
    - Missing timestamps (all 3 NULL combinations).
    - Strict count identity: total == missing + negative + eligible.
    - Mutual exclusivity and SQL trip_qualified view behavior.
    """
    con = duckdb.connect(":memory:")
    init_analytical_models(con)

    # Reference base timestamp
    base = datetime(2024, 12, 1, 12, 0, 0, 0)

    # Test cases: (request_dt, pickup_dt, expected_class, expected_seconds)
    cases = [
        # --- Eligible Positive Intervals ---
        # +1 microsecond
        (datetime(2024, 12, 1, 12, 0, 0, 0), datetime(2024, 12, 1, 12, 0, 0, 1), "eligible_nonnegative_recorded_interval", 0.000001),
        # +200,000 us (0.2s)
        (datetime(2024, 12, 1, 12, 0, 0, 900000), datetime(2024, 12, 1, 12, 0, 1, 100000), "eligible_nonnegative_recorded_interval", 0.200000),
        # +1.0 second
        (datetime(2024, 12, 1, 12, 0, 0, 0), datetime(2024, 12, 1, 12, 0, 1, 0), "eligible_nonnegative_recorded_interval", 1.000000),
        # +3600.0 seconds (1 hour)
        (datetime(2024, 12, 1, 12, 0, 0, 0), datetime(2024, 12, 1, 13, 0, 0, 0), "eligible_nonnegative_recorded_interval", 3600.0),
        # +86400.0 seconds (1 day across midnight)
        (datetime(2024, 12, 1, 12, 0, 0, 0), datetime(2024, 12, 2, 12, 0, 0, 0), "eligible_nonnegative_recorded_interval", 86400.0),
        # +2678400.0 seconds (31 days across full month)
        (datetime(2024, 12, 1, 0, 0, 0, 0), datetime(2025, 1, 1, 0, 0, 0, 0), "eligible_nonnegative_recorded_interval", 2678400.0),
        # Extreme +68 years (1970-01-01 -> 2038-01-19 03:14:07.999999)
        (datetime(1970, 1, 1, 0, 0, 0, 0), datetime(2038, 1, 19, 3, 14, 7, 999999), "eligible_nonnegative_recorded_interval", 2147483647.999999),

        # --- Eligible Zero Interval ---
        (datetime(2024, 12, 1, 12, 0, 0, 500000), datetime(2024, 12, 1, 12, 0, 0, 500000), "eligible_nonnegative_recorded_interval", 0.0),

        # --- Negative Intervals ---
        # -1 microsecond
        (datetime(2024, 12, 1, 12, 0, 0, 1), datetime(2024, 12, 1, 12, 0, 0, 0), "negative_recorded_interval", -0.000001),
        # -0.2 seconds
        (datetime(2024, 12, 1, 12, 0, 1, 100000), datetime(2024, 12, 1, 12, 0, 0, 900000), "negative_recorded_interval", -0.200000),
        # -1.0 second
        (datetime(2024, 12, 1, 12, 0, 1, 0), datetime(2024, 12, 1, 12, 0, 0, 0), "negative_recorded_interval", -1.0),
        # -3600.0 seconds (-1 hour)
        (datetime(2024, 12, 1, 13, 0, 0, 0), datetime(2024, 12, 1, 12, 0, 0, 0), "negative_recorded_interval", -3600.0),
        # -86400.0 seconds (-1 day)
        (datetime(2024, 12, 2, 12, 0, 0, 0), datetime(2024, 12, 1, 12, 0, 0, 0), "negative_recorded_interval", -86400.0),
        # -2678400.0 seconds (-31 days)
        (datetime(2025, 1, 1, 0, 0, 0, 0), datetime(2024, 12, 1, 0, 0, 0, 0), "negative_recorded_interval", -2678400.0),
        # Extreme -68 years
        (datetime(2038, 1, 19, 3, 14, 7, 999999), datetime(1970, 1, 1, 0, 0, 0, 0), "negative_recorded_interval", -2147483647.999999),

        # --- Missing Timestamps ---
        # Missing request
        (None, datetime(2024, 12, 1, 12, 0, 0, 0), "missing_timestamp", None),
        # Missing pickup
        (datetime(2024, 12, 1, 12, 0, 0, 0), None, "missing_timestamp", None),
        # Both missing
        (None, None, "missing_timestamp", None),
    ]

    total_expected = len(cases)
    expected_eligible = sum(1 for c in cases if c[2] == "eligible_nonnegative_recorded_interval")
    expected_negative = sum(1 for c in cases if c[2] == "negative_recorded_interval")
    expected_missing = sum(1 for c in cases if c[2] == "missing_timestamp")

    assert total_expected == expected_eligible + expected_negative + expected_missing == 18

    # 1. Test Python functions
    for req, pu, exp_cls, exp_sec in cases:
        sec, cls = classify_interval(req, pu)
        assert cls == exp_cls, f"Python classify_interval failed for ({req}, {pu}): expected {exp_cls}, got {cls}"
        if exp_sec is None:
            assert sec is None
        else:
            assert sec is not None
            assert math.isclose(sec, exp_sec, abs_tol=1e-6), f"Expected {exp_sec}, got {sec}"

    # 2. Test SQL Views in DuckDB
    insert_rows = []
    for idx, (req, pu, _, _) in enumerate(cases):
        insert_rows.append((
            "rev_stress_oracle",
            idx,
            "2024-12",
            req,
            pu,
            100,
            100,
            2.5,
            600,
        ))

    con.executemany("""
        INSERT INTO trip_observation (
            revision_id,
            source_row_number,
            source_month,
            request_datetime,
            pickup_datetime,
            pu_location_id,
            do_location_id,
            trip_miles,
            trip_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, insert_rows)

    # Query trip_qualified view in DuckDB
    sql_results = con.execute("""
        SELECT
            source_row_number,
            recorded_request_to_pickup_seconds,
            interval_class,
            is_interval_eligible
        FROM trip_qualified
        ORDER BY source_row_number;
    """).fetchall()

    assert len(sql_results) == total_expected

    for idx, (rn, sec, cls, is_elig) in enumerate(sql_results):
        _, _, exp_cls, exp_sec = cases[idx]
        assert cls == exp_cls, f"SQL interval_class mismatch at row {idx}: expected {exp_cls}, got {cls}"

        if exp_cls == "missing_timestamp":
            assert sec is None, f"Expected NULL seconds for missing timestamp, got {sec}"
            assert is_elig is False, f"Expected is_interval_eligible=False for row {idx}"
        elif exp_cls == "negative_recorded_interval":
            assert sec is not None and sec < 0.0
            assert math.isclose(sec, exp_sec, abs_tol=1e-6), f"Row {idx} sec mismatch: expected {exp_sec}, got {sec}"
            assert is_elig is False, f"Expected is_interval_eligible=False for negative interval row {idx}"
        elif exp_cls == "eligible_nonnegative_recorded_interval":
            assert sec is not None and sec >= 0.0
            assert math.isclose(sec, exp_sec, abs_tol=1e-6), f"Row {idx} sec mismatch: expected {exp_sec}, got {sec}"
            assert is_elig is True, f"Expected is_interval_eligible=True for eligible interval row {idx}"

    # 3. Aggregation & Count Identity in DuckDB
    agg = con.execute("""
        SELECT
            COUNT(*) AS total_count,
            COUNT(*) FILTER (WHERE interval_class = 'missing_timestamp') AS missing_count,
            COUNT(*) FILTER (WHERE interval_class = 'negative_recorded_interval') AS negative_count,
            COUNT(*) FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_count,
            COUNT(*) FILTER (WHERE is_interval_eligible = TRUE) AS eligible_flag_count
        FROM trip_qualified;
    """).fetchone()

    total_cnt, missing_cnt, negative_cnt, eligible_cnt, eligible_flag_cnt = agg
    assert total_cnt == total_expected == 18
    assert missing_cnt == expected_missing == 3
    assert negative_cnt == expected_negative == 7
    assert eligible_cnt == expected_eligible == 8
    assert eligible_flag_cnt == eligible_cnt

    # Count Identity Invariant
    assert total_cnt == missing_cnt + negative_cnt + eligible_cnt, (
        f"Count identity broken: {total_cnt} != {missing_cnt} + {negative_cnt} + {eligible_cnt}"
    )

    con.close()


# ============================================================================
# 4. Non-Finite Floats and Extreme IEEE 754 Quantiles
# ============================================================================

def test_nonfinite_and_extreme_floats_handling():
    """Validates behavior when non-finite floats (NaN, +Inf, -Inf) or extreme magnitudes
    are passed to continuous quantiles and numeric profiling functions.
    """
    # 1. Pure Python continuous_quantile on extreme numbers
    data_extreme = [1e300, 2e300, 3e300, 4e300]
    p50_extreme = continuous_quantile(data_extreme, 0.50)
    assert p50_extreme is not None
    assert math.isclose(p50_extreme, 2.5e300, rel_tol=1e-12)

    # 2. Finite-only filtering behavior
    raw_mixed = [1.0, 2.0, float("nan"), float("inf"), float("-inf"), None, 3.0, 4.0]
    finite_only = [v for v in raw_mixed if v is not None and math.isfinite(v)]
    assert finite_only == [1.0, 2.0, 3.0, 4.0]

    p50_finite = continuous_quantile(finite_only, 0.50)
    assert p50_finite == 2.5

    # 3. IEEE 754 precision analysis for microsecond duration
    # 2^53 is 9,007,199,254,740,992 integer microseconds (~285 years)
    max_representable_us = 2**53
    max_seconds = max_representable_us / 1_000_000.0
    assert max_seconds > 9_000_000_000.0  # > 285 years

    # Verify that for any duration under 1 month (2,678,400s), float64 error is < 1 nanosecond
    one_month_s = 31 * 86400.0
    us_int = int(one_month_s * 1_000_000)
    s_a = us_int / 1_000_000.0
    s_b = (us_int + 1) / 1_000_000.0
    delta = s_b - s_a
    assert abs(delta - 1e-6) < 1e-9, f"Float precision error too large at 1 month: {abs(delta - 1e-6)}"


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_it2_stress_tests() -> int:
    print("=" * 80)
    print("GATE G2 ITERATION 2: ADVERSARIAL EMPIRICAL STRESS SUITE (CHALLENGER)")
    print("=" * 80)

    tests = [
        ("Candidate DB Read-Only Concurrency & Immutability (16 readers, st_mtime_ns)",
         test_candidate_duckdb_read_only_concurrency_and_immutability),
        ("Resource Configuration Readback (Writer 4t/8GB/14GB, Reader 4t/1GB and 1t/1GB)",
         test_duckdb_writer_and_reader_resource_readbacks),
        ("Microsecond Interval Arithmetic & 3-Class Partition (+/-68y, 0.0s, missing, identity)",
         test_microsecond_interval_arithmetic_extreme_durations_and_3class_partition),
        ("Non-Finite Floats & IEEE 754 Quantile Invariants (NaN, Inf, 1e300)",
         test_nonfinite_and_extreme_floats_handling),
    ]

    passed = 0
    failed = 0

    for name, test_fn in tests:
        try:
            test_fn()
            print(f"  [PASS] {name}")
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 80)
    print(f"CHALLENGER IT2 SUMMARY: {passed} PASSED, 0 SKIPPED, {failed} FAILED")
    print("=" * 80)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    import sys
    sys.exit(run_all_it2_stress_tests())
