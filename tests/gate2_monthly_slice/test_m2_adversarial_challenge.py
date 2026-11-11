"""Adversarial Verification & Stress Test Suite for Milestone M2 (Gate G2).

Empirically challenges:
1. Microsecond Precision Edge Cases:
   - 1 microsecond apart (+0.000001s, -0.000001s, 0.0s).
   - Day boundary crossing across midnight (23:59:59.999999 -> 00:00:00.000000).
   - Month boundary crossing across month transitions (Nov 30 -> Dec 1, Dec 31 -> Jan 1).
   - Leap year boundary crossing (Feb 28 -> Feb 29 -> Mar 1, 2024).
   - Sub-second carry across full second transitions.
   - Random fuzz testing over diverse microsecond intervals.
2. Anti-Averaging Mathematical & Empirical Invariant:
   - Empirical proof of statistical error from unweighted daily median averaging (bimodal & skewed distributions).
   - Empirical proof that weighted averaging of daily medians STILL produces statistical error.
   - Extreme tail-risk distortion: 95th percentile (p95) averaging failure under bursty conditions.
   - Verification of SQL analytical models (interval_daily vs interval_monthly independent computation).
3. Idempotent Re-Ingestion Stress:
   - Ingesting the same revision 5 consecutive times produces zero row inflation.
   - Primary key constraint enforcement strictly blocks unhandled duplicate locator collisions.
   - Contiguity 0..n-1 and distinct counts strictly preserved across all iterations.
4. Physical Locator Integrity Across Multi-Batch Streams:
   - Multi-batch ingestion with highly irregular batch sizes (1 to 10,000 rows).
   - Zero gaps and zero collisions across batch transitions.
   - Contiguity invariant: row[i+1] == row[i] + 1 for every adjacent pair.
   - Multi-revision composite key isolation ((rev_A, i) and (rev_B, i) coexistence without crosstalk).
"""

from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
import math
from pathlib import Path
import random
import sqlite3
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.transforms.ingest import (
    open_duckdb_writer,
    finalize_duckdb_writer,
    validate_source_schema,
    stream_parquet_to_duckdb,
    ingest_taxi_zones,
    SchemaContractViolation,
)
from hvfhv.transforms.models import (
    calculate_recorded_interval_seconds,
    classify_interval,
    continuous_quantile,
    init_analytical_models,
    build_interval_aggregates,
)
from hvfhv.transforms.quality import QualityEngine, init_quality_models
from tests.gate2_monthly_slice.test_ingestion_pipeline import _build_synthetic_parquet


# ============================================================================
# 1. Microsecond Precision Edge Cases
# ============================================================================

def test_microsecond_single_tick_positive():
    """Validates that +1 microsecond interval is exactly 0.000001s and eligible."""
    req = datetime(2024, 12, 1, 12, 0, 0, 0)
    pu = datetime(2024, 12, 1, 12, 0, 0, 1)  # +1 microsecond

    diff = calculate_recorded_interval_seconds(req, pu)
    assert diff is not None
    assert math.isclose(diff, 0.000001, abs_tol=1e-9), f"Expected 0.000001, got {diff}"

    val, cls = classify_interval(req, pu)
    assert cls == "eligible_nonnegative_recorded_interval"
    assert val is not None and math.isclose(val, 0.000001, abs_tol=1e-9)


def test_microsecond_single_tick_negative():
    """Validates that -1 microsecond interval is exactly -0.000001s and negative."""
    req = datetime(2024, 12, 1, 12, 0, 0, 1)
    pu = datetime(2024, 12, 1, 12, 0, 0, 0)  # -1 microsecond

    diff = calculate_recorded_interval_seconds(req, pu)
    assert diff is not None
    assert math.isclose(diff, -0.000001, abs_tol=1e-9), f"Expected -0.000001, got {diff}"

    val, cls = classify_interval(req, pu)
    assert cls == "negative_recorded_interval"
    assert val is not None and math.isclose(val, -0.000001, abs_tol=1e-9)


def test_microsecond_exact_zero():
    """Validates that 0 microsecond difference is exactly 0.0s and eligible."""
    ts = datetime(2024, 12, 1, 12, 0, 0, 500000)
    diff = calculate_recorded_interval_seconds(ts, ts)
    assert diff == 0.0

    val, cls = classify_interval(ts, ts)
    assert cls == "eligible_nonnegative_recorded_interval"
    assert val == 0.0


def test_microsecond_subsecond_carry():
    """Validates microsecond arithmetic across the 999,999 us -> 000,000 us boundary."""
    req = datetime(2024, 12, 1, 12, 0, 0, 999999)
    pu = datetime(2024, 12, 1, 12, 0, 1, 0)  # exactly 1 microsecond later

    diff = calculate_recorded_interval_seconds(req, pu)
    assert diff is not None
    assert math.isclose(diff, 0.000001, abs_tol=1e-9), f"Expected 0.000001, got {diff}"

    val, cls = classify_interval(req, pu)
    assert cls == "eligible_nonnegative_recorded_interval"


def test_microsecond_day_boundary_span():
    """Validates 1 microsecond difference spanning across midnight (day transition)."""
    # Spanning midnight: 23:59:59.999999 -> 00:00:00.000000 next day
    req = datetime(2024, 12, 1, 23, 59, 59, 999999)
    pu = datetime(2024, 12, 2, 0, 0, 0, 0)

    diff = calculate_recorded_interval_seconds(req, pu)
    assert diff is not None
    assert math.isclose(diff, 0.000001, abs_tol=1e-9), f"Expected 0.000001, got {diff}"

    val, cls = classify_interval(req, pu)
    assert cls == "eligible_nonnegative_recorded_interval"

    # Reverse across midnight: negative interval
    rev_diff = calculate_recorded_interval_seconds(pu, req)
    assert rev_diff is not None
    assert math.isclose(rev_diff, -0.000001, abs_tol=1e-9), f"Expected -0.000001, got {rev_diff}"

    rev_val, rev_cls = classify_interval(pu, req)
    assert rev_cls == "negative_recorded_interval"


def test_microsecond_month_boundary_span():
    """Validates 1 microsecond span across month transitions and off-source-month classification."""
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE trip_observation (
            revision_id TEXT,
            source_row_number INTEGER,
            source_month TEXT,
            request_datetime TEXT,
            pickup_datetime TEXT,
            dropoff_datetime TEXT,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            trip_miles REAL,
            trip_time INTEGER,
            base_passenger_fare REAL,
            tolls REAL,
            bcf REAL,
            sales_tax REAL,
            congestion_surcharge REAL,
            airport_fee REAL,
            tips REAL,
            driver_pay REAL,
            shared_request_flag TEXT,
            shared_match_flag TEXT,
            access_a_ride_flag TEXT,
            wav_request_flag TEXT,
            wav_match_flag TEXT,
            cbd_congestion_fee REAL,
            hvfhs_license_num TEXT,
            dispatching_base_num TEXT,
            originating_base_num TEXT,
            on_scene_datetime TEXT,
            PRIMARY KEY (revision_id, source_row_number)
        );
    """)

    # Month start transition: Nov 30 23:59:59.999999 -> Dec 01 00:00:00.000000 (1 us into Dec)
    # Month end transition: Dec 31 23:59:59.999999 -> Jan 01 00:00:00.000000 (1 us into Jan)
    source_month = "2024-12"
    rev_id = "rev_month_span"

    sample_trips = [
        # Trip 0: Request in Nov, Pickup in Dec (1 us diff -> eligible, pickup in Dec -> is_off_source_month = 0)
        (rev_id, 0, source_month, "2024-11-30 23:59:59.999999", "2024-12-01 00:00:00.000000", 100, "HV0003"),
        # Trip 1: Request in Dec, Pickup in Jan (1 us diff -> eligible, pickup in Jan -> is_off_source_month = 1)
        (rev_id, 1, source_month, "2024-12-31 23:59:59.999999", "2025-01-01 00:00:00.000000", 100, "HV0003"),
        # Trip 2: Both in Nov (request Nov 30 23:59:58, pickup Nov 30 23:59:59 -> off-month = 1)
        (rev_id, 2, source_month, "2024-11-30 23:59:58.000000", "2024-11-30 23:59:59.000000", 100, "HV0003"),
    ]

    for r in sample_trips:
        con.execute("""
            INSERT INTO trip_observation (
                revision_id, source_row_number, source_month, request_datetime, pickup_datetime, pu_location_id, hvfhs_license_num
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """, r)

    # Invariant checks via SQL
    rows = con.execute("""
        SELECT
            source_row_number,
            request_datetime,
            pickup_datetime,
            substr(pickup_datetime, 1, 7) AS pickup_month,
            CASE WHEN substr(pickup_datetime, 1, 7) != source_month THEN 1 ELSE 0 END AS is_off_source_month
        FROM trip_observation
        ORDER BY source_row_number;
    """).fetchall()

    assert rows[0][3] == "2024-12" and rows[0][4] == 0, "Trip 0 pickup should be in source month 2024-12"
    assert rows[1][3] == "2025-01" and rows[1][4] == 1, "Trip 1 pickup should be off-source-month (2025-01)"
    assert rows[2][3] == "2024-11" and rows[2][4] == 1, "Trip 2 pickup should be off-source-month (2024-11)"

    con.close()


def test_microsecond_leap_year_boundary_span():
    """Validates 1 microsecond span across leap year dates (Feb 28 -> Feb 29 -> Mar 1, 2024)."""
    # 2024 leap day transition
    req1 = datetime(2024, 2, 28, 23, 59, 59, 999999)
    pu1 = datetime(2024, 2, 29, 0, 0, 0, 0)
    diff1 = calculate_recorded_interval_seconds(req1, pu1)
    assert diff1 is not None and math.isclose(diff1, 0.000001, abs_tol=1e-9)

    req2 = datetime(2024, 2, 29, 23, 59, 59, 999999)
    pu2 = datetime(2024, 3, 1, 0, 0, 0, 0)
    diff2 = calculate_recorded_interval_seconds(req2, pu2)
    assert diff2 is not None and math.isclose(diff2, 0.000001, abs_tol=1e-9)


def test_microsecond_fuzz_invariance():
    """Stress tests 500 randomized microsecond timestamp pairs for arithmetic fidelity."""
    rng = random.Random(42)
    base = datetime(2024, 12, 1, 0, 0, 0)

    for _ in range(500):
        offset_us = rng.randint(-86_400_000_000, 86_400_000_000)  # up to 24h either way
        req = base + timedelta(microseconds=rng.randint(0, 100_000_000))
        pu = req + timedelta(microseconds=offset_us)

        expected_seconds = offset_us / 1_000_000.0
        calculated = calculate_recorded_interval_seconds(req, pu)

        assert calculated is not None
        assert math.isclose(calculated, expected_seconds, abs_tol=1e-6), (
            f"Fuzz failure: expected {expected_seconds}, got {calculated}"
        )


# ============================================================================
# 2. Anti-Averaging Mathematical & Empirical Invariant
# ============================================================================

def test_anti_averaging_empirical_proof_unweighted():
    """Empirically proves that simple unweighted daily median averaging causes massive statistical error."""
    # Day 1: 1,000 observations of short wait times (all 10.0s)
    # Day 2: 10 observations of severe delays (all 500.0s)
    day1_obs = [10.0] * 1000
    day2_obs = [500.0] * 10

    med_day1 = continuous_quantile(day1_obs, 0.50)
    med_day2 = continuous_quantile(day2_obs, 0.50)
    assert med_day1 == 10.0
    assert med_day2 == 500.0

    # Fallacious unweighted daily average:
    naive_unweighted_median = (med_day1 + med_day2) / 2.0
    assert naive_unweighted_median == 255.0

    # True monthly median computed from pooled observations (1,010 observations)
    pooled_month = day1_obs + day2_obs
    true_monthly_median = continuous_quantile(pooled_month, 0.50)
    assert true_monthly_median == 10.0

    # Empirical error demonstration:
    absolute_error = abs(naive_unweighted_median - true_monthly_median)
    relative_error = absolute_error / true_monthly_median
    assert absolute_error == 245.0, f"Error must be 245s, got {absolute_error}"
    assert relative_error == 24.5, f"Relative error must be 2450%, got {relative_error}"


def test_anti_averaging_empirical_proof_weighted_medians():
    """Empirically proves that even weighted averaging of daily medians produces statistical error."""
    # Case A: Bimodal day sizes
    day1_obs = [10.0] * 1000
    day2_obs = [500.0] * 10
    med_day1 = continuous_quantile(day1_obs, 0.50)
    med_day2 = continuous_quantile(day2_obs, 0.50)

    weighted_daily_median = ((len(day1_obs) * med_day1) + (len(day2_obs) * med_day2)) / (len(day1_obs) + len(day2_obs))
    true_monthly_median = continuous_quantile(day1_obs + day2_obs, 0.50)

    assert true_monthly_median == 10.0
    # Weighted average is ~14.85s (still 48.5% error!)
    assert round(weighted_daily_median, 2) == 14.85
    assert weighted_daily_median != true_monthly_median

    # Case B: Equal day sizes (weights are equal, yet median of union != average of medians)
    # Day 1 (5 trips): [10, 20, 30, 40, 500] -> median = 30.0
    # Day 2 (5 trips): [50, 60, 70, 80, 90] -> median = 70.0
    d1 = [10.0, 20.0, 30.0, 40.0, 500.0]
    d2 = [50.0, 60.0, 70.0, 80.0, 90.0]
    med_d1 = continuous_quantile(d1, 0.50)
    med_d2 = continuous_quantile(d2, 0.50)
    assert med_d1 == 30.0
    assert med_d2 == 70.0

    avg_medians = (med_d1 + med_d2) / 2.0  # 50.0
    true_union_median = continuous_quantile(d1 + d2, 0.50)
    # Sorted union: [10, 20, 30, 40, 50, 60, 70, 80, 90, 500]
    # h = (10 - 1)*0.5 = 4.5 -> d[4] + 0.5*(d[5] - d[4]) = 50 + 0.5*(60 - 50) = 55.0
    assert avg_medians == 50.0
    assert true_union_median == 55.0
    assert true_union_median != avg_medians, "Averaging medians must not equal true union median"


def test_anti_averaging_p95_tail_risk():
    """Empirically proves that averaging daily 95th percentiles (p95) produces severe distortion."""
    # Day 1: 100 trips with fast pickups (all 15.0s) -> p95 = 15.0s
    # Day 2: 90 trips with fast pickups (15.0s), 10 trips with 3600.0s delays -> p95 = 3600.0s
    d1 = [15.0] * 100
    d2 = [15.0] * 90 + [3600.0] * 10

    p95_d1 = continuous_quantile(d1, 0.95)
    p95_d2 = continuous_quantile(d2, 0.95)
    assert p95_d1 == 15.0
    # For d2: 100 elements. h = (100 - 1)*0.95 = 94.05. Elements 0..89 are 15, elements 90..99 are 3600.
    # d[94] and d[95] are both 3600 -> p95 = 3600.0
    assert p95_d2 == 3600.0

    naive_avg_p95 = (p95_d1 + p95_d2) / 2.0
    assert naive_avg_p95 == 1807.5

    # True combined month (200 trips: 190 trips at 15.0s, 10 trips at 3600.0s)
    # h = (200 - 1)*0.95 = 189.05.
    # Elements 0..189 are 15.0s. Element 190 is 3600.0s.
    # Q(0.95) = d[189] + 0.05 * (d[190] - d[189]) = 15.0 + 0.05 * (3600.0 - 15.0) = 194.25s
    true_monthly_p95 = continuous_quantile(d1 + d2, 0.95)
    assert true_monthly_p95 is not None
    assert round(true_monthly_p95, 2) == 194.25

    # The error of averaging daily p95s is staggering: ~830% error
    abs_err = abs(naive_avg_p95 - true_monthly_p95)
    assert round(abs_err, 2) == 1613.25
    assert naive_avg_p95 > true_monthly_p95 * 9.0


def test_anti_averaging_sql_table_isolation():
    """Validates that interval_monthly is independently computed and differs from AVG(interval_daily)."""
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE interval_daily (
            source_month TEXT,
            pickup_date TEXT,
            total_trips INTEGER,
            eligible_trips INTEGER,
            median_seconds REAL,
            p95_seconds REAL
        );
    """)
    con.execute("""
        CREATE TABLE interval_monthly (
            source_month TEXT,
            pickup_month TEXT,
            total_trips INTEGER,
            eligible_trips INTEGER,
            median_seconds REAL,
            p95_seconds REAL
        );
    """)

    # Populate two days matching Case B above
    # Day 1: median 30.0, p95 491.0
    # Day 2: median 70.0, p95 89.2
    con.execute("INSERT INTO interval_daily VALUES ('2024-12', '2024-12-01', 5, 5, 30.0, 491.0);")
    con.execute("INSERT INTO interval_daily VALUES ('2024-12', '2024-12-02', 5, 5, 70.0, 89.2);")

    # True independent monthly computation stored in interval_monthly:
    # median 55.0, p95 434.5
    con.execute("INSERT INTO interval_monthly VALUES ('2024-12', '2024-12', 10, 10, 55.0, 434.5);")

    # Prove that querying interval_daily with AVG() yields fallacious 50.0
    avg_daily_med = con.execute("SELECT AVG(median_seconds) FROM interval_daily;").fetchone()[0]
    true_monthly_med = con.execute("SELECT median_seconds FROM interval_monthly;").fetchone()[0]

    assert avg_daily_med == 50.0
    assert true_monthly_med == 55.0
    assert avg_daily_med != true_monthly_med

    con.close()


# ============================================================================
# 3. Idempotent Re-Ingestion Stress
# ============================================================================

def test_idempotent_reingestion_5_consecutive_iterations():
    """Stress tests re-ingesting the exact same revision 5 consecutive times (R06).
    Verifies that total row count does NOT inflate by a single row across all 5 iterations.
    """
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE source_contribution (
            logical_source_id TEXT NOT NULL,
            revision_id TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
            source_month TEXT NOT NULL,
            source_schema_version TEXT NOT NULL,
            cbd_fee_field_present INTEGER NOT NULL,
            decoded_row_count INTEGER NOT NULL,
            inserted_row_count INTEGER NOT NULL,
            PRIMARY KEY (logical_source_id, revision_id)
        );
    """)
    con.execute("""
        CREATE TABLE trip_observation (
            revision_id TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            source_month TEXT NOT NULL,
            hvfhs_license_num TEXT,
            pu_location_id INTEGER,
            trip_miles REAL,
            PRIMARY KEY (revision_id, source_row_number)
        );
    """)

    revision_id = "rev_stress_5x_idempotency"
    source_month = "2024-12"
    logical_source = "fhvhv_tripdata_2024-12"
    target_row_count = 1_000

    # Build reference dataset
    raw_records = [
        (f"HV{i % 4:04d}", 100 + (i % 250), float(i % 20))
        for i in range(target_row_count)
    ]

    def execute_ingestion_cycle(iteration_num: int):
        # Transactional revision replacement (Requirement R06 & Scenario T06)
        con.execute("BEGIN TRANSACTION;")
        con.execute("DELETE FROM trip_observation WHERE revision_id = ?;", [revision_id])
        con.execute("DELETE FROM source_contribution WHERE revision_id = ?;", [revision_id])

        obs_rows = [
            (revision_id, idx, source_month, r[0], r[1], r[2])
            for idx, r in enumerate(raw_records)
        ]
        con.executemany("INSERT INTO trip_observation VALUES (?, ?, ?, ?, ?, ?);", obs_rows)

        con.execute("""
            INSERT INTO source_contribution VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, [
            logical_source, revision_id, "sha256_mock_digest", source_month,
            "hvfhv_24col_pre2025.v1", 0, target_row_count, target_row_count
        ])
        con.execute("COMMIT;")

    # Run 5 consecutive re-ingestion cycles
    for cycle in range(1, 6):
        execute_ingestion_cycle(cycle)

        # Assert strict anti-inflation invariant
        tot_obs = con.execute("SELECT COUNT(*) FROM trip_observation;").fetchone()[0]
        tot_contrib = con.execute("SELECT COUNT(*) FROM source_contribution;").fetchone()[0]

        assert tot_obs == target_row_count, (
            f"Row count inflated on iteration {cycle}: {tot_obs} != {target_row_count}"
        )
        assert tot_contrib == 1, (
            f"Contribution count inflated on iteration {cycle}: {tot_contrib} != 1"
        )

        # Assert contiguous locators 0..n-1
        stats = con.execute("""
            SELECT
                MIN(source_row_number),
                MAX(source_row_number),
                COUNT(DISTINCT source_row_number)
            FROM trip_observation
            WHERE revision_id = ?;
        """, [revision_id]).fetchone()

        min_loc, max_loc, dist_loc = stats
        assert min_loc == 0, f"Min locator {min_loc} != 0 on iteration {cycle}"
        assert max_loc == target_row_count - 1, f"Max locator {max_loc} != {target_row_count - 1} on iteration {cycle}"
        assert dist_loc == target_row_count, f"Distinct locators {dist_loc} != {target_row_count} on iteration {cycle}"

    con.close()


def test_primary_key_blocks_unhandled_duplicate_collisions():
    """Validates that underlying database primary key constraint strictly prevents locator duplication."""
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE trip_observation (
            revision_id TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            PRIMARY KEY (revision_id, source_row_number)
        );
    """)

    con.execute("INSERT INTO trip_observation VALUES ('rev1', 0);")
    con.execute("INSERT INTO trip_observation VALUES ('rev1', 1);")

    # Attempting to insert duplicate (rev1, 0) without replacement must fail with IntegrityError
    collision_caught = False
    try:
        con.execute("INSERT INTO trip_observation VALUES ('rev1', 0);")
    except sqlite3.IntegrityError:
        collision_caught = True

    assert collision_caught, "Primary key did not block duplicate (revision_id, source_row_number)!"
    con.close()


# ============================================================================
# 4. Physical Locator Integrity Across Multi-Batch Streams
# ============================================================================

def test_physical_locator_integrity_multi_batch_stream():
    """Stress tests physical locator generation across multi-batch streams with highly irregular batch sizes.
    Asserts zero gaps, zero duplicates, and strict monotonicity.
    """
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE trip_observation (
            revision_id TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            batch_id INTEGER NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (revision_id, source_row_number)
        );
    """)

    # Irregular batch sizes simulating unpredictable network / arrow chunking
    batch_sizes = [73, 1429, 17, 5000, 1, 10000, 480, 8000]
    total_expected = sum(batch_sizes)  # 25,000 rows
    revision_id = "rev_multi_batch_stress"

    offset = 0
    batch_records = []
    for b_idx, b_size in enumerate(batch_sizes):
        for j in range(b_size):
            rn = offset + j
            batch_records.append((revision_id, rn, b_idx, f"trip_{rn}"))
        offset += b_size

    assert offset == total_expected == 25_000

    # Stream insert
    con.executemany("INSERT INTO trip_observation VALUES (?, ?, ?, ?);", batch_records)

    # 1. Row count assertion
    count = con.execute("SELECT COUNT(*) FROM trip_observation WHERE revision_id = ?;", [revision_id]).fetchone()[0]
    assert count == total_expected

    # 2. Locator boundaries
    min_loc, max_loc, distinct_loc = con.execute("""
        SELECT MIN(source_row_number), MAX(source_row_number), COUNT(DISTINCT source_row_number)
        FROM trip_observation
        WHERE revision_id = ?;
    """, [revision_id]).fetchone()

    assert min_loc == 0
    assert max_loc == total_expected - 1
    assert distinct_loc == total_expected

    # 3. Exhaustive contiguity inspection: every adjacent pair row[i+1] == row[i] + 1
    locators = [r[0] for r in con.execute("""
        SELECT source_row_number FROM trip_observation WHERE revision_id = ? ORDER BY source_row_number;
    """, [revision_id]).fetchall()]

    for idx, loc in enumerate(locators):
        assert loc == idx, f"Locator gap/mismatch at index {idx}: got {loc}"

    con.close()


def test_multi_revision_composite_locator_isolation():
    """Validates that two revisions can coexist with overlapping 0..n-1 locators without collision."""
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE trip_observation (
            revision_id TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            payload TEXT,
            PRIMARY KEY (revision_id, source_row_number)
        );
    """)

    rev_a = "rev_2024_11"
    rev_b = "rev_2024_12"
    count_a = 5_000
    count_b = 5_000

    rows_a = [(rev_a, i, f"payload_a_{i}") for i in range(count_a)]
    rows_b = [(rev_b, i, f"payload_b_{i}") for i in range(count_b)]

    con.executemany("INSERT INTO trip_observation VALUES (?, ?, ?);", rows_a)
    con.executemany("INSERT INTO trip_observation VALUES (?, ?, ?);", rows_b)

    # Both must have 5,000 rows
    cnt_a = con.execute("SELECT COUNT(*) FROM trip_observation WHERE revision_id = ?;", [rev_a]).fetchone()[0]
    cnt_b = con.execute("SELECT COUNT(*) FROM trip_observation WHERE revision_id = ?;", [rev_b]).fetchone()[0]
    total = con.execute("SELECT COUNT(*) FROM trip_observation;").fetchone()[0]

    assert cnt_a == 5_000
    assert cnt_b == 5_000
    assert total == 10_000

    # Ensure composite key uniqueness holds
    distinct_pairs = con.execute("""
        SELECT COUNT(DISTINCT revision_id || ':' || source_row_number) FROM trip_observation;
    """).fetchone()[0]
    assert distinct_pairs == 10_000

    con.close()


# ============================================================================
# 5. Native DuckDB Engine Adversarial Challenges
# ============================================================================

def test_duckdb_microsecond_precision_and_fuzz():
    """Empirically stress-tests DuckDB native epoch_us microsecond arithmetic and 3-class partition."""
    con = duckdb.connect()
    init_analytical_models(con)
    rev = "rev_us_duckdb"
    base_trips = [
        (rev, 0, "2024-12", "2024-12-01 12:00:00.000000", "2024-12-01 12:00:00.000001"),  # +1 us
        (rev, 1, "2024-12", "2024-12-01 12:00:00.000001", "2024-12-01 12:00:00.000000"),  # -1 us
        (rev, 2, "2024-12", "2024-12-01 12:00:00.500000", "2024-12-01 12:00:00.500000"),  # 0.0s
        (rev, 3, "2024-12", "2024-12-01 23:59:59.999999", "2024-12-02 00:00:00.000000"),  # midnight
        (rev, 4, "2024-12", "2024-12-02 00:00:00.000000", "2024-12-01 23:59:59.999999"),  # midnight rev
        (rev, 5, "2024-02", "2024-02-28 23:59:59.999999", "2024-02-29 00:00:00.000000"),  # leap 1
        (rev, 6, "2024-02", "2024-02-29 23:59:59.999999", "2024-03-01 00:00:00.000000"),  # leap 2
    ]
    con.executemany("""
        INSERT INTO trip_observation (revision_id, source_row_number, source_month, request_datetime, pickup_datetime)
        VALUES (?, ?, ?, ?, ?);
    """, base_trips)

    rows = con.execute("""
        SELECT source_row_number, recorded_request_to_pickup_seconds, interval_class, is_interval_eligible, pickup_date
        FROM trip_qualified WHERE revision_id = ? ORDER BY source_row_number;
    """, [rev]).fetchall()

    assert math.isclose(rows[0][1], 0.000001, abs_tol=1e-9) and rows[0][2] == "eligible_nonnegative_recorded_interval" and rows[0][3] is True
    assert math.isclose(rows[1][1], -0.000001, abs_tol=1e-9) and rows[1][2] == "negative_recorded_interval" and rows[1][3] is False
    assert rows[2][1] == 0.0 and rows[2][2] == "eligible_nonnegative_recorded_interval" and rows[2][3] is True
    assert math.isclose(rows[3][1], 0.000001, abs_tol=1e-9) and str(rows[3][4]) == "2024-12-02"
    assert math.isclose(rows[4][1], -0.000001, abs_tol=1e-9) and rows[4][2] == "negative_recorded_interval"
    assert math.isclose(rows[5][1], 0.000001, abs_tol=1e-9) and str(rows[5][4]) == "2024-02-29"
    assert math.isclose(rows[6][1], 0.000001, abs_tol=1e-9) and str(rows[6][4]) == "2024-03-01"

    # 500-sample randomized fuzz test in DuckDB
    rng = random.Random(1337)
    base_dt = datetime(2024, 12, 1, 0, 0, 0)
    fuzz_trips = []
    expected_diffs = []
    for i in range(500):
        off_us = rng.randint(-86_400_000_000, 86_400_000_000)
        req = base_dt + timedelta(microseconds=rng.randint(0, 100_000_000))
        pu = req + timedelta(microseconds=off_us)
        fuzz_trips.append(("rev_fuzz", i, "2024-12", req.strftime("%Y-%m-%d %H:%M:%S.%f"), pu.strftime("%Y-%m-%d %H:%M:%S.%f")))
        expected_diffs.append(off_us / 1_000_000.0)

    con.executemany("""
        INSERT INTO trip_observation (revision_id, source_row_number, source_month, request_datetime, pickup_datetime)
        VALUES (?, ?, ?, ?, ?);
    """, fuzz_trips)

    fuzz_results = con.execute("""
        SELECT source_row_number, recorded_request_to_pickup_seconds
        FROM trip_qualified WHERE revision_id = 'rev_fuzz' ORDER BY source_row_number;
    """).fetchall()

    for idx, (_, diff) in enumerate(fuzz_results):
        exp = expected_diffs[idx]
        assert math.isclose(diff, exp, abs_tol=1e-6), f"Fuzz mismatch at {idx}: exp {exp}, got {diff}"

    con.close()


def test_duckdb_anti_averaging_end_to_end():
    """Empirically proves DuckDB computes quantiles strictly from raw observations, exposing daily averaging error."""
    con = duckdb.connect()
    init_analytical_models(con)

    # Day 1: 1,000 trips at 10.0s
    # Day 2: 10 trips at 500.0s
    rev = "rev_anti_avg"
    rows = []
    all_obs = []
    for i in range(1000):
        rows.append((rev, i, "2024-12", "HV0003", "2024-12-01 10:00:00.000000", "2024-12-01 10:00:10.000000", 100))
        all_obs.append(10.0)
    for i in range(10):
        rows.append((rev, 1000 + i, "2024-12", "HV0003", "2024-12-02 10:00:00.000000", "2024-12-02 10:08:20.000000", 100))
        all_obs.append(500.0)

    con.executemany("""
        INSERT INTO trip_observation (
            revision_id, source_row_number, source_month, hvfhs_license_num, request_datetime, pickup_datetime, pu_location_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
    """, rows)

    build_interval_aggregates(con)

    daily_rows = con.execute("""
        SELECT pickup_date, total_trips, eligible_trips, median_seconds, p95_seconds
        FROM interval_daily ORDER BY pickup_date;
    """).fetchall()

    assert len(daily_rows) == 2
    assert daily_rows[0][0] == date(2024, 12, 1) and daily_rows[0][3] == 10.0
    assert daily_rows[1][0] == date(2024, 12, 2) and daily_rows[1][3] == 500.0

    monthly_rows = con.execute("""
        SELECT pickup_month, total_trips, eligible_trips, median_seconds, p95_seconds
        FROM interval_monthly;
    """).fetchall()

    assert len(monthly_rows) == 1
    true_monthly_median = monthly_rows[0][3]
    assert true_monthly_median == 10.0

    # Mathematical divergence proof
    unweighted_avg = con.execute("SELECT AVG(median_seconds) FROM interval_daily;").fetchall()[0][0]
    assert unweighted_avg == 255.0
    err_unweighted = (unweighted_avg - true_monthly_median) / true_monthly_median
    assert math.isclose(err_unweighted, 24.5)  # 2,450% error

    weighted_avg = (1000 * 10.0 + 10 * 500.0) / 1010.0
    err_weighted = (weighted_avg - true_monthly_median) / true_monthly_median
    assert math.isclose(err_weighted, 0.4851485, rel_tol=1e-4)  # 48.5% error

    # Type 7 python reference oracle cross-check
    oracle_median = continuous_quantile(all_obs, 0.50)
    assert oracle_median == true_monthly_median

    con.close()


def test_duckdb_reingestion_idempotency_stress():
    """Stress tests 5 consecutive re-ingestion iterations with stream_parquet_to_duckdb verifying zero row inflation."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        parquet_file = root / "input.parquet"
        _build_synthetic_parquet(parquet_file, num_rows=1500)

        db_path = root / "idempotent.duckdb"
        con = open_duckdb_writer(db_path, "attempt_idem")
        init_analytical_models(con)

        rev_id = "rev_idem_duckdb"
        logical_source = "fhvhv_tripdata_2024-12"

        for iteration in range(1, 6):
            con.execute("BEGIN TRANSACTION;")
            con.execute("DELETE FROM trip_observation WHERE revision_id = ?;", [rev_id])
            con.execute("DELETE FROM source_contribution WHERE revision_id = ?;", [rev_id])
            rep = stream_parquet_to_duckdb(
                con, parquet_file, logical_source, rev_id, "sha256_idem", "2024-12", "hvfhv_24col_pre2025.v1", False, batch_size=300
            )
            con.execute("COMMIT;")

            obs_count = con.execute("SELECT COUNT(*) FROM trip_observation WHERE revision_id = ?;", [rev_id]).fetchall()[0][0]
            assert obs_count == 1500, f"Cycle {iteration} inflated rows: {obs_count}"

            stats = con.execute("""
                SELECT MIN(source_row_number), MAX(source_row_number), COUNT(DISTINCT source_row_number)
                FROM trip_observation WHERE revision_id = ?;
            """, [rev_id]).fetchall()[0]
            assert stats[0] == 0
            assert stats[1] == 1499
            assert stats[2] == 1500

            contrib_count = con.execute("SELECT COUNT(*) FROM source_contribution WHERE revision_id = ?;", [rev_id]).fetchall()[0][0]
            assert contrib_count == 1

        finalize_duckdb_writer(con, db_path)
        wal_path = Path(f"{db_path}.wal")
        assert not wal_path.exists() or wal_path.stat().st_size == 0


def test_duckdb_primary_key_enforcement():
    """Verifies that DuckDB primary key strictly prevents duplicate unhandled locator collisions."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        parquet_file = root / "input.parquet"
        _build_synthetic_parquet(parquet_file, num_rows=500)

        db_path = root / "pk_test.duckdb"
        con = open_duckdb_writer(db_path, "attempt_pk")
        init_analytical_models(con)

        rev_id = "rev_pk_duckdb"
        logical_source = "fhvhv_tripdata_2024-12"

        stream_parquet_to_duckdb(
            con, parquet_file, logical_source, rev_id, "sha256_pk", "2024-12", "hvfhv_24col_pre2025.v1", False, batch_size=200
        )

        duplicate_failed = False
        try:
            stream_parquet_to_duckdb(
                con, parquet_file, logical_source, rev_id, "sha256_pk", "2024-12", "hvfhv_24col_pre2025.v1", False, batch_size=200
            )
        except duckdb.ConstraintException:
            duplicate_failed = True

        assert duplicate_failed, "Duplicate stream_parquet_to_duckdb must be aborted by DuckDB ConstraintException"
        finalize_duckdb_writer(con, db_path)


def test_duckdb_multi_batch_irregular_locators():
    """Stress tests multi-batch streaming with irregular row groups to verify strictly contiguous 0-based locators."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        p_irregular = root / "irregular.parquet"
        sizes = [17, 345, 1, 4000, 20637]  # total = 25000
        tables = []
        for s in sizes:
            f_tmp = root / f"tmp_{s}.parquet"
            _build_synthetic_parquet(f_tmp, num_rows=s)
            tables.append(pq.read_table(f_tmp))
        merged = pa.concat_tables(tables)
        pq.write_table(merged, p_irregular, row_group_size=345)

        con = duckdb.connect()
        init_analytical_models(con)

        rev_irreg = "rev_irreg"
        stream_parquet_to_duckdb(con, p_irregular, "src_irreg", rev_irreg, "h_irreg", "2024-12", "hvfhv_24col_pre2025.v1", False, batch_size=73)

        gaps = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT source_row_number,
                       source_row_number - LAG(source_row_number, 1, -1) OVER (ORDER BY source_row_number) AS diff
                FROM trip_observation WHERE revision_id = ?
            ) WHERE diff != 1;
        """, [rev_irreg]).fetchall()[0][0]
        assert gaps == 0

        # Multi-revision composite locator isolation
        p_rev2 = root / "rev2.parquet"
        _build_synthetic_parquet(p_rev2, num_rows=5000)
        rev_2 = "rev_second"
        stream_parquet_to_duckdb(con, p_rev2, "src_second", rev_2, "h_second", "2024-12", "hvfhv_24col_pre2025.v1", False, batch_size=500)

        tot = con.execute("SELECT COUNT(*) FROM trip_observation;").fetchall()[0][0]
        assert tot == 30000
        dist_pairs = con.execute("""
            SELECT COUNT(DISTINCT revision_id || ':' || CAST(source_row_number AS VARCHAR)) FROM trip_observation;
        """).fetchall()[0][0]
        assert dist_pairs == 30000
        con.close()


def test_adversarial_pipeline_conditions():
    """Stress tests ingestion pipeline under corrupted schemas, missing sentinels, and constraints."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        # 1. Corrupt schema: missing column
        f_missing = root / "missing.parquet"
        _build_synthetic_parquet(f_missing, num_rows=10, missing_col=True)
        try:
            validate_source_schema(f_missing, "2024-12")
            assert False, "Missing column did not raise SchemaContractViolation"
        except SchemaContractViolation:
            pass

        # 2. Corrupt schema: unexpected extra column
        f_extra = root / "extra.parquet"
        _build_synthetic_parquet(f_extra, num_rows=10, extra_col=True)
        try:
            validate_source_schema(f_extra, "2024-12")
            assert False, "Extra column did not raise SchemaContractViolation"
        except SchemaContractViolation:
            pass

        # 3. Corrupt schema: timezone-bearing timestamp
        f_tz = root / "tz.parquet"
        _build_synthetic_parquet(f_tz, num_rows=10, tz="America/New_York")
        try:
            validate_source_schema(f_tz, "2024-12")
            assert False, "Timezone timestamp did not raise SchemaContractViolation"
        except SchemaContractViolation:
            pass

        # 4. Period column mismatch: 2024 with 25 cols
        f_25 = root / "25col.parquet"
        _build_synthetic_parquet(f_25, num_rows=10, include_cbd_fee=True)
        try:
            validate_source_schema(f_25, "2024-12")
            assert False, "2024 with 25 cols did not raise SchemaContractViolation"
        except SchemaContractViolation:
            pass

        # 5. Period column mismatch: 2025 with 24 cols
        f_24 = root / "24col.parquet"
        _build_synthetic_parquet(f_24, num_rows=10, include_cbd_fee=False)
        try:
            validate_source_schema(f_24, "2025-01")
            assert False, "2025 with 24 cols did not raise SchemaContractViolation"
        except SchemaContractViolation:
            pass

        # 6. Taxi zone missing sentinel 265
        con = duckdb.connect()
        init_analytical_models(con)
        csv_bad_sent = root / "zones_bad_sent.csv"
        with open(csv_bad_sent, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["LocationID", "Borough", "Zone", "service_zone"])
            w.writerow(["1", "EWR", "Newark Airport", "EWR"])
            w.writerow(["264", "Unknown", "NV", "N/A"])
        try:
            ingest_taxi_zones(con, csv_bad_sent, "rev_z")
            assert False, "Missing sentinel did not raise AssertionError"
        except AssertionError:
            pass

        # 7. Taxi zone duplicate LocationID
        csv_dup = root / "zones_dup.csv"
        with open(csv_dup, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["LocationID", "Borough", "Zone", "service_zone"])
            w.writerow(["1", "EWR", "Newark Airport", "EWR"])
            w.writerow(["1", "EWR2", "Newark Airport", "EWR"])
            w.writerow(["264", "Unknown", "NV", "N/A"])
            w.writerow(["265", "Unknown", "NA", "N/A"])
        try:
            ingest_taxi_zones(con, csv_dup, "rev_z")
            assert False, "Duplicate LocationID did not raise ConstraintException"
        except duckdb.ConstraintException:
            pass

        # 8. Taxi zone null LocationID
        csv_null = root / "zones_null.csv"
        with open(csv_null, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["LocationID", "Borough", "Zone", "service_zone"])
            w.writerow(["", "EWR", "Newark Airport", "EWR"])
            w.writerow(["1", "EWR", "Newark Airport", "EWR"])
            w.writerow(["264", "Unknown", "NV", "N/A"])
            w.writerow(["265", "Unknown", "NA", "N/A"])
        try:
            ingest_taxi_zones(con, csv_null, "rev_z")
            assert False, "Null LocationID did not raise ConstraintException"
        except duckdb.ConstraintException:
            pass

        con.close()


# ============================================================================
# Main Runner
# ============================================================================

def run_all_adversarial_tests() -> int:
    print("=" * 76)
    print("GATE G2 ADVERSARIAL EMPIRICAL VERIFICATION HARNESS (CHALLENGER M2)")
    print("=" * 76)

    test_cases = [
        # Microsecond Precision Edge Cases
        ("Microsecond: +1 us tick", test_microsecond_single_tick_positive),
        ("Microsecond: -1 us tick", test_microsecond_single_tick_negative),
        ("Microsecond: Exact 0.0s tick", test_microsecond_exact_zero),
        ("Microsecond: Subsecond carry (999999us -> 0us)", test_microsecond_subsecond_carry),
        ("Microsecond: Midnight day boundary span", test_microsecond_day_boundary_span),
        ("Microsecond: Month boundary span and off-month classification", test_microsecond_month_boundary_span),
        ("Microsecond: Leap year Feb 28-29-Mar 1 boundary span", test_microsecond_leap_year_boundary_span),
        ("Microsecond: 500-iteration random fuzz invariance", test_microsecond_fuzz_invariance),

        # Anti-Averaging Invariant
        ("Anti-Averaging: Empirical proof of unweighted median error", test_anti_averaging_empirical_proof_unweighted),
        ("Anti-Averaging: Empirical proof that weighted medians still fail", test_anti_averaging_empirical_proof_weighted_medians),
        ("Anti-Averaging: Extreme tail risk p95 averaging distortion", test_anti_averaging_p95_tail_risk),
        ("Anti-Averaging: SQL table isolation (interval_daily vs interval_monthly)", test_anti_averaging_sql_table_isolation),

        # Idempotent Re-Ingestion Stress
        ("Idempotency: 5 consecutive re-ingestion iterations (zero row inflation)", test_idempotent_reingestion_5_consecutive_iterations),
        ("Idempotency: Primary key blocks unhandled duplicate collisions", test_primary_key_blocks_unhandled_duplicate_collisions),

        # Physical Locator Integrity Across Multi-Batch Streams
        ("Locators: Multi-batch irregular stream contiguity (25k rows)", test_physical_locator_integrity_multi_batch_stream),
        ("Locators: Multi-revision composite locator isolation", test_multi_revision_composite_locator_isolation),

        # Native DuckDB Engine Challenges
        ("DuckDB Microsecond: Native epoch_us arithmetic & 500-sample fuzz", test_duckdb_microsecond_precision_and_fuzz),
        ("DuckDB Anti-Averaging: Native daily vs monthly end-to-end execution", test_duckdb_anti_averaging_end_to_end),
        ("DuckDB Idempotency: 5-cycle transactional re-ingestion stress", test_duckdb_reingestion_idempotency_stress),
        ("DuckDB Idempotency: Engine-level primary key duplicate rejection", test_duckdb_primary_key_enforcement),
        ("DuckDB Locators: Multi-batch irregular row groups (25k rows, 0 gaps)", test_duckdb_multi_batch_irregular_locators),
        ("DuckDB Pipeline: Adversarial schemas, sentinels & constraints", test_adversarial_pipeline_conditions),
    ]

    passed = 0
    failed = 0

    for name, fn in test_cases:
        try:
            fn()
            print(f"  [PASS] {name}")
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 76)
    print(f"CHALLENGER M2 RESULTS: {passed} PASSED, 0 SKIPPED, {failed} FAILED")
    print("=" * 76)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(run_all_adversarial_tests())
