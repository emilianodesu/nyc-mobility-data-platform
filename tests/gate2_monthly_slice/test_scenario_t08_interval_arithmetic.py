"""Scenario T08: Microsecond Interval Arithmetic, 3-Class Partition & Count Identity.

Verifies:
1. Exact 64-bit microsecond integer arithmetic on naive timestamps (no whole-second truncation).
2. Fractional and negative interval preservation (e.g. 0.200000s and -0.200000s).
3. 3-class mutually exclusive partition: missing_timestamp, negative_recorded_interval, eligible_nonnegative_recorded_interval.
4. Count identity: total == missing + negative + eligible.
5. Exact continuous quantiles over eligible population.
"""

from datetime import datetime
from pathlib import Path
import sqlite3
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.transforms.models import (
    calculate_recorded_interval_seconds,
    classify_interval,
    continuous_quantile,
)


def test_t08_naive_microsecond_arithmetic_precision():
    """Validates microsecond integer subtraction matching architectural oracle."""
    # Hand-calculated acceptance example from technical-readiness.md:
    # Request at 12:00:00.900000, pickup at 12:00:01.100000 -> 0.2 seconds
    req = datetime(2024, 12, 1, 12, 0, 0, 900000)
    pu = datetime(2024, 12, 1, 12, 0, 1, 100000)

    diff = calculate_recorded_interval_seconds(req, pu)
    assert diff is not None
    assert round(diff, 6) == 0.200000, f"Expected 0.200000, got {diff}"

    # Reversing order: negative interval preserved
    rev_diff = calculate_recorded_interval_seconds(pu, req)
    assert rev_diff is not None
    assert round(rev_diff, 6) == -0.200000, f"Expected -0.200000, got {rev_diff}"

    # Exact zero interval (eligible)
    zero_diff = calculate_recorded_interval_seconds(req, req)
    assert zero_diff == 0.0

    # Missing timestamps
    assert calculate_recorded_interval_seconds(None, pu) is None
    assert calculate_recorded_interval_seconds(req, None) is None
    assert calculate_recorded_interval_seconds(None, None) is None


def test_t08_interval_3class_partition_and_count_identity():
    """Curated 9-row fixture validating 3-class partition and count identity."""
    fixture_rows = [
        # Missing timestamps (3 rows)
        (None, datetime(2024, 12, 1, 12, 0, 0), "missing_timestamp"),
        (datetime(2024, 12, 1, 12, 0, 0), None, "missing_timestamp"),
        (None, None, "missing_timestamp"),
        # Negative intervals (2 rows)
        (datetime(2024, 12, 1, 12, 5, 0), datetime(2024, 12, 1, 12, 0, 0), "negative_recorded_interval"),  # -300s
        (datetime(2024, 12, 1, 12, 0, 1, 100000), datetime(2024, 12, 1, 12, 0, 0, 900000), "negative_recorded_interval"),  # -0.2s
        # Eligible non-negative intervals (4 rows: 0.0, 0.2, 1.0, 10.0)
        (datetime(2024, 12, 1, 12, 0, 0), datetime(2024, 12, 1, 12, 0, 0), "eligible_nonnegative_recorded_interval"),  # 0.0s
        (datetime(2024, 12, 1, 12, 0, 0, 800000), datetime(2024, 12, 1, 12, 0, 1, 0), "eligible_nonnegative_recorded_interval"),  # 0.2s
        (datetime(2024, 12, 1, 12, 0, 0), datetime(2024, 12, 1, 12, 0, 1), "eligible_nonnegative_recorded_interval"),  # 1.0s
        (datetime(2024, 12, 1, 12, 0, 0), datetime(2024, 12, 1, 12, 0, 10), "eligible_nonnegative_recorded_interval"),  # 10.0s
    ]

    total = len(fixture_rows)
    missing_count = 0
    negative_count = 0
    eligible_count = 0
    eligible_values = []

    for req, pu, expected_class in fixture_rows:
        val, cls = classify_interval(req, pu)
        assert cls == expected_class, f"Expected {expected_class}, got {cls} for ({req}, {pu})"

        if cls == "missing_timestamp":
            missing_count += 1
            assert val is None
        elif cls == "negative_recorded_interval":
            negative_count += 1
            assert val is not None and val < 0.0
        elif cls == "eligible_nonnegative_recorded_interval":
            eligible_count += 1
            assert val is not None and val >= 0.0
            eligible_values.append(val)

    # Invariant: Count identity holds
    assert missing_count == 3
    assert negative_count == 2
    assert eligible_count == 4
    assert total == missing_count + negative_count + eligible_count, (
        f"Identity violated: {total} != {missing_count} + {negative_count} + {eligible_count}"
    )

    # Continuous quantiles over eligible population [0.0, 0.2, 1.0, 10.0]
    med = continuous_quantile(eligible_values, 0.50)
    p95 = continuous_quantile(eligible_values, 0.95)

    assert med is not None and round(med, 2) == 0.60
    assert p95 is not None and round(p95, 2) == 8.65
