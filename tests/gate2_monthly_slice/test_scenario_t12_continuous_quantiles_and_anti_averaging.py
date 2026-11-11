"""Scenario T12: Continuous Quantiles (Type 7) & Anti-Averaging Invariant.

Verifies:
1. Exact Hyndman-Fan Type 7 continuous quantile algorithm matching DuckDB quantile_cont.
2. Boundary handling: empty population -> None, singleton -> value itself.
3. Reference oracle: [0.0, 0.2, 1.0, 10.0] -> median=0.60, p95=8.65.
4. Anti-averaging invariant: monthly quantiles computed independently from observations,
   never by averaging daily subgroup quantiles (Simpson's paradox avoidance).
"""

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.transforms.models import continuous_quantile


def test_t12_type7_continuous_quantile_oracles():
    """Validates boundary values and mathematical oracles of Type 7 continuous quantiles."""
    # 1. Empty population -> None
    assert continuous_quantile([], 0.50) is None
    assert continuous_quantile([], 0.95) is None

    # 2. Singleton -> identical value
    assert continuous_quantile([42.0], 0.50) == 42.0
    assert continuous_quantile([42.0], 0.95) == 42.0

    # 3. Two elements: [10.0, 20.0]
    # h = (2 - 1)*0.5 = 0.5 -> 10 + 0.5*10 = 15.0
    # h = (2 - 1)*0.95 = 0.95 -> 10 + 0.95*10 = 19.5
    assert continuous_quantile([10.0, 20.0], 0.50) == 15.0
    assert continuous_quantile([10.0, 20.0], 0.95) == 19.5

    # 4. Reference oracle from Acceptance Plan: [0.0, 0.2, 1.0, 10.0]
    # h = (4 - 1)*0.5 = 1.5 -> data[1] + 0.5*(data[2] - data[1]) = 0.2 + 0.5*(1.0 - 0.2) = 0.60
    # h = (4 - 1)*0.95 = 2.85 -> data[2] + 0.85*(data[3] - data[2]) = 1.0 + 0.85*(10.0 - 1.0) = 8.65
    sample = [0.0, 0.2, 1.0, 10.0]
    med = continuous_quantile(sample, 0.50)
    p95 = continuous_quantile(sample, 0.95)
    assert med is not None and round(med, 2) == 0.60, f"Expected median=0.60, got {med}"
    assert p95 is not None and round(p95, 2) == 8.65, f"Expected p95=8.65, got {p95}"


def test_t12_anti_averaging_mathematical_invariant():
    """Proves that averaging daily quantiles is mathematically invalid and rejected (R10)."""
    # Day 1: 2 observations [10.0, 20.0] -> median = 15.0
    day1_observations = [10.0, 20.0]
    median_day1 = continuous_quantile(day1_observations, 0.50)
    assert median_day1 == 15.0

    # Day 2: 3 observations [30.0, 40.0, 50.0] -> median = 40.0
    day2_observations = [30.0, 40.0, 50.0]
    median_day2 = continuous_quantile(day2_observations, 0.50)
    assert median_day2 == 40.0

    # Fallacious approach: average of daily medians
    naive_avg_daily_medians = (median_day1 + median_day2) / 2.0
    assert naive_avg_daily_medians == 27.5

    # Correct platform approach: exact continuous quantile over all 5 monthly observations
    all_month_observations = day1_observations + day2_observations
    true_monthly_median = continuous_quantile(all_month_observations, 0.50)
    # [10.0, 20.0, 30.0, 40.0, 50.0]: h = (5 - 1)*0.5 = 2.0 -> data[2] = 30.0
    assert true_monthly_median == 30.0

    # The invariant: true monthly median != average of daily medians
    assert true_monthly_median != naive_avg_daily_medians
    assert true_monthly_median == 30.0 and naive_avg_daily_medians == 27.5
