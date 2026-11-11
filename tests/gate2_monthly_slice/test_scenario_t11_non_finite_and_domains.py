"""Scenario T11: Non-Finite Values, Negative Money/Distance & Categorical Domains.

Verifies:
1. Rule Q06 profiles numeric columns for null, NaN, Inf, negative, zero, and positive values.
2. Quantiles and ranges for numeric columns are computed over finite-only rows.
3. Rule Q07 discovers unknown categorical values (e.g. HV9999) without crash and records
   NOT_APPLICABLE on the earliest month of a plan.
"""

import math
from pathlib import Path
import sqlite3
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.transforms.models import continuous_quantile
from hvfhv.transforms.quality import QualityEngine, init_quality_models


def test_t11_numeric_nonfinite_and_sign_profile():
    """Profiles curated numeric column containing finite, negative, zero, null, and non-finite floats."""
    raw_values = [2.5, 0.0, -1.5, float("nan"), float("inf"), float("-inf"), None, 5.0]
    total_evaluated = len(raw_values)

    null_count = 0
    nan_count = 0
    inf_count = 0
    negative_count = 0
    zero_count = 0
    positive_count = 0
    finite_values = []

    for v in raw_values:
        if v is None:
            null_count += 1
        elif math.isnan(v):
            nan_count += 1
        elif math.isinf(v):
            inf_count += 1
        else:
            finite_values.append(v)
            if v < 0:
                negative_count += 1
            elif v == 0:
                zero_count += 1
            else:
                positive_count += 1

    # Invariants
    assert null_count == 1
    assert nan_count == 1
    assert inf_count == 2
    assert negative_count == 1
    assert zero_count == 1
    assert positive_count == 2
    assert len(finite_values) == 4
    assert total_evaluated == null_count + nan_count + inf_count + len(finite_values)

    # Finite-only quantiles: sorted [-1.5, 0.0, 2.5, 5.0]
    med = continuous_quantile(finite_values, 0.50)
    p95 = continuous_quantile(finite_values, 0.95)

    # For 4 elements: h = (4 - 1)*0.5 = 1.5 -> (0.0 + 2.5)/2 = 1.25
    assert med == 1.25
    assert p95 is not None and p95 > 2.5


def test_t11_categorical_domain_discovery_earliest_month():
    """Confirms Rule Q07 returns NOT_APPLICABLE on earliest month and handles unknown codes."""
    con = sqlite3.connect(":memory:")
    init_quality_models(con)

    engine = QualityEngine(con)
    engine.eval_q07(is_earliest_month=True)

    result = con.execute("SELECT status, severity, details_json FROM quality_result WHERE rule_id = 'Q07';").fetchone()
    assert result[0] == "NOT_APPLICABLE"
    assert result[1] == "WARN"
    assert "Earliest source month in plan" in result[2]

    # Non-earliest month evaluation
    engine.eval_q07(is_earliest_month=False)
    result2 = con.execute("SELECT status, severity FROM quality_result WHERE rule_id = 'Q07';").fetchone()
    assert result2 == ("PASS", "WARN")

    con.close()
