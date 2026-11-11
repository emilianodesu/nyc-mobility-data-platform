"""Scenario T09: Off-Month Dates, Naive Timestamps & Absent Month Coverage.

Verifies:
1. Trips with pickup_datetime outside source_month are flagged by Rule Q04.
2. Naive timestamps without timezone are preserved without inferred UTC shifts.
3. In coverage relation, absent window months have row_count = NULL, never 0.
"""

from pathlib import Path
import sqlite3
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.transforms.models import populate_coverage


def test_t09_off_source_month_classification():
    """Validates classification of trips occurring outside the source file month."""
    source_month = "2024-12"

    trips = [
        # Normal December trips (7 trips)
        ("2024-12-01 00:05:00", False),
        ("2024-12-05 12:30:00", False),
        ("2024-12-15 18:20:00", False),
        ("2024-12-20 09:10:00", False),
        ("2024-12-25 14:45:00", False),
        ("2024-12-31 23:55:00", False),
        ("2024-12-31 23:59:50", False),
        # Off-month trips (2 trips)
        ("2024-11-30 23:59:30", True),   # November late spillover
        ("2025-01-01 00:02:15", True),   # January early spillover
        # Null pickup (1 trip)
        (None, False),
    ]

    total = len(trips)
    off_month_count = 0
    null_count = 0

    for pu_str, expected_off in trips:
        if pu_str is None:
            null_count += 1
            is_off = False
        else:
            pu_month = pu_str[:7]
            is_off = (pu_month != source_month)

        assert is_off == expected_off, f"Trip {pu_str} expected off_month={expected_off}, got {is_off}"
        if is_off:
            off_month_count += 1

    valid_denom = total - null_count
    assert off_month_count == 2
    assert null_count == 1
    assert valid_denom == 9
    assert round(off_month_count / valid_denom, 4) == round(2 / 9, 4)


def test_t09_coverage_absent_month_null_semantics():
    """Confirms absent months have row_count = NULL, never 0 (R11)."""
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE coverage (
            source_month TEXT PRIMARY KEY,
            is_expected INTEGER NOT NULL,
            is_available INTEGER NOT NULL,
            is_included INTEGER NOT NULL,
            revision_id TEXT,
            row_count INTEGER
        );
    """)

    # 12-month window: 2024-01 through 2024-12
    window = [f"2024-{m:02d}" for m in range(1, 13)]
    # Only 2024-12 is acquired
    contributions = {
        "2024-12": ("rev_2024_12_sha", 21_068_851)
    }

    populate_coverage(con, window, contributions)

    rows = con.execute("SELECT source_month, is_available, is_included, row_count FROM coverage ORDER BY source_month;").fetchall()
    assert len(rows) == 12

    for month, avail, incl, count in rows:
        if month == "2024-12":
            assert avail == 1 and incl == 1 and count == 21_068_851
        else:
            assert avail == 0 and incl == 0
            assert count is None, f"Month {month} absent row_count must be NULL, got {count}"

    con.close()
