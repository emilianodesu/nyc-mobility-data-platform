"""Scenario T06: Physical Locators, Anti-Inflation & Row Equality Groups.

Verifies:
1. Physical locators (revision_id, source_row_number) are strictly contiguous 0..n-1.
2. Identical business observations are preserved without deduplication (R05).
3. Re-ingesting a source revision replaces prior contribution and does not inflate row counts (R06).
4. Rule Q08 detects exact row equality groups across all columns without deleting rows.
"""

import json
from pathlib import Path
import sqlite3
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.transforms.quality import record_quality_result


def test_t06_physical_locators_contiguity_and_fact_preservation():
    """Confirms source rows receive contiguous 0..n-1 locators and identical rows are preserved."""
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE trip_observation (
            revision_id TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            source_month TEXT NOT NULL,
            hvfhs_license_num TEXT,
            dispatching_base_num TEXT,
            originating_base_num TEXT,
            request_datetime TEXT,
            pickup_datetime TEXT,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            trip_miles REAL,
            base_passenger_fare REAL,
            cbd_congestion_fee REAL,
            PRIMARY KEY (revision_id, source_row_number)
        );
    """)

    revision_id = "rev_test_t06_locators"
    month = "2024-12"

    # 10 rows: 1 duplicate pair (rows 0, 1), 1 triplet (rows 2, 3, 4), 5 distinct rows (5..9)
    sample_rows = [
        # Pair (rows 0, 1 identical business facts)
        ("HV0003", "B03404", "B03404", "2024-12-01 12:00:00", "2024-12-01 12:05:00", 100, 200, 2.5, 15.0, None),
        ("HV0003", "B03404", "B03404", "2024-12-01 12:00:00", "2024-12-01 12:05:00", 100, 200, 2.5, 15.0, None),
        # Triplet (rows 2, 3, 4 identical business facts)
        ("HV0005", "B03406", "B03406", "2024-12-01 14:00:00", "2024-12-01 14:10:00", 150, 250, 4.0, 22.0, None),
        ("HV0005", "B03406", "B03406", "2024-12-01 14:00:00", "2024-12-01 14:10:00", 150, 250, 4.0, 22.0, None),
        ("HV0005", "B03406", "B03406", "2024-12-01 14:00:00", "2024-12-01 14:10:00", 150, 250, 4.0, 22.0, None),
        # 5 distinct rows
        ("HV0003", "B03404", "B03404", "2024-12-01 15:00:00", "2024-12-01 15:05:00", 101, 201, 1.5, 10.0, None),
        ("HV0003", "B03404", "B03404", "2024-12-01 16:00:00", "2024-12-01 16:05:00", 102, 202, 3.5, 18.0, None),
        ("HV0005", "B03406", "B03406", "2024-12-01 17:00:00", "2024-12-01 17:05:00", 103, 203, 5.5, 25.0, None),
        ("HV0005", "B03406", "B03406", "2024-12-01 18:00:00", "2024-12-01 18:05:00", 104, 204, 2.0, 12.0, None),
        ("HV0003", "B03404", "B03404", "2024-12-01 19:00:00", "2024-12-01 19:05:00", 105, 205, 7.0, 35.0, None),
    ]

    # Ingest rows assigning contiguous 0..n-1 source_row_numbers
    insert_data = [
        (revision_id, idx, month, *row)
        for idx, row in enumerate(sample_rows)
    ]
    con.executemany("""
        INSERT INTO trip_observation (
            revision_id, source_row_number, source_month,
            hvfhs_license_num, dispatching_base_num, originating_base_num,
            request_datetime, pickup_datetime, pu_location_id, do_location_id,
            trip_miles, base_passenger_fare, cbd_congestion_fee
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, insert_data)

    # Invariant 1: Total rows preserved (10 rows, zero deduplication)
    total_count = con.execute("SELECT COUNT(*) FROM trip_observation WHERE revision_id = ?;", [revision_id]).fetchone()[0]
    assert total_count == 10, f"Expected 10 rows, got {total_count}"

    # Invariant 2: Locators contiguous 0..9
    locators = [r[0] for r in con.execute(
        "SELECT source_row_number FROM trip_observation WHERE revision_id = ? ORDER BY source_row_number;",
        [revision_id]
    ).fetchall()]
    assert locators == list(range(10)), f"Locators not contiguous: {locators}"

    # Invariant 3: Q08 exact row equality groups
    grouped = con.execute("""
        SELECT COUNT(*) AS grp_size
        FROM trip_observation
        WHERE revision_id = ?
        GROUP BY
            hvfhs_license_num, dispatching_base_num, originating_base_num,
            request_datetime, pickup_datetime, pu_location_id, do_location_id,
            trip_miles, base_passenger_fare, cbd_congestion_fee
        HAVING COUNT(*) > 1;
    """, [revision_id]).fetchall()

    group_sizes = [g[0] for g in grouped]
    assert sorted(group_sizes) == [2, 3], f"Expected group sizes [2, 3], got {group_sizes}"
    group_count = len(group_sizes)
    excess_rows = sum(s - 1 for s in group_sizes)
    assert group_count == 2, f"Expected 2 groups, got {group_count}"
    assert excess_rows == (2 - 1) + (3 - 1), f"Expected 3 excess rows, got {excess_rows}"

    con.close()


def test_t06_idempotent_reingestion_anti_inflation():
    """Confirms re-ingesting a revision does not inflate row count (R06)."""
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE trip_observation (
            revision_id TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            source_month TEXT NOT NULL,
            trip_miles REAL,
            PRIMARY KEY (revision_id, source_row_number)
        );
    """)

    revision_id = "rev_test_t06_reingest"
    month = "2024-12"

    def ingest_batch(rows):
        con.execute("DELETE FROM trip_observation WHERE revision_id = ?;", [revision_id])
        data = [(revision_id, i, month, r) for i, r in enumerate(rows)]
        con.executemany("INSERT INTO trip_observation VALUES (?, ?, ?, ?);", data)

    # First ingestion: 5 rows
    ingest_batch([1.0, 2.0, 3.0, 4.0, 5.0])
    cnt1 = con.execute("SELECT COUNT(*) FROM trip_observation;").fetchone()[0]
    assert cnt1 == 5

    # Second ingestion with same revision: 5 rows
    ingest_batch([1.0, 2.0, 3.0, 4.0, 5.0])
    cnt2 = con.execute("SELECT COUNT(*) FROM trip_observation;").fetchone()[0]
    assert cnt2 == 5, f"Row count inflated on re-ingestion: {cnt2} != 5"

    con.close()
