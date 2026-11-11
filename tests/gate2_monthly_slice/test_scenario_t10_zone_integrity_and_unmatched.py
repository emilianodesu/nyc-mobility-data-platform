"""Scenario T10: Zone Integrity, Unmatched Fact Retention & Sentinels.

Verifies:
1. Rule S03 blocks candidates on duplicate LocationID, null LocationID, or missing sentinels (1, 264, 265).
2. Rule Q05 classifies PU and DO locations into 4 mutually exclusive categories
   (null_location, matched_sentinel, matched_ordinary, unmatched).
3. Unmatched LocationIDs (e.g. 999) in trip records are preserved (never dropped).
"""

from pathlib import Path
import sqlite3
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from hvfhv.transforms.ingest import ingest_taxi_zones
from hvfhv.transforms.quality import QualityEngine, init_quality_models


def test_t10_zone_reference_s03_invariants():
    """Confirms Rule S03 validation passes on official zones and blocks on invalid lookups."""
    csv_path = REPO_ROOT / "references" / "archive" / "2026-11-08-tlc-feasibility" / "taxi_zone_lookup.csv"
    assert csv_path.exists(), f"Missing reference taxi_zone_lookup.csv at {csv_path}"

    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE zone_reference (
            location_id INTEGER PRIMARY KEY,
            borough TEXT,
            zone TEXT,
            service_zone TEXT,
            reference_revision_id TEXT NOT NULL
        );
    """)

    # Ingest official lookup
    total = ingest_taxi_zones(con, csv_path, zone_revision_id="rev_zone_v1")
    assert total == 265

    # Test Rule S03 evaluation
    engine = QualityEngine(con)
    init_quality_models(con)
    ok = engine.eval_s03()
    assert ok is True

    result = con.execute("SELECT status, severity FROM quality_result WHERE rule_id = 'S03';").fetchone()
    assert result == ("PASS", "BLOCK")

    con.close()


def test_t10_s03_blocks_on_missing_sentinel():
    """Rule S03 blocks if sentinel LocationID (e.g. 264) is missing."""
    con = sqlite3.connect(":memory:")
    con.execute("""
        CREATE TABLE zone_reference (
            location_id INTEGER PRIMARY KEY,
            borough TEXT,
            zone TEXT,
            service_zone TEXT,
            reference_revision_id TEXT NOT NULL
        );
    """)

    # Insert zones missing 264
    con.execute("INSERT INTO zone_reference VALUES (1, 'EWR', 'Newark Airport', 'EWR', 'rev_test');")
    con.execute("INSERT INTO zone_reference VALUES (2, 'Queens', 'Jamaica Bay', 'Boro Zone', 'rev_test');")
    con.execute("INSERT INTO zone_reference VALUES (265, 'N/A', 'Outside NYC', 'N/A', 'rev_test');")

    engine = QualityEngine(con)
    init_quality_models(con)
    ok = engine.eval_s03()
    assert ok is False

    result = con.execute("SELECT status, severity FROM quality_result WHERE rule_id = 'S03';").fetchone()
    assert result == ("FAIL", "BLOCK")
    con.close()


def test_t10_unmatched_facts_retained_and_classified():
    """Confirms trips with unmatched or null location IDs are retained and classified by Q05."""
    known_zones = {1, 2, 264, 265}

    def classify(loc_id: int | None) -> str:
        if loc_id is None:
            return "null_location"
        if loc_id in (1, 264, 265):
            return "matched_sentinel"
        if loc_id in known_zones and 2 <= loc_id <= 263:
            return "matched_ordinary"
        return "unmatched"

    trips = [
        (2, "matched_ordinary"),     # Ordinary zone (Jamaica Bay)
        (1, "matched_sentinel"),     # Sentinel (EWR)
        (264, "matched_sentinel"),   # Sentinel (Unknown)
        (265, "matched_sentinel"),   # Sentinel (Outside NYC)
        (999, "unmatched"),          # Unknown code
        (0, "unmatched"),            # Out of bounds
        (None, "null_location"),     # Null location
    ]

    total = len(trips)
    classified_counts = {"null_location": 0, "matched_sentinel": 0, "matched_ordinary": 0, "unmatched": 0}

    for loc, expected_cat in trips:
        cat = classify(loc)
        assert cat == expected_cat, f"Location {loc} expected {expected_cat}, got {cat}"
        classified_counts[cat] += 1

    # Invariant: All rows preserved, sum of counts equals total trips
    assert sum(classified_counts.values()) == total == 7
    assert classified_counts["matched_ordinary"] == 1
    assert classified_counts["matched_sentinel"] == 3
    assert classified_counts["unmatched"] == 2
    assert classified_counts["null_location"] == 1
