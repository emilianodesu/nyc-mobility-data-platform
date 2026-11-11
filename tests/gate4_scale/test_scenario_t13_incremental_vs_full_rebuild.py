"""Scenario T13 & W02: Incremental Revision & Window Advance vs. Full Rebuild Equivalence.

Acceptance Criteria:
1. Re-ingesting or replacing a source revision removes its prior contribution without row inflation.
2. Full rebuild and incremental build under the identical frozen input plan yield identical:
   - source_contribution row counts and revision IDs
   - trip_observation physical locators (contiguous 0..n-1)
   - interval_daily and interval_monthly continuous quantiles
   - coverage absent-month NULL row count semantics
   - pre-seal quality rule statuses (S01–S07, Q01–Q09)
"""

from __future__ import annotations

import datetime
import hashlib
from pathlib import Path
import tempfile
import uuid

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hvfhv.contracts.identity import (
    compute_revision_id,
    compute_derivation_id,
    FrozenInputPlan,
    SourceRevisionEntry,
)
from hvfhv.transforms.builder import (
    build_analytical_candidate,
    verify_build_equivalence,
    IncrementalEquivalenceError,
)


def _create_mock_parquet(path: Path, count: int, base_date: str = "2024-12-15") -> str:
    """Creates a deterministic valid Parquet file and returns its sha256."""
    data = {
        "hvfhs_license_num": ["HV0003"] * count,
        "dispatching_base_num": ["B03404"] * count,
        "originating_base_num": ["B03404"] * count,
        "request_datetime": [datetime.datetime.strptime(f"{base_date} 12:00:00", "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=i) for i in range(count)],
        "on_scene_datetime": [datetime.datetime.strptime(f"{base_date} 12:02:00", "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=i) for i in range(count)],
        "pickup_datetime": [datetime.datetime.strptime(f"{base_date} 12:03:00", "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=i) for i in range(count)],
        "dropoff_datetime": [datetime.datetime.strptime(f"{base_date} 12:15:00", "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=i) for i in range(count)],
        "PULocationID": [100 + (i % 50) for i in range(count)],
        "DOLocationID": [200 + (i % 50) for i in range(count)],
        "trip_miles": [2.5 + (i % 10) * 0.1 for i in range(count)],
        "trip_time": [720 for _ in range(count)],
        "base_passenger_fare": [25.0 for _ in range(count)],
        "tolls": [0.0 for _ in range(count)],
        "bcf": [0.75 for _ in range(count)],
        "sales_tax": [2.2 for _ in range(count)],
        "congestion_surcharge": [2.75 for _ in range(count)],
        "airport_fee": [0.0 for _ in range(count)],
        "tips": [5.0 for _ in range(count)],
        "driver_pay": [20.0 for _ in range(count)],
        "shared_request_flag": ["N"] * count,
        "shared_match_flag": ["N"] * count,
        "access_a_ride_flag": ["N"] * count,
        "wav_request_flag": ["N"] * count,
        "wav_match_flag": ["N"] * count,
    }
    table = pa.Table.from_pydict(data)
    pq.write_table(table, path)

    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def _create_mock_zones(path: Path) -> None:
    """Creates a taxi zones lookup CSV."""
    rows = ["LocationID,Borough,Zone,service_zone", "1,EWR,Newark Airport,EWR"]
    for i in range(2, 264):
        rows.append(f"{i},Manhattan,Zone {i},Yellow Zone")
    rows.append("264,Unknown,NV,N/A")
    rows.append("265,Unknown,NA,N/A")
    path.write_text("\n".join(rows), encoding="utf-8")


def test_t13_incremental_vs_full_rebuild_equivalence():
    """Verify that incremental build reproduces full rebuild with exact logical equivalence."""
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        raw_dir = tpath / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        zones_csv = tpath / "taxi_zones.csv"
        _create_mock_zones(zones_csv)

        # Generate month 1 (2024-11) and month 2 (2024-12)
        m1_sha = _create_mock_parquet(raw_dir / "2024-11.parquet", 500, "2024-11-15")
        m1_rev = compute_revision_id("tlc/hvfhv/2024-11", m1_sha)

        m2_sha = _create_mock_parquet(raw_dir / "2024-12.parquet", 600, "2024-12-15")
        m2_rev = compute_revision_id("tlc/hvfhv/2024-12", m2_sha)

        # Plan 1: Only Month 1
        plan1 = FrozenInputPlan(
            source_revisions=[
                SourceRevisionEntry("tlc/hvfhv/2024-11", m1_rev),
            ],
            window_months=["2024-11", "2024-12"],
            zone_revision_id="zone_rev_1",
            schema_adapter_version="hvfhv_24col_pre2025.v1",
            rules_hash="r" * 64,
            metric_definition_hash="m" * 64,
            code_bundle_manifest_hash="c" * 64,
            lockfile_hash="l" * 64,
            model_schema_version="1.0.0",
            semantic_configuration={},
        )

        staging_base = tpath / "staging_base"
        base_db = build_analytical_candidate(
            plan=plan1,
            staging_dir=staging_base,
            raw_data_dir=raw_dir,
            zone_lookup_csv=zones_csv,
            is_incremental=False,
        )

        # Plan 2: Advance window to include Month 1 and Month 2
        plan2 = FrozenInputPlan(
            source_revisions=[
                SourceRevisionEntry("tlc/hvfhv/2024-11", m1_rev),
                SourceRevisionEntry("tlc/hvfhv/2024-12", m2_rev),
            ],
            window_months=["2024-11", "2024-12"],
            zone_revision_id="zone_rev_1",
            schema_adapter_version="hvfhv_24col_pre2025.v1",
            rules_hash="r" * 64,
            metric_definition_hash="m" * 64,
            code_bundle_manifest_hash="c" * 64,
            lockfile_hash="l" * 64,
            model_schema_version="1.0.0",
            semantic_configuration={},
        )

        # Build Candidate A via Full Rebuild
        staging_full = tpath / "staging_full"
        full_db = build_analytical_candidate(
            plan=plan2,
            staging_dir=staging_full,
            raw_data_dir=raw_dir,
            zone_lookup_csv=zones_csv,
            is_incremental=False,
        )

        # Build Candidate B via Incremental Build from Base DB
        staging_inc = tpath / "staging_inc"
        inc_db = build_analytical_candidate(
            plan=plan2,
            staging_dir=staging_inc,
            raw_data_dir=raw_dir,
            zone_lookup_csv=zones_csv,
            base_release_db_path=base_db,
            is_incremental=True,
        )

        # Verify equivalence
        result = verify_build_equivalence(full_db, inc_db)
        assert result["status"] == "EQUIVALENT"
        assert result["trip_count"] == 1100
        assert result["contributions_count"] == 2


def test_t13_source_revision_replacement_removes_old_rows_without_inflation():
    """Verify that when a source revision changes, the old rows are removed and replaced (Scenario W02)."""
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        raw_dir = tpath / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        zones_csv = tpath / "taxi_zones.csv"
        _create_mock_zones(zones_csv)

        # Month 1 revision X (500 rows)
        x_sha = _create_mock_parquet(raw_dir / "2024-12.parquet", 500, "2024-12-15")
        x_rev = compute_revision_id("tlc/hvfhv/2024-12", x_sha)

        plan_x = FrozenInputPlan(
            source_revisions=[
                SourceRevisionEntry("tlc/hvfhv/2024-12", x_rev),
            ],
            window_months=["2024-12"],
            zone_revision_id="zone_rev_1",
            schema_adapter_version="hvfhv_24col_pre2025.v1",
            rules_hash="r" * 64,
            metric_definition_hash="m" * 64,
            code_bundle_manifest_hash="c" * 64,
            lockfile_hash="l" * 64,
            model_schema_version="1.0.0",
            semantic_configuration={},
        )

        staging_x = tpath / "staging_x"
        base_db = build_analytical_candidate(
            plan=plan_x,
            staging_dir=staging_x,
            raw_data_dir=raw_dir,
            zone_lookup_csv=zones_csv,
            is_incremental=False,
        )

        # Month 1 revision Y (replacing X with 700 rows)
        y_sha = _create_mock_parquet(raw_dir / f"{x_rev}.parquet", 700, "2024-12-15")
        y_rev = compute_revision_id("tlc/hvfhv/2024-12", y_sha)
        (raw_dir / f"{y_rev}.parquet").write_bytes((raw_dir / f"{x_rev}.parquet").read_bytes())

        plan_y = FrozenInputPlan(
            source_revisions=[
                SourceRevisionEntry("tlc/hvfhv/2024-12", y_rev),
            ],
            window_months=["2024-12"],
            zone_revision_id="zone_rev_1",
            schema_adapter_version="hvfhv_24col_pre2025.v1",
            rules_hash="r" * 64,
            metric_definition_hash="m" * 64,
            code_bundle_manifest_hash="c" * 64,
            lockfile_hash="l" * 64,
            model_schema_version="1.0.0",
            semantic_configuration={},
        )

        staging_y = tpath / "staging_y"
        inc_y = build_analytical_candidate(
            plan=plan_y,
            staging_dir=staging_y,
            raw_data_dir=raw_dir,
            zone_lookup_csv=zones_csv,
            base_release_db_path=base_db,
            is_incremental=True,
        )

        con = duckdb.connect(str(inc_y), read_only=True)
        try:
            total_trips = con.execute("SELECT count(*) FROM trip_observation").fetchone()[0]
            # Should be exactly 700, NOT 500 + 700 = 1200
            assert total_trips == 700, f"Expected 700 rows, got {total_trips} (row inflation detected)"

            # Ensure only revision Y is recorded in source_contribution
            contribs = con.execute("SELECT revision_id, inserted_row_count FROM source_contribution").fetchall()
            assert len(contribs) == 1
            assert contribs[0][0] == y_rev
            assert contribs[0][1] == 700
        finally:
            con.close()
