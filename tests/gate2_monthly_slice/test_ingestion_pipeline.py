"""Direct Unit & Integration Tests for DuckDB & PyArrow Ingestion Pipeline.

Exercises:
1. open_duckdb_writer & finalize_duckdb_writer (threads=4, memory_limit, spill, zero WAL postcondition)
2. validate_source_schema (hvfhv_24col_pre2025.v1, hvfhv_25col_2025onward.v1, timezone/unknown col rejections)
3. stream_parquet_to_duckdb (PyArrow RecordBatch streaming, 0-based contiguous source_row_number,
   multi-batch streaming, cbd_congestion_fee adaptation)
4. ingest_taxi_zones into DuckDB (LocationID uniqueness, sentinels 1, 264, 265, Rule S03 invariants)
"""

from __future__ import annotations

import datetime
from pathlib import Path
import tempfile
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hvfhv.transforms.ingest import (
    open_duckdb_writer,
    finalize_duckdb_writer,
    validate_source_schema,
    stream_parquet_to_duckdb,
    ingest_taxi_zones,
    SchemaContractViolation,
)
from hvfhv.transforms.models import init_analytical_models

REPO_ROOT = Path(__file__).resolve().parents[2]


def _build_synthetic_parquet(
    file_path: Path,
    num_rows: int = 1000,
    include_cbd_fee: bool = False,
    tz: str | None = None,
    extra_col: bool = False,
    missing_col: bool = False,
) -> None:
    """Helper to generate a well-typed Parquet file matching contracts."""
    ts_type = pa.timestamp("us", tz=tz)

    cols = {
        "hvfhs_license_num": pa.array(["HV0003"] * num_rows, type=pa.string()),
        "dispatching_base_num": pa.array(["B03404"] * num_rows, type=pa.string()),
        "originating_base_num": pa.array(["B03404"] * num_rows, type=pa.string()),
        "request_datetime": pa.array(
            [datetime.datetime(2024, 12, 1, 10, 0, 0) + datetime.timedelta(microseconds=i * 100) for i in range(num_rows)],
            type=ts_type,
        ),
        "on_scene_datetime": pa.array(
            [datetime.datetime(2024, 12, 1, 10, 2, 0) + datetime.timedelta(microseconds=i * 100) for i in range(num_rows)],
            type=ts_type,
        ),
        "pickup_datetime": pa.array(
            [datetime.datetime(2024, 12, 1, 10, 5, 0) + datetime.timedelta(microseconds=i * 100) for i in range(num_rows)],
            type=ts_type,
        ),
        "dropoff_datetime": pa.array(
            [datetime.datetime(2024, 12, 1, 10, 25, 0) + datetime.timedelta(microseconds=i * 100) for i in range(num_rows)],
            type=ts_type,
        ),
        "PULocationID": pa.array([100 + (i % 150) for i in range(num_rows)], type=pa.int64()),
        "DOLocationID": pa.array([200 + (i % 50) for i in range(num_rows)], type=pa.int64()),
        "trip_miles": pa.array([float(1 + (i % 20)) for i in range(num_rows)], type=pa.float64()),
        "trip_time": pa.array([1200 + i for i in range(num_rows)], type=pa.int64()),
        "base_passenger_fare": pa.array([25.0 + float(i % 50) for i in range(num_rows)], type=pa.float64()),
        "tolls": pa.array([0.0 for _ in range(num_rows)], type=pa.float64()),
        "bcf": pa.array([1.25 for _ in range(num_rows)], type=pa.float64()),
        "sales_tax": pa.array([2.0 for _ in range(num_rows)], type=pa.float64()),
        "congestion_surcharge": pa.array([2.75 for _ in range(num_rows)], type=pa.float64()),
        "airport_fee": pa.array([0.0 for _ in range(num_rows)], type=pa.float64()),
        "tips": pa.array([float(i % 5) for i in range(num_rows)], type=pa.float64()),
        "driver_pay": pa.array([18.0 + float(i % 30) for i in range(num_rows)], type=pa.float64()),
        "shared_request_flag": pa.array(["N"] * num_rows, type=pa.string()),
        "shared_match_flag": pa.array(["N"] * num_rows, type=pa.string()),
        "access_a_ride_flag": pa.array(["N"] * num_rows, type=pa.string()),
        "wav_request_flag": pa.array(["N"] * num_rows, type=pa.string()),
        "wav_match_flag": pa.array(["N"] * num_rows, type=pa.string()),
    }

    if missing_col:
        del cols["tips"]

    if extra_col:
        cols["unexpected_bonus_col"] = pa.array([1] * num_rows, type=pa.int64())

    if include_cbd_fee:
        cols["cbd_congestion_fee"] = pa.array([0.75 for _ in range(num_rows)], type=pa.float64())

    table = pa.Table.from_pydict(cols)
    pq.write_table(table, file_path, row_group_size=min(500, num_rows))


def test_open_and_finalize_duckdb_writer(tmp_path: Path):
    """Verifies open_duckdb_writer configures resource bounds and finalize cleans up WAL."""
    db_path = tmp_path / "writer_test.duckdb"
    con = open_duckdb_writer(db_path, "attempt_test_open_finalize")

    # Verify settings readback
    eff_threads = int(con.execute("SELECT current_setting('threads');").fetchone()[0])
    eff_mem = con.execute("SELECT current_setting('memory_limit');").fetchone()[0]
    eff_temp = con.execute("SELECT current_setting('temp_directory');").fetchone()[0]

    assert eff_threads == 4
    assert eff_mem is not None and len(eff_mem) > 0
    assert "spill_writer" in eff_temp

    init_analytical_models(con)
    con.execute("INSERT INTO coverage (source_month, is_expected, is_available, is_included) VALUES ('2024-12', true, true, true);")

    finalize_duckdb_writer(con, db_path)

    # Database file exists and WAL is 0 bytes or absent
    assert db_path.exists()
    wal_path = Path(f"{db_path}.wal")
    assert not wal_path.exists() or wal_path.stat().st_size == 0


def test_validate_source_schema_pre2025_and_2025onward(tmp_path: Path):
    """Verifies validate_source_schema correctly validates 24-col and 25-col schemas and rejects violations."""
    # 1. Valid pre-2025 (24 columns)
    p24 = tmp_path / "valid_2024_12.parquet"
    _build_synthetic_parquet(p24, num_rows=10, include_cbd_fee=False)
    adapter, cbd_present = validate_source_schema(p24, "2024-12")
    assert adapter == "hvfhv_24col_pre2025.v1"
    assert cbd_present is False

    # 2. Valid 2025 onward (25 columns)
    p25 = tmp_path / "valid_2025_01.parquet"
    _build_synthetic_parquet(p25, num_rows=10, include_cbd_fee=True)
    adapter25, cbd_present25 = validate_source_schema(p25, "2025-01")
    assert adapter25 == "hvfhv_25col_2025onward.v1"
    assert cbd_present25 is True

    # 3. Missing column rejected (column count mismatch)
    pmiss = tmp_path / "missing_col.parquet"
    _build_synthetic_parquet(pmiss, num_rows=10, missing_col=True)
    with pytest.raises(SchemaContractViolation, match="Expected 24 columns"):
        validate_source_schema(pmiss, "2024-12")

    # 4. Unknown column rejected (extra column count mismatch)
    pextra = tmp_path / "extra_col.parquet"
    _build_synthetic_parquet(pextra, num_rows=10, extra_col=True)
    with pytest.raises(SchemaContractViolation, match="Expected 24 columns"):
        validate_source_schema(pextra, "2024-12")

    # 5. Timezone-bearing timestamp rejected
    ptz = tmp_path / "tz_col.parquet"
    _build_synthetic_parquet(ptz, num_rows=10, tz="America/New_York")
    with pytest.raises(SchemaContractViolation, match="naive timestamp without timezone"):
        validate_source_schema(ptz, "2024-12")


def test_stream_parquet_to_duckdb_and_locators(tmp_path: Path):
    """Verifies stream_parquet_to_duckdb correctly streams batches, attaches contiguous locators,
    and inserts source contribution into DuckDB.
    """
    total_rows = 3500
    pfile = tmp_path / "test_stream.parquet"
    _build_synthetic_parquet(pfile, num_rows=total_rows, include_cbd_fee=False)

    db_path = tmp_path / "stream_test.duckdb"
    con = open_duckdb_writer(db_path, "attempt_test_stream")
    init_analytical_models(con)

    report = stream_parquet_to_duckdb(
        con=con,
        parquet_path=pfile,
        logical_source_id="fhvhv_tripdata_2024-12",
        revision_id="rev_test_stream_001",
        content_sha256="0" * 64,
        source_month="2024-12",
        schema_adapter_id="hvfhv_24col_pre2025.v1",
        cbd_fee_field_present=False,
        batch_size=512,  # Exercise multiple batch iterations
    )

    assert report.inserted_row_count == total_rows
    assert report.footer_row_count == total_rows
    assert report.min_source_row_number == 0
    assert report.max_source_row_number == total_rows - 1

    # Verify DuckDB records directly
    obs_count = con.execute("SELECT COUNT(*) FROM trip_observation WHERE revision_id = 'rev_test_stream_001';").fetchone()[0]
    assert obs_count == total_rows

    # Contiguity check in DuckDB
    bounds = con.execute("""
        SELECT MIN(source_row_number), MAX(source_row_number), COUNT(DISTINCT source_row_number)
        FROM trip_observation WHERE revision_id = 'rev_test_stream_001';
    """).fetchone()
    assert bounds[0] == 0
    assert bounds[1] == total_rows - 1
    assert bounds[2] == total_rows

    # Check cbd_congestion_fee is NULL
    null_cbd_count = con.execute("""
        SELECT COUNT(*) FROM trip_observation
        WHERE revision_id = 'rev_test_stream_001' AND cbd_congestion_fee IS NULL;
    """).fetchone()[0]
    assert null_cbd_count == total_rows

    # Check source_contribution
    contrib = con.execute("SELECT inserted_row_count, cbd_fee_field_present FROM source_contribution;").fetchone()
    assert contrib[0] == total_rows
    assert contrib[1] is False

    finalize_duckdb_writer(con, db_path)


def test_ingest_taxi_zones_duckdb(tmp_path: Path):
    """Verifies ingest_taxi_zones populates zone_reference in DuckDB and enforces Rule S03."""
    csv_path = REPO_ROOT / "references" / "archive" / "2026-11-08-tlc-feasibility" / "taxi_zone_lookup.csv"
    assert csv_path.exists(), f"Reference zone lookup missing: {csv_path}"

    db_path = tmp_path / "zone_test.duckdb"
    con = open_duckdb_writer(db_path, "attempt_test_zones")
    init_analytical_models(con)

    count = ingest_taxi_zones(con, csv_path, "zone_rev_v1")
    assert count == 265

    # Check sentinels in DuckDB
    sentinels = set(r[0] for r in con.execute("SELECT location_id FROM zone_reference WHERE location_id IN (1, 264, 265);").fetchall())
    assert sentinels == {1, 264, 265}

    finalize_duckdb_writer(con, db_path)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        print("Testing open and finalize...")
        test_open_and_finalize_duckdb_writer(tpath)
        print("Testing schema validation...")
        test_validate_source_schema_pre2025_and_2025onward(tpath)
        print("Testing streaming parquet to DuckDB...")
        test_stream_parquet_to_duckdb_and_locators(tpath)
        print("Testing zone ingestion into DuckDB...")
        test_ingest_taxi_zones_duckdb(tpath)
        print("ALL INGESTION PIPELINE TESTS PASSED!")
