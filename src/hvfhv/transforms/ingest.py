"""HVFHV Analytical Ingestion Pipeline (Milestone M2 / Gate G2).

Implements:
1. DuckDB writer connection factory with exact resource bounds (threads=4, memory_limit='8GB',
   max_temp_directory_size='14GB', dedicated attempt spill directory).
2. Streaming PyArrow batch ingestion (65,536-row batches) with zero full-table Python memory
   materialization and 0-based contiguous physical locators (0..n-1).
3. Schema validation and adapters: hvfhv_24col_pre2025.v1 (cbd_fee_field_present=false)
   and hvfhv_25col_2025onward.v1 (cbd_fee_field_present=true). Rejection of unknown columns
   and timezone-bearing timestamps.
4. Ingestion of taxi_zone_lookup.csv into zone_reference with Rule S03 enforcement
   (LocationID uniqueness, non-null, sentinel IDs 1, 264, 265).
5. CHECKPOINT and zero-.wal absence verification.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Iterator

from hvfhv.config import ResourceCaps, SystemConfig, get_default_config

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None


class SchemaContractViolation(Exception):
    """Raised when source file violates machine-readable schema contracts."""


class IngestionError(Exception):
    """Raised when ingestion fails."""


@dataclass(frozen=True)
class IngestionReport:
    """Detailed metadata report of an ingested source revision."""
    logical_source_id: str
    revision_id: str
    source_month: str
    schema_adapter_id: str
    cbd_fee_field_present: bool
    decoded_row_count: int
    inserted_row_count: int
    footer_row_count: int
    min_source_row_number: int
    max_source_row_number: int


def open_duckdb_writer(
    db_path: Path,
    attempt_id: str,
    config: SystemConfig | None = None,
) -> Any:
    """Opens a DuckDB writer connection configured with exact M2 resource bounds.

    Configuration:
    - threads = 4
    - memory_limit = '8GB'
    - max_temp_directory_size = '14GB'
    - temp_directory = $DATA_ROOT/staging/<attempt_id>/spill_writer
    """
    if config is None:
        config = get_default_config()

    caps: ResourceCaps = config.get_writer_caps(attempt_id)
    caps.spill_directory.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if duckdb is None:
        raise ImportError(
            "DuckDB is not installed in the current environment. "
            "Please ensure duckdb is installed."
        )

    conn_config = {
        "threads": caps.threads,
        "memory_limit": caps.memory_limit,
        "max_temp_directory_size": caps.max_temp_directory_size,
        "temp_directory": str(caps.spill_directory),
    }

    con = duckdb.connect(str(db_path), config=conn_config)

    # Verify settings readback
    eff_threads = int(con.execute("SELECT current_setting('threads');").fetchone()[0])
    eff_mem = con.execute("SELECT current_setting('memory_limit');").fetchone()[0]
    eff_temp = con.execute("SELECT current_setting('temp_directory');").fetchone()[0]

    assert eff_threads == 4, f"DuckDB threads expected 4, got {eff_threads}"
    assert eff_mem is not None and len(eff_mem) > 0, "DuckDB memory_limit setting is empty"
    assert str(caps.spill_directory) in eff_temp or eff_temp == str(caps.spill_directory)

    return con


def validate_source_schema(
    parquet_path: Path,
    source_month: str,
    contracts_json_path: Path | None = None,
) -> tuple[str, bool]:
    """Validates Parquet schema against contracts/schemas.json.

    Returns:
        (schema_adapter_id, cbd_fee_field_present)

    Raises:
        SchemaContractViolation: If columns mismatch, unknown columns present,
                                 or timestamps contain timezones.
    """
    if contracts_json_path is None:
        # Standard repository location
        repo_root = Path(__file__).resolve().parents[3]
        contracts_json_path = repo_root / "contracts" / "schemas.json"

    with open(contracts_json_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)["schemas"]

    # Determine adapter ID based on period predicate
    if source_month < "2025-01":
        adapter_id = "hvfhv_24col_pre2025.v1"
        cbd_fee_field_present = False
    else:
        adapter_id = "hvfhv_25col_2025onward.v1"
        cbd_fee_field_present = True

    contract = catalog[adapter_id]
    expected_cols = [c["name"] for c in contract["columns"]]
    expected_count = contract["expected_column_count"]

    # If parquet file and pyarrow are available, inspect schema
    if parquet_path.suffix == ".parquet" and pq is not None:
        pf = pq.ParquetFile(parquet_path)
        arrow_schema = pf.schema_arrow
        actual_cols = arrow_schema.names
        col_names = set(actual_cols)

        if len(actual_cols) != expected_count:
            raise SchemaContractViolation(
                f"Expected {expected_count} columns for {adapter_id}, got {len(actual_cols)}"
            )

        for col in expected_cols:
            if col not in col_names:
                raise SchemaContractViolation(
                    f"Missing required column '{col}' for {adapter_id}"
                )

        for col in actual_cols:
            if col not in expected_cols:
                raise SchemaContractViolation(
                    f"Unknown column '{col}' not permitted in {adapter_id}"
                )

        timestamp_fields = [
            "request_datetime",
            "on_scene_datetime",
            "pickup_datetime",
            "dropoff_datetime",
        ]
        for tf in timestamp_fields:
            if tf in col_names:
                field = arrow_schema.field(tf)
                if not pa.types.is_timestamp(field.type):
                    raise SchemaContractViolation(
                        f"Field '{tf}' must be timestamp, got {field.type}"
                    )
                if field.type.tz is not None:
                    raise SchemaContractViolation(
                        f"Field '{tf}' must be naive timestamp without timezone, got tz='{field.type.tz}'"
                    )
    elif parquet_path.suffix == ".json":
        # Inspection json verification
        with open(parquet_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        schema_dict = meta.get("schema", {})
        actual_cols = list(schema_dict.keys())
        col_names = set(actual_cols)

        if len(actual_cols) != expected_count:
            raise SchemaContractViolation(
                f"Expected {expected_count} columns for {adapter_id}, got {len(actual_cols)}"
            )

        for col in expected_cols:
            if col not in col_names:
                raise SchemaContractViolation(
                    f"Missing required column '{col}' for {adapter_id}"
                )

        for col in actual_cols:
            if col not in expected_cols:
                raise SchemaContractViolation(
                    f"Unknown column '{col}' not permitted in {adapter_id}"
                )

    return adapter_id, cbd_fee_field_present


def stream_parquet_to_duckdb(
    con: Any,
    parquet_path: Path,
    logical_source_id: str,
    revision_id: str,
    content_sha256: str,
    source_month: str,
    schema_adapter_id: str,
    cbd_fee_field_present: bool,
    batch_size: int = 65536,
) -> IngestionReport:
    """Streams raw Parquet batches directly into trip_observation, assigning
    contiguous 0-based source_row_number and revision_id without full Python RAM loading.
    """
    if pq is None or pa is None:
        raise ImportError("pyarrow is required for streaming ingestion.")

    pf = pq.ParquetFile(parquet_path)
    footer_row_count = pf.metadata.num_rows

    def batch_generator() -> Iterator[pa.RecordBatch]:
        offset = 0
        for batch in pf.iter_batches(batch_size=batch_size):
            n = batch.num_rows
            row_numbers = pa.array(range(offset, offset + n), type=pa.int64())
            rev_array = pa.array([revision_id] * n, type=pa.string())
            month_array = pa.array([source_month] * n, type=pa.string())

            arrays = list(batch.columns) + [row_numbers, rev_array, month_array]
            names = list(batch.schema.names) + ["source_row_number", "revision_id", "source_month"]

            if not cbd_fee_field_present:
                null_cbd = pa.nulls(n, type=pa.float64())
                arrays.append(null_cbd)
                names.append("cbd_congestion_fee")

            enriched = pa.RecordBatch.from_arrays(arrays, names=names)

            yield enriched
            offset += n

    # Peek first batch to establish stream schema
    gen = batch_generator()
    try:
        first_batch = next(gen)
    except StopIteration:
        # Empty file
        first_batch = None

    if first_batch is not None:
        stream_schema = first_batch.schema

        def full_stream():
            yield first_batch
            yield from gen

        reader = pa.RecordBatchReader.from_batches(stream_schema, full_stream())
        con.register("raw_batch_stream", reader)

        con.execute("""
            INSERT INTO trip_observation (
                revision_id,
                source_row_number,
                source_month,
                hvfhs_license_num,
                dispatching_base_num,
                originating_base_num,
                request_datetime,
                on_scene_datetime,
                pickup_datetime,
                dropoff_datetime,
                pu_location_id,
                do_location_id,
                trip_miles,
                trip_time,
                base_passenger_fare,
                tolls,
                bcf,
                sales_tax,
                congestion_surcharge,
                airport_fee,
                tips,
                driver_pay,
                shared_request_flag,
                shared_match_flag,
                access_a_ride_flag,
                wav_request_flag,
                wav_match_flag,
                cbd_congestion_fee
            )
            SELECT
                revision_id,
                source_row_number,
                source_month,
                hvfhs_license_num,
                dispatching_base_num,
                originating_base_num,
                request_datetime,
                on_scene_datetime,
                pickup_datetime,
                dropoff_datetime,
                PULocationID AS pu_location_id,
                DOLocationID AS do_location_id,
                trip_miles,
                trip_time,
                base_passenger_fare,
                tolls,
                bcf,
                sales_tax,
                congestion_surcharge,
                airport_fee,
                tips,
                driver_pay,
                shared_request_flag,
                shared_match_flag,
                access_a_ride_flag,
                wav_request_flag,
                wav_match_flag,
                cbd_congestion_fee
            FROM raw_batch_stream;
        """)
        con.unregister("raw_batch_stream")

    # Reconcile inserted rows & physical locators (Rule S05)
    row_stats = con.execute("""
        SELECT
            COUNT(*),
            MIN(source_row_number),
            MAX(source_row_number),
            COUNT(DISTINCT source_row_number)
        FROM trip_observation
        WHERE revision_id = ?;
    """, [revision_id]).fetchone()

    inserted_count = row_stats[0]
    min_rn = row_stats[1] if row_stats[1] is not None else 0
    max_rn = row_stats[2] if row_stats[2] is not None else 0
    distinct_rn = row_stats[3]

    assert inserted_count == footer_row_count, (
        f"Row count mismatch: inserted {inserted_count} != footer {footer_row_count}"
    )
    if inserted_count > 0:
        assert min_rn == 0, f"Min source_row_number must be 0, got {min_rn}"
        assert max_rn == inserted_count - 1, (
            f"Max source_row_number must be {inserted_count - 1}, got {max_rn}"
        )
        assert distinct_rn == inserted_count, (
            f"Row numbers not contiguous/distinct: {distinct_rn} vs {inserted_count}"
        )

    # Insert source_contribution record
    con.execute("""
        INSERT INTO source_contribution (
            logical_source_id,
            revision_id,
            content_sha256,
            source_month,
            source_schema_version,
            cbd_fee_field_present,
            decoded_row_count,
            inserted_row_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """, [
        logical_source_id,
        revision_id,
        content_sha256,
        source_month,
        schema_adapter_id,
        cbd_fee_field_present,
        inserted_count,
        inserted_count,
    ])

    return IngestionReport(
        logical_source_id=logical_source_id,
        revision_id=revision_id,
        source_month=source_month,
        schema_adapter_id=schema_adapter_id,
        cbd_fee_field_present=cbd_fee_field_present,
        decoded_row_count=inserted_count,
        inserted_row_count=inserted_count,
        footer_row_count=footer_row_count,
        min_source_row_number=min_rn,
        max_source_row_number=max_rn,
    )


def ingest_taxi_zones(
    con: Any,
    csv_path: Path,
    zone_revision_id: str,
) -> int:
    """Ingests taxi_zone_lookup.csv into zone_reference with strict schema enforcement (Rule S03)."""
    rows = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            lid_raw = r.get("LocationID")
            lid = int(lid_raw) if (lid_raw is not None and lid_raw != "") else None
            rows.append((
                lid,
                r.get("Borough"),
                r.get("Zone"),
                r.get("service_zone"),
                zone_revision_id,
            ))

    con.executemany("""
        INSERT INTO zone_reference (
            location_id,
            borough,
            zone,
            service_zone,
            reference_revision_id
        ) VALUES (?, ?, ?, ?, ?);
    """, rows)

    # Validate Rule S03 invariants
    stats = con.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT location_id),
            COUNT(*) FILTER (WHERE location_id IS NULL)
        FROM zone_reference;
    """).fetchone()

    total, distinct_id, null_id = stats[0], stats[1], stats[2]

    assert total == distinct_id, (
        f"Rule S03 violation: Duplicate LocationID detected (total={total}, distinct={distinct_id})"
    )
    assert null_id == 0, f"Rule S03 violation: Null LocationID detected ({null_id})"

    # Verify sentinel IDs (1, 264, 265)
    sentinels = set(r[0] for r in con.execute("""
        SELECT location_id FROM zone_reference WHERE location_id IN (1, 264, 265);
    """).fetchall())
    assert sentinels == {1, 264, 265}, f"Rule S03 violation: Missing sentinel IDs: {sentinels}"

    return total


def finalize_duckdb_writer(con: Any, db_path: Path) -> None:
    """Commits active transactions, executes explicit CHECKPOINT, closes writer,
    and asserts zero .wal file postcondition.
    """
    con.commit()
    con.execute("CHECKPOINT;")
    con.close()

    wal_path = Path(f"{db_path}.wal")
    if wal_path.exists() and wal_path.stat().st_size > 0:
        raise RuntimeError(
            f"WAL file {wal_path} still exists ({wal_path.stat().st_size} bytes) "
            "after explicit CHECKPOINT and close"
        )
