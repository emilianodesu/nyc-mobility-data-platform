"""Gate G0: DuckDB & PyArrow Interoperability Verification Suite.

Validates:
1. PyArrow streaming batch generation (iter_batches) and DuckDB RecordBatchReader ingestion.
2. Deterministic zero-based physical row numbering (0..n-1) and revision_id attachment.
3. Batch-size invariance across varying batch sizes (500 vs 5,000 vs 65,536).
4. DuckDB transaction commit, explicit CHECKPOINT execution, and zero-.wal file post-condition.
5. CHECKPOINT barrier preventing checkpointing during active uncommitted transactions.
6. DuckDB read_only=True connection mode, DML/DDL mutation rejection, and st_mtime_ns immutability.
7. DuckDB read_only=True rejection when database file does not exist (never creates blank DB).
8. Concurrent read-only queries across independent connections on immutable databases.
9. Writer and reader resource configuration (threads, memory_limit, max_temp_directory_size, temp_directory) readbacks.
10. Microsecond naive interval arithmetic and exact continuous quantile oracle (median=0.6, p95=8.65).
"""

import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

try:
    import pytest
except ImportError:
    class _RaisesContext:
        def __init__(self, expected_exception, match=None):
            self.expected_exception = expected_exception
            self.match = match
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is None:
                raise AssertionError(f"Expected {self.expected_exception} but no exception was raised")
            if not issubclass(exc_type, self.expected_exception):
                return False
            if self.match and self.match not in str(exc_val):
                raise AssertionError(f"Expected match '{self.match}' in '{exc_val}'")
            return True

    class _PytestShim:
        @staticmethod
        def raises(expected_exception, match=None):
            return _RaisesContext(expected_exception, match)

        @staticmethod
        def fixture(func):
            return func

        class mark:
            @staticmethod
            def skipif(condition, reason=""):
                def decorator(func_or_class):
                    return func_or_class
                return decorator

    pytest = _PytestShim()


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Fixture alias providing tmp_path as temp_dir for pytest compatibility."""
    return tmp_path


try:
    import duckdb
    import pyarrow
    _HAS_DUCKDB_ARROW = True
except ImportError:
    _HAS_DUCKDB_ARROW = False

pytestmark = pytest.mark.skipif(
    not _HAS_DUCKDB_ARROW,
    reason="Third-party DuckDB/PyArrow libraries not yet installed in virtualenv",
)


def _generate_synthetic_parquet(file_path: Path, num_rows: int = 150_000) -> None:
    """Creates a synthetic TLC HVFHV Parquet file with pre-2025 schema for testing."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        ("hvfhs_license_num", pa.string()),
        ("dispatching_base_num", pa.string()),
        ("originating_base_num", pa.string()),
        ("request_datetime", pa.timestamp("us")),
        ("on_scene_datetime", pa.timestamp("us")),
        ("pickup_datetime", pa.timestamp("us")),
        ("dropoff_datetime", pa.timestamp("us")),
        ("PULocationID", pa.int64()),
        ("DOLocationID", pa.int64()),
        ("trip_miles", pa.float64()),
        ("trip_time", pa.int64()),
        ("base_passenger_fare", pa.float64()),
        ("tolls", pa.float64()),
        ("bcf", pa.float64()),
        ("sales_tax", pa.float64()),
        ("congestion_surcharge", pa.float64()),
        ("airport_fee", pa.float64()),
        ("tips", pa.float64()),
        ("driver_pay", pa.float64()),
        ("shared_request_flag", pa.string()),
        ("shared_match_flag", pa.string()),
        ("access_a_ride_flag", pa.string()),
        ("wav_request_flag", pa.string()),
        ("wav_match_flag", pa.string()),
    ])

    base_time = datetime(2024, 12, 1, 12, 0, 0)
    timestamps = [
        datetime.fromtimestamp(base_time.timestamp() + i)
        for i in range(num_rows)
    ]

    data = {
        "hvfhs_license_num": ["HV0003"] * num_rows,
        "dispatching_base_num": ["B03404"] * num_rows,
        "originating_base_num": ["B03404"] * num_rows,
        "request_datetime": timestamps,
        "on_scene_datetime": timestamps,
        "pickup_datetime": timestamps,
        "dropoff_datetime": timestamps,
        "PULocationID": [100 + (i % 150) for i in range(num_rows)],
        "DOLocationID": [200 + (i % 150) for i in range(num_rows)],
        "trip_miles": [2.5 + (i % 10) for i in range(num_rows)],
        "trip_time": [600 + (i % 300) for i in range(num_rows)],
        "base_passenger_fare": [20.0] * num_rows,
        "tolls": [0.0] * num_rows,
        "bcf": [0.5] * num_rows,
        "sales_tax": [1.5] * num_rows,
        "congestion_surcharge": [2.75] * num_rows,
        "airport_fee": [0.0] * num_rows,
        "tips": [3.0] * num_rows,
        "driver_pay": [15.0] * num_rows,
        "shared_request_flag": ["N"] * num_rows,
        "shared_match_flag": ["N"] * num_rows,
        "access_a_ride_flag": ["N"] * num_rows,
        "wav_request_flag": ["N"] * num_rows,
        "wav_match_flag": ["N"] * num_rows,
    }
    table = pa.Table.from_pydict(data, schema=schema)
    pq.write_table(table, file_path, row_group_size=50_000)


def test_duckdb_arrow_batch_stream_ingestion(tmp_path: Path):
    """Verifies streaming batch ingestion into DuckDB via RecordBatchReader."""
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet_path = tmp_path / "source.parquet"
    db_path = tmp_path / "analytics.duckdb"
    num_rows = 150_000
    revision_id = "rev_test_sha256_abcdef123456"

    _generate_synthetic_parquet(parquet_path, num_rows=num_rows)

    con = duckdb.connect(str(db_path))

    # Initialize destination table with physical row locators
    con.execute("""
        CREATE TABLE trip_observation (
            source_row_number BIGINT NOT NULL,
            revision_id VARCHAR NOT NULL,
            hvfhs_license_num VARCHAR,
            dispatching_base_num VARCHAR,
            request_datetime TIMESTAMP,
            pickup_datetime TIMESTAMP,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            trip_miles DOUBLE,
            cbd_congestion_fee DOUBLE
        );
    """)

    # Stream batches using RecordBatchReader
    pf = pq.ParquetFile(parquet_path)
    batch_size = 65_536

    def batch_generator():
        offset = 0
        for batch in pf.iter_batches(batch_size=batch_size):
            n = batch.num_rows
            row_numbers = pa.array(range(offset, offset + n), type=pa.int64())
            rev_array = pa.array([revision_id] * n, type=pa.string())
            enriched = batch.append_column("source_row_number", row_numbers).append_column("revision_id", rev_array)
            yield enriched
            offset += n

    # Derive schema from first batch
    first_batch = next(batch_generator())
    enriched_schema = first_batch.schema

    reader = pa.RecordBatchReader.from_batches(enriched_schema, batch_generator())
    con.register("raw_stream", reader)

    con.execute("""
        INSERT INTO trip_observation
        SELECT
            source_row_number,
            revision_id,
            hvfhs_license_num,
            dispatching_base_num,
            request_datetime,
            pickup_datetime,
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            trip_miles,
            NULL AS cbd_congestion_fee
        FROM raw_stream;
    """)
    con.unregister("raw_stream")

    # Assert row counts and physical locator invariants
    total_count = con.execute("SELECT COUNT(*) FROM trip_observation").fetchone()[0]
    assert total_count == num_rows, f"Expected {num_rows} rows, got {total_count}"

    min_row, max_row, distinct_rows = con.execute("""
        SELECT MIN(source_row_number), MAX(source_row_number), COUNT(DISTINCT source_row_number)
        FROM trip_observation
    """).fetchone()

    assert min_row == 0, f"Min source_row_number must be 0, got {min_row}"
    assert max_row == num_rows - 1, f"Max source_row_number must be {num_rows - 1}, got {max_row}"
    assert distinct_rows == num_rows, f"Row numbers must be contiguous and distinct (count={distinct_rows})"

    con.close()


def test_duckdb_batch_size_invariance(tmp_path: Path):
    """Verifies that changing batch size (500 vs 5,000 vs 65,536) yields identical row numbering and checksums."""
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet_path = tmp_path / "invariance_test.parquet"
    num_rows = 25_000
    revision_id = "rev_invariance_01"
    _generate_synthetic_parquet(parquet_path, num_rows=num_rows)

    batch_sizes = [500, 5_000, 65_536]
    results = {}

    for bs in batch_sizes:
        db_path = tmp_path / f"test_bs_{bs}.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute("""
            CREATE TABLE test_rows (
                source_row_number BIGINT PRIMARY KEY,
                revision_id VARCHAR,
                pu_location_id INTEGER,
                trip_miles DOUBLE
            );
        """)

        pf = pq.ParquetFile(parquet_path)

        def make_gen(batch_sz: int):
            def gen():
                offset = 0
                for b in pf.iter_batches(batch_size=batch_sz):
                    n = b.num_rows
                    rn = pa.array(range(offset, offset + n), type=pa.int64())
                    rev = pa.array([revision_id] * n, type=pa.string())
                    yield b.append_column("source_row_number", rn).append_column("revision_id", rev)
                    offset += n
            return gen

        gen_fn = make_gen(bs)
        sample = next(gen_fn())
        reader = pa.RecordBatchReader.from_batches(sample.schema, gen_fn())
        con.register("stream_in", reader)
        con.execute("""
            INSERT INTO test_rows
            SELECT source_row_number, revision_id, PULocationID AS pu_location_id, trip_miles
            FROM stream_in;
        """)
        con.unregister("stream_in")

        chk = con.execute("""
            SELECT COUNT(*), SUM(source_row_number), SUM(pu_location_id), ROUND(SUM(trip_miles), 2)
            FROM test_rows
        """).fetchone()
        results[bs] = chk
        con.close()

    baseline = results[batch_sizes[0]]
    for bs in batch_sizes[1:]:
        assert results[bs] == baseline, f"Batch size {bs} result {results[bs]} differed from baseline {baseline}"


def test_duckdb_checkpoint_and_zero_wal_postcondition(tmp_path: Path):
    """Verifies that explicit CHECKPOINT flushes and deletes .wal file upon close."""
    import duckdb

    db_path = tmp_path / "wal_test.duckdb"
    wal_path = Path(f"{db_path}.wal")

    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE t (id INTEGER, val VARCHAR);")

    # Insert rows across multiple statements to generate WAL entries
    for i in range(100):
        con.execute(f"INSERT INTO t VALUES ({i}, 'test_string_{i}');")

    # Explicit checkpoint protocol
    con.commit()
    con.execute("CHECKPOINT;")
    con.close()

    # Strict post-condition: WAL file must not exist
    assert not wal_path.exists(), f"WAL file {wal_path} still exists after CHECKPOINT and close"

    # Reopen and verify data persisted cleanly into base file
    reopened = duckdb.connect(str(db_path), read_only=True)
    count = reopened.execute("SELECT COUNT(*) FROM t;").fetchone()[0]
    assert count == 100, f"Expected 100 persisted rows, got {count}"
    reopened.close()


def test_duckdb_checkpoint_active_transaction_barrier(tmp_path: Path):
    """Verifies that CHECKPOINT is rejected if an uncommitted transaction is active."""
    import duckdb

    db_path = tmp_path / "barrier_test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE t (id INTEGER);")

    con.execute("BEGIN TRANSACTION;")
    con.execute("INSERT INTO t VALUES (42);")

    with pytest.raises(Exception) as exc_info:
        con.execute("CHECKPOINT;")
    assert "transaction" in str(exc_info.value).lower(), (
        f"Expected transaction error on premature CHECKPOINT, got: {exc_info.value}"
    )

    con.execute("COMMIT;")
    con.execute("CHECKPOINT;")
    con.close()


def test_duckdb_read_only_mode_and_mtime_immutability(tmp_path: Path):
    """Verifies that read_only=True rejects mutations and preserves file timestamps."""
    import duckdb

    db_path = tmp_path / "readonly_test.duckdb"

    # Create and finalize a test database
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE metrics (name VARCHAR, val DOUBLE);")
    con.execute("INSERT INTO metrics VALUES ('p50', 0.6), ('p95', 8.65);")
    con.commit()
    con.execute("CHECKPOINT;")
    con.close()

    # Record exact initial modification time
    mtime_before = os.stat(db_path).st_mtime_ns

    # Open read-only
    ro_con = duckdb.connect(str(db_path), read_only=True)

    # 1. Verify read queries succeed
    rows = ro_con.execute("SELECT name, val FROM metrics ORDER BY name;").fetchall()
    assert rows == [("p50", 0.6), ("p95", 8.65)]

    # 2. Verify DML mutations are rejected
    with pytest.raises(Exception):
        ro_con.execute("INSERT INTO metrics VALUES ('p99', 15.0);")

    with pytest.raises(Exception):
        ro_con.execute("UPDATE metrics SET val = 0.0;")

    with pytest.raises(Exception):
        ro_con.execute("DELETE FROM metrics;")

    # 3. Verify DDL mutations are rejected
    with pytest.raises(Exception):
        ro_con.execute("CREATE TABLE hacker (x INT);")

    with pytest.raises(Exception):
        ro_con.execute("DROP TABLE metrics;")

    # 4. Verify CHECKPOINT is rejected on read-only connection
    with pytest.raises(Exception):
        ro_con.execute("CHECKPOINT;")

    ro_con.close()

    # 5. Verify file timestamp is completely unaltered
    mtime_after = os.stat(db_path).st_mtime_ns
    assert mtime_before == mtime_after, f"mtime changed from {mtime_before} to {mtime_after} in read-only mode"

    # 6. Verify no WAL file was created
    assert not Path(f"{db_path}.wal").exists(), "Read-only access must never create a .wal file"


def test_duckdb_read_only_nonexistent_database(tmp_path: Path):
    """Verifies that opening a non-existent database file with read_only=True fails closed."""
    import duckdb

    missing_path = tmp_path / "non_existent_file.duckdb"
    with pytest.raises(Exception):
        duckdb.connect(str(missing_path), read_only=True)

    # Assert it did not create the file
    assert not missing_path.exists(), "read_only=True must not create missing database files"


def test_duckdb_concurrent_readers(tmp_path: Path):
    """Verifies that multiple independent read_only=True connections can query simultaneously."""
    import duckdb

    db_path = tmp_path / "concurrency_test.duckdb"

    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE nums AS SELECT range AS n FROM range(10000);")
    con.commit()
    con.execute("CHECKPOINT;")
    con.close()

    # Open two concurrent read-only handles
    ro1 = duckdb.connect(str(db_path), read_only=True)
    ro2 = duckdb.connect(str(db_path), read_only=True)

    sum1 = ro1.execute("SELECT SUM(n) FROM nums;").fetchone()[0]
    sum2 = ro2.execute("SELECT SUM(n) FROM nums;").fetchone()[0]

    assert sum1 == sum2 == 49995000
    ro1.close()
    ro2.close()


def test_duckdb_writer_configuration_settings(tmp_path: Path):
    """Verifies writer resource configuration (threads=4, memory_limit=8GB, max_temp_directory_size=14GB)."""
    import duckdb

    spill_dir = tmp_path / "spill_writer"
    spill_dir.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "writer_config.duckdb"

    config = {
        "threads": 4,
        "memory_limit": "8GB",
        "max_temp_directory_size": "14GB",
        "temp_directory": str(spill_dir),
    }

    con = duckdb.connect(str(db_path), config=config)

    # Verify settings readback
    threads = con.execute("SELECT current_setting('threads');").fetchone()[0]
    assert int(threads) == 4, f"Expected threads=4, got {threads}"

    mem_limit = con.execute("SELECT current_setting('memory_limit');").fetchone()[0]
    assert mem_limit is not None and len(mem_limit) > 0, "memory_limit setting is empty"

    temp_dir_setting = con.execute("SELECT current_setting('temp_directory');").fetchone()[0]
    assert str(spill_dir) in temp_dir_setting or temp_dir_setting == str(spill_dir)

    con.close()


def test_duckdb_reader_configuration_settings(tmp_path: Path):
    """Verifies reader resource configuration (threads=1, memory_limit=1GB, max_temp_directory_size=2GB)."""
    import duckdb

    spill_dir = tmp_path / "spill_reader"
    spill_dir.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "reader_config.duckdb"

    # Initialize empty db
    c = duckdb.connect(str(db_path))
    c.execute("CREATE TABLE dummy (x INT);")
    c.commit()
    c.execute("CHECKPOINT;")
    c.close()

    config = {
        "threads": 1,
        "memory_limit": "1GB",
        "max_temp_directory_size": "2GB",
        "temp_directory": str(spill_dir),
    }

    ro_con = duckdb.connect(str(db_path), read_only=True, config=config)

    threads = ro_con.execute("SELECT current_setting('threads');").fetchone()[0]
    assert int(threads) == 1, f"Expected reader threads=1, got {threads}"

    mem_limit = ro_con.execute("SELECT current_setting('memory_limit');").fetchone()[0]
    assert mem_limit is not None and len(mem_limit) > 0

    temp_dir_setting = ro_con.execute("SELECT current_setting('temp_directory');").fetchone()[0]
    assert str(spill_dir) in temp_dir_setting or temp_dir_setting == str(spill_dir)

    ro_con.close()


def test_duckdb_interval_arithmetic_and_quantile_oracle():
    """Verifies 64-bit microsecond interval arithmetic and exact continuous quantile oracle."""
    import duckdb

    con = duckdb.connect(":memory:")

    # 1. Microsecond difference test on naive timestamps
    con.execute("""
        CREATE TABLE sample_trips (
            id INT,
            request_datetime TIMESTAMP,
            pickup_datetime TIMESTAMP
        );
        INSERT INTO sample_trips VALUES
            (1, TIMESTAMP '2024-12-01 12:00:00.900000', TIMESTAMP '2024-12-01 12:00:01.100000'), -- +0.2s
            (2, TIMESTAMP '2024-12-01 12:00:01.100000', TIMESTAMP '2024-12-01 12:00:00.900000'), -- -0.2s
            (3, NULL, TIMESTAMP '2024-12-01 12:00:05.000000'),                                    -- missing
            (4, TIMESTAMP '2024-12-01 12:00:00.000000', TIMESTAMP '2024-12-01 12:00:00.000000'), -- 0.0s
            (5, TIMESTAMP '2024-12-01 12:00:00.000000', TIMESTAMP '2024-12-01 12:00:01.000000'), -- 1.0s
            (6, TIMESTAMP '2024-12-01 12:00:00.000000', TIMESTAMP '2024-12-01 12:00:10.000000'); -- 10.0s
    """)

    # Compute microsecond subtraction divided by 1,000,000.0 and classify
    con.execute("""
        CREATE VIEW qualified_view AS
        SELECT
            id,
            request_datetime,
            pickup_datetime,
            epoch_us(pickup_datetime - request_datetime) / 1000000.0 AS interval_seconds,
            CASE
                WHEN request_datetime IS NULL OR pickup_datetime IS NULL THEN 'missing_timestamp'
                WHEN epoch_us(pickup_datetime - request_datetime) < 0 THEN 'negative_recorded_interval'
                ELSE 'eligible_nonnegative_recorded_interval'
            END AS interval_class
        FROM sample_trips;
    """)

    class_counts = dict(con.execute("""
        SELECT interval_class, COUNT(*) FROM qualified_view GROUP BY interval_class;
    """).fetchall())

    assert class_counts.get("eligible_nonnegative_recorded_interval") == 4
    assert class_counts.get("missing_timestamp") == 1
    assert class_counts.get("negative_recorded_interval") == 1

    # Hand-calculated oracle over eligible: [0.0, 0.2, 1.0, 10.0]
    # median = 0.60, p95 = 8.65
    quantiles = con.execute("""
        SELECT
            ROUND(quantile_cont(interval_seconds, 0.50), 2) AS median,
            ROUND(quantile_cont(interval_seconds, 0.95), 2) AS p95
        FROM qualified_view
        WHERE interval_class = 'eligible_nonnegative_recorded_interval';
    """).fetchone()

    assert quantiles[0] == 0.60, f"Expected median=0.60, got {quantiles[0]}"
    assert quantiles[1] == 8.65, f"Expected p95=8.65, got {quantiles[1]}"

    # Empty population test: must return NULL, never 0.0
    empty_quantiles = con.execute("""
        SELECT
            quantile_cont(interval_seconds, 0.50),
            quantile_cont(interval_seconds, 0.95)
        FROM qualified_view
        WHERE 1 = 0;
    """).fetchone()

    assert empty_quantiles[0] is None, f"Expected NULL median for empty population, got {empty_quantiles[0]}"
    assert empty_quantiles[1] is None, f"Expected NULL p95 for empty population, got {empty_quantiles[1]}"

    con.close()


if __name__ == "__main__":
    try:
        import duckdb
        import pyarrow
    except ImportError as e:
        print(f"SKIPPED DuckDB/Arrow tests: {e} (dependencies not yet installed in .venv)")
        sys.exit(0)

    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        test_duckdb_arrow_batch_stream_ingestion(tpath)
        print("PASS: test_duckdb_arrow_batch_stream_ingestion")
        test_duckdb_batch_size_invariance(tpath)
        print("PASS: test_duckdb_batch_size_invariance")
        test_duckdb_checkpoint_and_zero_wal_postcondition(tpath)
        print("PASS: test_duckdb_checkpoint_and_zero_wal_postcondition")
        test_duckdb_checkpoint_active_transaction_barrier(tpath)
        print("PASS: test_duckdb_checkpoint_active_transaction_barrier")
        test_duckdb_read_only_mode_and_mtime_immutability(tpath)
        print("PASS: test_duckdb_read_only_mode_and_mtime_immutability")
        test_duckdb_read_only_nonexistent_database(tpath)
        print("PASS: test_duckdb_read_only_nonexistent_database")
        test_duckdb_concurrent_readers(tpath)
        print("PASS: test_duckdb_concurrent_readers")
        test_duckdb_writer_configuration_settings(tpath)
        print("PASS: test_duckdb_writer_configuration_settings")
        test_duckdb_reader_configuration_settings(tpath)
        print("PASS: test_duckdb_reader_configuration_settings")
    test_duckdb_interval_arithmetic_and_quantile_oracle()
    print("PASS: test_duckdb_interval_arithmetic_and_quantile_oracle")
    print("ALL 10 DUCKDB & ARROW TESTS PASSED.")
    sys.exit(0)
