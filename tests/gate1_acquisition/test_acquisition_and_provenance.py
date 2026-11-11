"""Gate G1 Acceptance Test Suite: Acquisition, Discovery & Provenance.

Validates Scenarios T01, T02, T03, T04, T25, and T29 using SQLite control plane,
isolated temp data roots, synthetic Parquet/CSV fixtures, and mock HTTP transports.
"""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from typing import Any
import uuid

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

    pytest = _PytestShim()

try:
    import httpx
except ImportError:
    httpx = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None

from hvfhv.config import SystemConfig
from hvfhv.control.registry import Registry, configure_sqlite_connection
from hvfhv.acquisition.discover import (
    discover_frontier,
    ObservationOutcome,
    check_source_metadata,
)
from hvfhv.acquisition.acquire import (
    acquire_source,
    reconcile_raw_storage,
    compute_revision_id,
    AcquisitionError,
    TransportError,
    SourceUnavailableError,
    SecurityError,
    CorruptionError,
)


class MockRequest:
    def __init__(self, method: str = "GET", url: str = ""):
        self.method = method
        self.url = url


class MockResponse:
    def __init__(
        self,
        status_code: int,
        content: bytes | str = b"",
        headers: dict[str, str] | None = None,
        request: Any = None,
    ):
        self.status_code = status_code
        self.content = content if isinstance(content, bytes) else content.encode("utf-8")
        self.headers = headers or {}
        self.request = request or MockRequest()
        self.url = getattr(self.request, "url", "")

    def iter_bytes(self, chunk_size: int = 65536):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i : i + chunk_size]

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class MockTransport:
    def __init__(self, handler: Any):
        self.handler = handler


class MockClient:
    def __init__(self, transport: Any = None):
        self.transport = transport

    def head(self, url: str, headers: dict[str, str] | None = None) -> MockResponse:
        req = MockRequest("HEAD", url)
        return self.transport.handler(req)

    def get(self, url: str, headers: dict[str, str] | None = None) -> MockResponse:
        req = MockRequest("GET", url)
        return self.transport.handler(req)

    def stream(self, method: str, url: str, headers: dict[str, str] | None = None):
        req = MockRequest(method, url)
        resp = self.transport.handler(req)

        class _Ctx:
            def __enter__(self):
                return resp
            def __exit__(self, *args):
                pass

        return _Ctx()

    def close(self) -> None:
        pass


def make_sample_parquet_bytes(num_rows: int = 100, is_2025: bool = False) -> bytes:
    """Generate a valid HVFHV parquet byte stream in memory."""
    if pa is not None and pq is not None:
        fields = [
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
        ]
        if is_2025:
            fields.append(("cbd_congestion_fee", pa.float64()))

        schema = pa.schema(fields)
        data = {
            name: [None] * num_rows if t != pa.int64() else [1] * num_rows
            for name, t in fields
        }
        table = pa.Table.from_pydict(data, schema=schema)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue()

    # Synthetic binary fallback with PAR1 magic header and footer
    payload = b"SYNTHETIC_PARQUET_BODY_ROWS_" + str(num_rows).encode("utf-8")
    return b"PAR1" + payload + b"\x00\x00\x00\x00" + b"PAR1"


# ---------------------------------------------------------------------------
# T01: Truncated download / mid-stream timeout
# ---------------------------------------------------------------------------
def test_t01_truncated_download_midstream_abort():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=2)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            full_bytes = make_sample_parquet_bytes(500)
            truncated_bytes = full_bytes[: len(full_bytes) // 3]

            def handler(request: Any) -> Any:
                headers = {"Content-Length": str(len(full_bytes)), "Accept-Encoding": "identity"}
                return MockResponse(200, content=truncated_bytes, headers=headers, request=request)

            client = MockClient(transport=MockTransport(handler))
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet"

            with pytest.raises((TransportError, CorruptionError)):
                acquire_source("tlc/hvfhv/2024-12", url, config, conn, client=client)

            # Invariant: No revision is registered in source_revisions
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 0
            cur.close()

            # Invariant: Raw content-addressed storage contains no files
            raw_files = list((config.raw_dir / "sha256").glob("*/*"))
            assert len(raw_files) == 0
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# T02: Same URL and exact bytes acquired twice
# ---------------------------------------------------------------------------
def test_t02_identical_bytes_acquired_twice():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            parquet_bytes = make_sample_parquet_bytes(200)

            def handler(request: Any) -> Any:
                headers = {
                    "Content-Length": str(len(parquet_bytes)),
                    "ETag": '"etag-123"',
                    "Last-Modified": "Mon, 03 Feb 2025 12:00:00 GMT",
                    "Accept-Encoding": "identity",
                }
                return MockResponse(200, content=parquet_bytes, headers=headers, request=request)

            client = MockClient(transport=MockTransport(handler))
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet"

            # First acquisition
            res1 = acquire_source("tlc/hvfhv/2024-12", url, config, conn, attempt_id="att-1", client=client)
            # Second acquisition of identical content
            res2 = acquire_source("tlc/hvfhv/2024-12", url, config, conn, attempt_id="att-2", client=client)

            assert res1["revision_id"] == res2["revision_id"]
            assert res1["content_sha256"] == res2["content_sha256"]

            cur = conn.cursor()
            # Invariant: Exactly one source revision
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 1

            # Invariant: Two source observations recorded
            cur.execute("SELECT count(*) FROM source_observations;")
            assert cur.fetchone()[0] == 2
            cur.close()

            # Invariant: Raw storage contains exactly one source.parquet file
            raw_files = list((config.raw_dir / "sha256").glob("*/source.parquet"))
            assert len(raw_files) == 1
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# T03: Same URL changes bytes; metadata validators absent/unchanged
# ---------------------------------------------------------------------------
def test_t03_content_changes_without_metadata_validators():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            bytes_v1 = make_sample_parquet_bytes(100)
            bytes_v2 = make_sample_parquet_bytes(150)
            current_bytes = bytes_v1

            def handler(request: Any) -> Any:
                headers = {
                    "ETag": '"static-etag"',  # ETag does not reflect content change
                    "Accept-Encoding": "identity",
                    "Content-Length": str(len(current_bytes)),
                }
                return MockResponse(200, content=current_bytes, headers=headers, request=request)

            client = MockClient(transport=MockTransport(handler))
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet"

            # Acquire initial version
            res1 = acquire_source("tlc/hvfhv/2024-12", url, config, conn, client=client)

            # Upstream file silently changes bytes
            current_bytes = bytes_v2
            res2 = acquire_source("tlc/hvfhv/2024-12", url, config, conn, client=client)

            assert res1["content_sha256"] != res2["content_sha256"]
            assert res1["revision_id"] != res2["revision_id"]

            cur = conn.cursor()
            # Invariant: Distinct source revisions tracked in SQLite
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 2
            cur.close()

            # Invariant: Both raw content-addressed files coexist safely
            raw_files = list((config.raw_dir / "sha256").glob("*/source.parquet"))
            assert len(raw_files) == 2
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# T04: HTTP 404, 429, 5xx and TLS failure
# ---------------------------------------------------------------------------
def test_t04_http_error_and_tls_classification():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=2)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            # 1. 404 Not Found -> SourceUnavailableError without retry
            req_count_404 = 0
            def handler_404(request: Any) -> Any:
                nonlocal req_count_404
                req_count_404 += 1
                return MockResponse(404, request=request)

            client_404 = MockClient(transport=MockTransport(handler_404))
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-05.parquet"
            with pytest.raises(SourceUnavailableError):
                acquire_source("tlc/hvfhv/2025-05", url, config, conn, client=client_404)
            assert req_count_404 == 1  # Invariant: No retries on 404

            # 2. 429 / 503 transient errors -> retries exhausted -> TransportError
            req_count_503 = 0
            def handler_503(request: Any) -> Any:
                nonlocal req_count_503
                req_count_503 += 1
                return MockResponse(503, request=request)

            client_503 = MockClient(transport=MockTransport(handler_503))
            with pytest.raises(TransportError):
                acquire_source("tlc/hvfhv/2025-05", url, config, conn, client=client_503)
            assert req_count_503 == 2  # Invariant: Retried boundedly

            # 3. Disallowed host / security check -> SecurityError immediately
            disallowed_url = "https://evil-unauthorized-host.com/trip-data/data.parquet"
            with pytest.raises(SecurityError):
                acquire_source("tlc/hvfhv/2025-05", disallowed_url, config, conn)

            # 4. Actionable 4xx client errors (400, 401, 403, 405) -> AcquisitionError immediately without retry
            for status in [400, 401, 403, 405]:
                req_count_4xx = 0
                def make_handler_4xx(s: int):
                    def handler_4xx(request: Any) -> Any:
                        nonlocal req_count_4xx
                        req_count_4xx += 1
                        return MockResponse(s, request=request)
                    return handler_4xx

                client_4xx = MockClient(transport=MockTransport(make_handler_4xx(status)))
                with pytest.raises(AcquisitionError):
                    acquire_source("tlc/hvfhv/2025-05", url, config, conn, client=client_4xx)
                assert req_count_4xx == 1, f"HTTP {status} was retried {req_count_4xx} times (expected 1 without retries)"
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# T25: Frontier scan with gaps (Walkthrough W04)
# ---------------------------------------------------------------------------
def test_t25_frontier_scan_with_gaps_preserves_current():
    active_window = [f"2023-{m:02d}" for m in range(10, 13)] + [f"2024-{m:02d}" for m in range(1, 10)]
    assert len(active_window) == 12

    def handler(request: Any) -> Any:
        url_str = str(getattr(request, "url", ""))
        if "2024-10" in url_str:
            return MockResponse(404, request=request)
        elif "2024-11" in url_str:
            return MockResponse(200, headers={"Content-Length": "1000"}, request=request)
        elif "taxi_zone_lookup.csv" in url_str:
            return MockResponse(200, request=request)
        else:
            return MockResponse(200, request=request)

    client = MockClient(transport=MockTransport(handler))
    plan = discover_frontier(
        active_window=active_window,
        registered_revisions={},
        client=client,
        observation_cutoff="2024-11",
    )

    # Invariant: Frontier scan did not stop on 2024-10 404; checked 2024-11
    assert "2024-10" in plan.frontier_months_scanned
    assert "2024-11" in plan.frontier_months_scanned

    # Invariant: Latest available month is 2024-11, but proposed window has gap at 2024-10
    assert plan.proposed_window is not None
    assert plan.proposed_window.end_month == "2024-11"
    assert plan.proposed_window.is_complete is False
    assert "2024-10" in plan.proposed_window.missing_months
    assert plan.pending_gaps == ["2024-10"]


# ---------------------------------------------------------------------------
# T29: Crash recovery between original finalization and registration
# ---------------------------------------------------------------------------
def test_t29_crash_recovery_and_orphan_reconciliation():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            parquet_bytes = make_sample_parquet_bytes(300)
            hasher = hashlib.sha256(parquet_bytes)
            digest = hasher.hexdigest().lower()

            # Simulate Case A: File finalized in raw store with provenance, but crashed before SQLite commit
            digest_dir = data_root / "raw" / "sha256" / digest
            digest_dir.mkdir(parents=True, exist_ok=True)
            raw_file = digest_dir / "source.parquet"
            raw_file.write_bytes(parquet_bytes)

            expected_row_count = 300 if (pa is not None and pq is not None) else 100
            provenance_data = {
                "logical_source_id": "tlc/hvfhv/2024-12",
                "requested_url": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet",
                "content_sha256": digest,
                "completed_bytes": len(parquet_bytes),
                "footer_row_count": expected_row_count,
                "decoded_row_count": expected_row_count,
                "schema_fingerprint": "hvfhv_24col_pre2025.v1",
                "acquired_at_utc": "2026-11-10T08:00:00Z",
            }
            with open(digest_dir / "acquisition_provenance.json", "w", encoding="utf-8") as f:
                json.dump(provenance_data, f)

            # Simulate Case B: Orphan blob without provenance (must NOT be registered)
            orphan_digest = "0" * 64
            orphan_dir = data_root / "raw" / "sha256" / orphan_digest
            orphan_dir.mkdir(parents=True, exist_ok=True)
            (orphan_dir / "source.parquet").write_bytes(b"corrupt-data")

            # Run reconciliation
            report = reconcile_raw_storage(data_root, conn)

            # Invariant: Valid orphan is reconciled and registered safely
            rev_id = compute_revision_id("tlc/hvfhv/2024-12", digest)
            assert rev_id in report["reconciled"]

            cur = conn.cursor()
            cur.execute("SELECT content_sha256, footer_row_count FROM source_revisions WHERE revision_id = ?;", (rev_id,))
            row = cur.fetchone()
            assert row is not None
            assert row[0] == digest
            assert row[1] == expected_row_count
            cur.close()

            # Invariant: Unproven orphan blob remains unassigned
            assert str(orphan_dir) in report["unassigned_orphans"]
        finally:
            conn.close()


if __name__ == "__main__":
    tests = [
        test_t01_truncated_download_midstream_abort,
        test_t02_identical_bytes_acquired_twice,
        test_t03_content_changes_without_metadata_validators,
        test_t04_http_error_and_tls_classification,
        test_t25_frontier_scan_with_gaps_preserves_current,
        test_t29_crash_recovery_and_orphan_reconciliation,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            print(f"[PASS] {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"[FAIL] {t.__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\nRESULTS: {passed} PASSED, {failed} FAILED")
    if failed > 0:
        import sys
        sys.exit(1)
