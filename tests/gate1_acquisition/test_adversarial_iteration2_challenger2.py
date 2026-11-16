"""Milestone M1 (Gate G1) Iteration 2 - Empirical Challenger 2 Stress Harness.

Rigorously stress-tests and verifies:
1. Full HTTP 4xx status code scan: 400, 401, 402, 403, 405, 406, 408, 409, 410, 413, 415, 422
   and extended 4xx codes (407, 411, 412, 414, 416, 417, 418, 421, 423, 424, 426, 428, 431, 451).
   Ensures ALL client errors raise AcquisitionError on attempt 1 with 0 retries.
2. 429 Too Many Requests: retried up to max_attempts, respects Retry-After, succeeds on recovery.
3. 500, 502, 503, 504 Server Errors: retried up to max_attempts, raises TransportError if persistent,
   succeeds on recovery.
4. Mixed status sequences: transient (5xx/429) transitioning to client error (4xx) halts immediately.
5. Mid-stream socket disconnects and TCP drops: retried boundedly, cleans partial .part staging,
   raises TransportError after max attempts, succeeds if transient.
6. Truncated Content-Length: detects byte mismatch and raises CorruptionError on attempt 1.
7. SSRF and Allowlist Enforcement: immediately raises SecurityError on unallowlisted hosts and redirects.
"""

from __future__ import annotations

import io
import os
from pathlib import Path
import sqlite3
import tempfile
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from hvfhv.config import SystemConfig
from hvfhv.control.registry import Registry
from hvfhv.acquisition.acquire import (
    acquire_source,
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
        generator_override: Any = None,
    ):
        self.status_code = status_code
        self.content = content if isinstance(content, bytes) else content.encode("utf-8")
        self.headers = headers or {}
        self.request = request or MockRequest()
        self.url = getattr(self.request, "url", "")
        self.generator_override = generator_override

    def iter_bytes(self, chunk_size: int = 65536):
        if self.generator_override is not None:
            yield from self.generator_override()
            return
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


def make_synthetic_parquet(num_rows: int = 100, is_2025: bool = False) -> bytes:
    """Return a genuine valid 24-column or 25-column PyArrow Parquet byte payload."""
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


# ==============================================================================
# TEST 1: Full 4xx Status Code Scan
# ==============================================================================

def test_adversarial_full_4xx_client_error_scan():
    """Verify all 4xx client errors (excluding 404 and 429) raise AcquisitionError on attempt 1 without retry."""
    # Target codes specifically called out in task:
    required_codes = [400, 401, 402, 403, 405, 406, 408, 409, 410, 413, 415, 422]
    # Extended 4xx client error codes according to RFC 9110:
    extended_codes = [407, 411, 412, 414, 416, 417, 418, 421, 423, 424, 425, 426, 428, 431, 451]
    all_client_codes = required_codes + extended_codes

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"

    for status in all_client_codes:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data_root = root / ".data"
            config = SystemConfig(data_root=data_root, http_max_retries=3)
            config.ensure_directories()
            reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
            conn = reg.get_connection()

            try:
                calls = 0
                def handler(req, s=status):
                    nonlocal calls
                    calls += 1
                    return MockResponse(s, request=req)

                client = MockClient(transport=MockTransport(handler))
                raised_exc = None
                try:
                    acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
                except Exception as exc:
                    raised_exc = exc

                # Assertion 1: Must raise an exception
                assert raised_exc is not None, f"HTTP {status} did not raise any exception!"

                # Assertion 2: Must be exactly AcquisitionError (not TransportError)
                assert isinstance(raised_exc, AcquisitionError), (
                    f"HTTP {status} raised {type(raised_exc).__name__} instead of AcquisitionError: {raised_exc}"
                )
                assert not isinstance(raised_exc, TransportError), (
                    f"HTTP {status} was misclassified as TransportError: {raised_exc}"
                )

                # Assertion 3: Exception message contains status code
                assert f"HTTP {status} client error" in str(raised_exc), (
                    f"HTTP {status} error message incorrect: {raised_exc}"
                )

                # Assertion 4: Must NOT retry (exactly 1 call made)
                assert calls == 1, (
                    f"HTTP {status} was retried {calls} times! Expected exactly 1 call (zero retries)."
                )

                # Assertion 5: Registry and raw storage must remain clean
                cur = conn.cursor()
                cur.execute("SELECT count(*) FROM source_revisions;")
                assert cur.fetchone()[0] == 0, f"HTTP {status} left orphaned rows in source_revisions"
                cur.close()

                raw_files = list((config.raw_dir / "sha256").glob("*/*"))
                assert len(raw_files) == 0, f"HTTP {status} leaked raw files: {raw_files}"

                staging_parts = list(config.staging_dir.glob("*.part"))
                assert len(staging_parts) == 0, f"HTTP {status} left dirty .part files: {staging_parts}"

            finally:
                conn.close()

    print(f"    [PASS] test_adversarial_full_4xx_client_error_scan (scanned {len(all_client_codes)} status codes, all 0 retries)")


# ==============================================================================
# TEST 2: HTTP 404 Not Found Handling
# ==============================================================================

def test_adversarial_404_source_unavailable():
    """Verify HTTP 404 raises SourceUnavailableError on attempt 1 without retries."""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=3)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            def handler(req):
                nonlocal calls
                calls += 1
                return MockResponse(404, request=req)

            client = MockClient(transport=MockTransport(handler))
            raised_exc = None
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
            except Exception as exc:
                raised_exc = exc

            assert isinstance(raised_exc, SourceUnavailableError), (
                f"Expected SourceUnavailableError, got {type(raised_exc).__name__}: {raised_exc}"
            )
            assert calls == 1, f"HTTP 404 was retried {calls} times, expected 1"
        finally:
            conn.close()

    print("    [PASS] test_adversarial_404_source_unavailable (immediate 404 observation)")


# ==============================================================================
# TEST 3: HTTP 429 Rate Limiting & Bounded Retries
# ==============================================================================

def test_adversarial_429_retry_bounded_and_recovery():
    """Verify HTTP 429 is retried up to max_attempts, respects Retry-After, and succeeds if transient."""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"
    parquet_bytes = make_synthetic_parquet()

    # Case A: Persistent 429 fails after max_attempts
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=3)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            def handler_persistent(req):
                nonlocal calls
                calls += 1
                return MockResponse(429, headers={"Retry-After": "0"}, request=req)

            client = MockClient(transport=MockTransport(handler_persistent))
            raised_exc = None
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
            except Exception as exc:
                raised_exc = exc

            assert isinstance(raised_exc, TransportError), (
                f"Expected TransportError on persistent 429, got {type(raised_exc).__name__}: {raised_exc}"
            )
            assert calls == 3, f"Expected 3 attempts for max_retries=3 on 429, got {calls}"

            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 0
            cur.close()
        finally:
            conn.close()

    # Case B: Transient 429 succeeds on attempt 2
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=3)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            def handler_transient(req):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return MockResponse(429, headers={"Retry-After": "0"}, request=req)
                return MockResponse(200, content=parquet_bytes, request=req)

            client = MockClient(transport=MockTransport(handler_transient))
            rec = acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)

            assert calls == 2, f"Expected exactly 2 attempts, got {calls}"
            assert rec["completed_bytes"] == len(parquet_bytes)

            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 1
            cur.close()
        finally:
            conn.close()

    print("    [PASS] test_adversarial_429_retry_bounded_and_recovery (persistent -> fail; transient -> succeed)")


# ==============================================================================
# TEST 4: Transient 5xx Server Errors (500, 502, 503, 504) Retries
# ==============================================================================

def test_adversarial_5xx_server_error_retries_and_recovery():
    """Verify 500, 502, 503, 504 are properly retried up to max_attempts and succeed upon recovery."""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"
    parquet_bytes = make_synthetic_parquet()

    test_statuses = [500, 502, 503, 504, 507, 508]

    for status in test_statuses:
        # Subcase 1: Persistent 5xx -> retried exactly max_attempts, raises TransportError
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data_root = root / ".data"
            config = SystemConfig(data_root=data_root, http_max_retries=3)
            config.ensure_directories()
            reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
            conn = reg.get_connection()

            try:
                calls = 0
                def handler_persistent(req, s=status):
                    nonlocal calls
                    calls += 1
                    return MockResponse(s, request=req)

                client = MockClient(transport=MockTransport(handler_persistent))
                raised_exc = None
                try:
                    acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
                except Exception as exc:
                    raised_exc = exc

                assert isinstance(raised_exc, TransportError), (
                    f"HTTP {status} expected TransportError, got {type(raised_exc).__name__}: {raised_exc}"
                )
                assert calls == 3, f"HTTP {status} expected 3 calls, got {calls}"

                cur = conn.cursor()
                cur.execute("SELECT count(*) FROM source_revisions;")
                assert cur.fetchone()[0] == 0
                cur.close()
            finally:
                conn.close()

        # Subcase 2: Transient 5xx recovers on attempt 3 -> SUCCEEDS
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data_root = root / ".data"
            config = SystemConfig(data_root=data_root, http_max_retries=3)
            config.ensure_directories()
            reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
            conn = reg.get_connection()

            try:
                calls = 0
                def handler_recover(req, s=status):
                    nonlocal calls
                    calls += 1
                    if calls < 3:
                        return MockResponse(s, request=req)
                    return MockResponse(200, content=parquet_bytes, request=req)

                client = MockClient(transport=MockTransport(handler_recover))
                rec = acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)

                assert calls == 3, f"Expected 3 calls on recovery, got {calls}"
                assert rec["completed_bytes"] == len(parquet_bytes)

                cur = conn.cursor()
                cur.execute("SELECT count(*) FROM source_revisions;")
                assert cur.fetchone()[0] == 1
                cur.close()
            finally:
                conn.close()

    print(f"    [PASS] test_adversarial_5xx_server_error_retries_and_recovery ({len(test_statuses)} 5xx codes verified)")


# ==============================================================================
# TEST 5: Mixed Transient & Actionable Sequence Oracle
# ==============================================================================

def test_adversarial_mixed_status_sequences():
    """Verify that transient errors followed by actionable client errors immediately abort without further retry."""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"
    parquet_bytes = make_synthetic_parquet()

    # Sequence A: 503 -> 502 -> 429 -> 200 OK (recovers across 4 attempts with max_retries=4)
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=4)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            seq = [503, 502, 429, 200]
            def handler_seq(req):
                nonlocal calls
                status = seq[calls]
                calls += 1
                if status == 200:
                    return MockResponse(200, content=parquet_bytes, request=req)
                return MockResponse(status, headers={"Retry-After": "0"} if status == 429 else {}, request=req)

            client = MockClient(transport=MockTransport(handler_seq))
            rec = acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
            assert calls == 4
            assert rec["completed_bytes"] == len(parquet_bytes)
        finally:
            conn.close()

    # Sequence B: Attempt 1 is 503 (transient), Attempt 2 is 400 Bad Request (actionable client error)
    # With max_retries=4, it must STOP after attempt 2 and NOT execute attempts 3 and 4!
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=4)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            def handler_abort_on_client_err(req):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return MockResponse(503, request=req)
                return MockResponse(400, request=req)

            client = MockClient(transport=MockTransport(handler_abort_on_client_err))
            raised_exc = None
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
            except Exception as exc:
                raised_exc = exc

            assert isinstance(raised_exc, AcquisitionError)
            assert not isinstance(raised_exc, TransportError)
            assert calls == 2, (
                f"Expected exactly 2 calls (attempt 1: 503, attempt 2: 400 abort), but got {calls} calls!"
            )
        finally:
            conn.close()

    # Sequence C: Attempt 1 is 429, Attempt 2 is 403 Forbidden
    # Must immediately abort after attempt 2 with AcquisitionError
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=4)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            def handler_abort_429_403(req):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return MockResponse(429, headers={"Retry-After": "0"}, request=req)
                return MockResponse(403, request=req)

            client = MockClient(transport=MockTransport(handler_abort_429_403))
            raised_exc = None
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
            except Exception as exc:
                raised_exc = exc

            assert isinstance(raised_exc, AcquisitionError)
            assert not isinstance(raised_exc, TransportError)
            assert calls == 2, (
                f"Expected exactly 2 calls (attempt 1: 429, attempt 2: 403 abort), but got {calls} calls!"
            )
        finally:
            conn.close()

    print("    [PASS] test_adversarial_mixed_status_sequences (multi-status chains & immediate 4xx abort)")


# ==============================================================================
# TEST 6: Mid-Stream Disconnects, Socket Drops, and Timeouts
# ==============================================================================

def test_adversarial_midstream_socket_disconnects_and_drops():
    """Verify mid-stream TCP socket reset and generator drops are boundedly retried and clean up staging."""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"
    parquet_bytes = make_synthetic_parquet()

    # Case A: Persistent socket drop during chunk streaming
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=3)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            def dropping_generator():
                yield parquet_bytes[:10]
                raise ConnectionResetError("TCP connection reset by peer mid-stream")

            def handler(req):
                nonlocal calls
                calls += 1
                return MockResponse(
                    200,
                    headers={"Content-Length": str(len(parquet_bytes))},
                    request=req,
                    generator_override=dropping_generator,
                )

            client = MockClient(transport=MockTransport(handler))
            raised_exc = None
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
            except Exception as exc:
                raised_exc = exc

            assert isinstance(raised_exc, TransportError), (
                f"Expected TransportError, got {type(raised_exc).__name__}: {raised_exc}"
            )
            assert "Acquisition failed after 3 attempts" in str(raised_exc)
            assert calls == 3, f"Expected 3 attempts, got {calls}"

            # Verify no orphaned .part files in staging
            part_files = list(config.staging_dir.glob("*.part"))
            assert len(part_files) == 0, f"Leaked part files in staging: {part_files}"

            # Verify zero SQLite entries
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 0
            cur.close()
        finally:
            conn.close()

    # Case B: Transient socket drop on attempt 1, clean delivery on attempt 2 -> SUCCEEDS
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=3)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            def handler_transient_drop(req):
                nonlocal calls
                calls += 1
                if calls == 1:
                    def drop():
                        yield parquet_bytes[:10]
                        raise OSError("Network socket dropped abruptly")
                    return MockResponse(200, request=req, generator_override=drop)
                return MockResponse(200, content=parquet_bytes, request=req)

            client = MockClient(transport=MockTransport(handler_transient_drop))
            rec = acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)

            assert calls == 2, f"Expected 2 attempts, got {calls}"
            assert rec["completed_bytes"] == len(parquet_bytes)

            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 1
            cur.close()
        finally:
            conn.close()

    # Case C: Client timeout during connection establishment
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=3)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            def handler_timeout(req):
                nonlocal calls
                calls += 1
                raise TimeoutError("HTTP connection timed out")

            client = MockClient(transport=MockTransport(handler_timeout))
            raised_exc = None
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
            except Exception as exc:
                raised_exc = exc

            assert isinstance(raised_exc, TransportError)
            assert calls == 3
        finally:
            conn.close()

    print("    [PASS] test_adversarial_midstream_socket_disconnects_and_drops (TCP reset, socket drop, timeout)")


# ==============================================================================
# TEST 7: Truncated Content-Length Mismatch
# ==============================================================================

def test_adversarial_truncated_content_length():
    """Verify that when server delivers fewer bytes than Content-Length, CorruptionError is raised immediately."""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"
    raw_payload = b"PAR1" + b"X" * 1000 + b"PAR1"

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=3)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            calls = 0
            def handler_mismatch(req):
                nonlocal calls
                calls += 1
                # Claim 50,000 bytes, but deliver only len(raw_payload)
                headers = {"Content-Length": "50000", "Accept-Encoding": "identity"}
                return MockResponse(200, content=raw_payload, headers=headers, request=req)

            client = MockClient(transport=MockTransport(handler_mismatch))
            raised_exc = None
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
            except Exception as exc:
                raised_exc = exc

            assert isinstance(raised_exc, CorruptionError), (
                f"Expected CorruptionError on Content-Length mismatch, got {type(raised_exc).__name__}: {raised_exc}"
            )
            assert "Truncated download" in str(raised_exc)
            # Corruption should raise immediately without blind retry
            assert calls == 1, f"Expected 1 call on payload corruption, got {calls}"

            # Ensure zero entries in SQLite and raw storage
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 0
            cur.close()

            raw_files = list((config.raw_dir / "sha256").glob("*/*"))
            assert len(raw_files) == 0
        finally:
            conn.close()

    print("    [PASS] test_adversarial_truncated_content_length (Content-Length mismatch rejected)")


# ==============================================================================
# TEST 8: SSRF & Host Allowlist Validation
# ==============================================================================

def test_adversarial_ssrf_and_host_allowlist_attacks():
    """Verify SSRF, localhost, metadata IP, non-HTTPS protocols, and malicious redirects are blocked immediately."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=3)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            # 1. Disallowed hosts
            bad_urls = [
                "https://169.254.169.254/latest/meta-data/",
                "https://127.0.0.1:8080/data.parquet",
                "https://localhost/data.parquet",
                "https://evil-attacker.com/trip-data.parquet",
                "http://d37ci6vzurychx.cloudfront.net/trip-data.parquet",  # plain HTTP
                "ftp://d37ci6vzurychx.cloudfront.net/trip-data.parquet",
            ]

            for bad_url in bad_urls:
                calls = 0
                def handler(req):
                    nonlocal calls
                    calls += 1
                    return MockResponse(200, request=req)

                client = MockClient(transport=MockTransport(handler))
                raised_exc = None
                try:
                    acquire_source("tlc/hvfhv/2025-01", bad_url, config, conn, client=client)
                except Exception as exc:
                    raised_exc = exc

                assert isinstance(raised_exc, SecurityError), (
                    f"URL {bad_url} expected SecurityError, got {type(raised_exc).__name__}: {raised_exc}"
                )
                assert calls == 0, f"URL {bad_url} made network calls! Must be rejected before network I/O"

            # 2. Malicious redirect from allowed host to attacker host
            allowed_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/redirect.parquet"
            calls_redir = 0
            def handler_redirect(req):
                nonlocal calls_redir
                calls_redir += 1
                return MockResponse(
                    302,
                    headers={"Location": "https://malicious-exfiltration.org/sink"},
                    request=req,
                )

            client_redir = MockClient(transport=MockTransport(handler_redirect))
            raised_redir_exc = None
            try:
                acquire_source("tlc/hvfhv/2025-01", allowed_url, config, conn, client=client_redir)
            except Exception as exc:
                raised_redir_exc = exc

            assert isinstance(raised_redir_exc, SecurityError)
            assert "Disallowed host" in str(raised_redir_exc)
            assert calls_redir == 1, f"Redirect attack expected 1 initial call, got {calls_redir}"

        finally:
            conn.close()

    print("    [PASS] test_adversarial_ssrf_and_host_allowlist_attacks (SSRF & redirects rejected)")


# ==============================================================================
# Suite Runner
# ==============================================================================

def run_all_iteration2_challenger2_tests():
    print("=" * 80)
    print("GATE G1 ITERATION 2 EMPIRICAL CHALLENGER 2 ADVERSARIAL STRESS SUITE")
    print("=" * 80)

    start_time = time.time()
    tests = [
        test_adversarial_full_4xx_client_error_scan,
        test_adversarial_404_source_unavailable,
        test_adversarial_429_retry_bounded_and_recovery,
        test_adversarial_5xx_server_error_retries_and_recovery,
        test_adversarial_mixed_status_sequences,
        test_adversarial_midstream_socket_disconnects_and_drops,
        test_adversarial_truncated_content_length,
        test_adversarial_ssrf_and_host_allowlist_attacks,
    ]

    passed = 0
    failed = 0

    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as exc:
            failed += 1
            print(f"    [FAIL] {test_fn.__name__}: {exc}")
            import traceback
            traceback.print_exc()

    elapsed = time.time() - start_time
    print("=" * 80)
    print(f"ITERATION 2 CHALLENGER 2 SUMMARY: {passed} PASSED, {failed} FAILED in {elapsed:.2f}s")
    print("=" * 80)

    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    run_all_iteration2_challenger2_tests()
