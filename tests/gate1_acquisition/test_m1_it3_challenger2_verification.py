"""Empirical Verification Suite for Gate G1 Iteration 3 (Challenger 2).

Tests:
1. HTTP 400, 401, 403, 422 immediate failure without retry
2. HTTP 404 does not halt frontier discovery
3. Bounded streaming memory usage under large Parquet payload simulation (tracemalloc)
4. Scenario T29 crash recovery:
   - Part A: Crash between finalization and registration (pristine orphan with provenance is reconciled)
   - Part B: Tampered/unproven orphans remain unassigned
   - Part C: Interrupted acquisition (.part file in staging) behavior and committed content preservation
5. Provenance JSON records exact SHA-256 and byte count matching disk and SQLite
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import tracemalloc
from typing import Any

from hvfhv.config import SystemConfig
from hvfhv.contracts.identity import compute_revision_id
from hvfhv.control.registry import Registry
from hvfhv.acquisition.acquire import (
    acquire_source,
    reconcile_raw_storage,
    AcquisitionError,
    SourceUnavailableError,
    TransportError,
    CorruptionError,
)
from hvfhv.acquisition.discover import (
    discover_frontier,
    ObservationOutcome,
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


def make_valid_synthetic_parquet(num_records: int = 100) -> bytes:
    """Return synthetic valid Parquet byte structure."""
    payload = b"HVFHV_DATA_ROW_" * num_records
    return b"PAR1" + payload + b"\x00\x00\x00\x00" + b"PAR1"


# ==============================================================================
# TEST 1: HTTP 400, 401, 403, 422 Immediate Failure Without Retry
# ==============================================================================
def test_http_4xx_immediate_fail_no_retry():
    target_codes = [400, 401, 403, 422]
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"

    for status in target_codes:
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

                assert raised_exc is not None, f"HTTP {status} did not raise an exception"
                assert type(raised_exc) is AcquisitionError, (
                    f"HTTP {status} raised {type(raised_exc).__name__} instead of AcquisitionError"
                )
                assert f"HTTP {status} client error" in str(raised_exc)
                assert calls == 1, f"HTTP {status} made {calls} attempts instead of 1 (retried!)"

                # Check SQLite and storage clean
                cur = conn.cursor()
                cur.execute("SELECT count(*) FROM source_revisions;")
                assert cur.fetchone()[0] == 0, f"HTTP {status} created source_revision"
                cur.close()

                raw_files = list((config.raw_dir / "sha256").glob("*/*"))
                assert len(raw_files) == 0, f"HTTP {status} leaked raw files"

            finally:
                conn.close()

    print("    [PASS] test_http_4xx_immediate_fail_no_retry (400, 401, 403, 422 verified)")


# ==============================================================================
# TEST 2: HTTP 404 Does Not Halt Frontier Discovery
# ==============================================================================
def test_http_404_does_not_halt_discovery():
    """Verify that HTTP 404 on intermediate or frontier months does not abort scanning."""
    active_window = ["2024-01", "2024-02"]

    # Month 2024-03 is 404, 2024-04 is 200, 2024-05 is 404, 2024-06 is 200
    statuses = {
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet": 200,
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-02.parquet": 200,
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-03.parquet": 404,
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-04.parquet": 200,
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-05.parquet": 404,
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-06.parquet": 200,
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv": 200,
    }

    scanned_urls = []
    def handler(req):
        scanned_urls.append(req.url)
        status = statuses.get(req.url, 404)
        return MockResponse(status, request=req)

    client = MockClient(transport=MockTransport(handler))

    plan = discover_frontier(
        active_window=active_window,
        registered_revisions={},
        client=client,
        observation_cutoff="2024-06",
    )

    # 1. Verify 2024-04 and 2024-06 were scanned despite 2024-03 being 404
    frontier_scanned = plan.frontier_months_scanned
    assert frontier_scanned == ["2024-03", "2024-04", "2024-05", "2024-06"], (
        f"Frontier scan halted prematurely: {frontier_scanned}"
    )

    # 2. Check observations for 404
    obs_map = {o.logical_source_id: o for o in plan.observations}
    assert obs_map["tlc/hvfhv/2024-03"].outcome == ObservationOutcome.UNAVAILABLE
    assert obs_map["tlc/hvfhv/2024-03"].http_status == 404
    assert obs_map["tlc/hvfhv/2024-04"].outcome == ObservationOutcome.AVAILABLE
    assert obs_map["tlc/hvfhv/2024-04"].http_status == 200
    assert obs_map["tlc/hvfhv/2024-05"].outcome == ObservationOutcome.UNAVAILABLE
    assert obs_map["tlc/hvfhv/2024-05"].http_status == 404
    assert obs_map["tlc/hvfhv/2024-06"].outcome == ObservationOutcome.AVAILABLE
    assert obs_map["tlc/hvfhv/2024-06"].http_status == 200

    # 3. Check proposals: only available months are proposed
    proposed_ls_ids = [p.logical_source_id for p in plan.proposals]
    assert "tlc/hvfhv/2024-03" not in proposed_ls_ids
    assert "tlc/hvfhv/2024-05" not in proposed_ls_ids
    assert "tlc/hvfhv/2024-04" in proposed_ls_ids
    assert "tlc/hvfhv/2024-06" in proposed_ls_ids

    # 4. Check proposed window captures pending gaps
    assert plan.proposed_window is not None
    assert plan.proposed_window.end_month == "2024-06"
    assert "2024-03" in plan.pending_gaps
    assert "2024-05" in plan.pending_gaps
    assert not plan.proposed_window.is_complete

    print("    [PASS] test_http_404_does_not_halt_discovery (interleaved 404 gaps verified)")


# ==============================================================================
# TEST 3: Bounded Streaming Memory Usage Under Large Payload Simulation
# ==============================================================================
def test_bounded_streaming_memory_large_payload():
    """Verify memory remains bounded (O(1) relative to payload) when streaming large Parquet."""
    payload_size = 50 * 1024 * 1024  # 50 MiB simulated stream
    chunk_size = 65536  # 64 KiB chunks
    num_chunks = payload_size // chunk_size

    def large_payload_generator():
        header = b"PAR1"
        yield header
        remaining = payload_size - 8
        chunk_data = b"X" * chunk_size
        while remaining > chunk_size:
            yield chunk_data
            remaining -= chunk_size
        if remaining > 0:
            yield b"X" * remaining
        footer = b"\x00\x00\x00\x00PAR1"
        yield footer

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            def handler(req):
                return MockResponse(
                    200,
                    headers={"Content-Length": str(payload_size + 4)},
                    request=req,
                    generator_override=large_payload_generator,
                )

            client = MockClient(transport=MockTransport(handler))

            tracemalloc.start()
            snap_before = tracemalloc.take_snapshot()

            prov = acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)

            snap_after = tracemalloc.take_snapshot()
            current_mem, peak_mem = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            # The entire payload is ~50 MiB.
            # Bounded streaming invariant: Peak memory during streaming MUST be << payload_size.
            # Typically peak memory overhead for streaming + hashing + disk write is < 5 MiB.
            assert peak_mem < 10 * 1024 * 1024, (
                f"Peak memory {peak_mem / (1024*1024):.2f} MiB exceeded bounded threshold for 50 MiB stream!"
            )
            assert prov["completed_bytes"] == payload_size + 4

            # Verify file on disk matches byte count
            raw_file = data_root / prov["relative_path"]
            assert raw_file.exists()
            assert raw_file.stat().st_size == payload_size + 4

        finally:
            conn.close()

    print(f"    [PASS] test_bounded_streaming_memory_large_payload (peak memory: {peak_mem / 1024:.1f} KiB for 50 MiB payload)")


# ==============================================================================
# TEST 4: Scenario T29 Crash Recovery and Raw Storage Reconciliation
# ==============================================================================
def test_scenario_t29_crash_recovery_and_part_handling():
    """Verify Scenario T29 invariants:
    1. Committed revisions in SQLite are preserved intact.
    2. Finalized raw files with verified provenance are reconciled into SQLite.
    3. Unverified or corrupt orphans in raw store are quarantined into unassigned_orphans.
    4. Crash during acquisition leaving .part file in staging does NOT corrupt raw store,
       and raw storage reconciliation preserves committed content while handling state.
    """
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            # Step A: Normal acquisition of an earlier month (committed content)
            committed_bytes = make_valid_synthetic_parquet(50)
            c_hash = hashlib.sha256(committed_bytes).hexdigest().lower()
            url_committed = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet"

            def handler_committed(req):
                return MockResponse(200, content=committed_bytes, request=req)

            client1 = MockClient(transport=MockTransport(handler_committed))
            rec1 = acquire_source("tlc/hvfhv/2024-01", url_committed, config, conn, client=client1)
            committed_rev_id = rec1["revision_id"]

            # Step B: Crash during acquisition of 2024-02:
            # Simulates process death while writing .part file in staging
            attempt_id = "crash-attempt-123"
            staging_crash_dir = config.staging_dir / attempt_id
            staging_crash_dir.mkdir(parents=True, exist_ok=True)
            part_file = staging_crash_dir / "tlc_hvfhv_2024-02.part"
            part_file.write_bytes(b"PARTIAL_INTERRUPTED_STREAM_DATA_CRASH")

            # Step C: Crash between raw finalization and SQLite registration (T29 textbook case):
            # 2024-03 raw file and acquisition_provenance.json finalized in raw/sha256/<digest>/,
            # but process killed before SQLite INSERT.
            p3_bytes = make_valid_synthetic_parquet(100)
            d3 = hashlib.sha256(p3_bytes).hexdigest().lower()
            p3_raw_dir = config.raw_dir / "sha256" / d3
            p3_raw_dir.mkdir(parents=True, exist_ok=True)
            (p3_raw_dir / "source.parquet").write_bytes(p3_bytes)
            prov3_data = {
                "logical_source_id": "tlc/hvfhv/2024-03",
                "requested_url": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-03.parquet",
                "final_url": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-03.parquet",
                "http_status": 200,
                "etag": '"etag-2024-03"',
                "last_modified": "Sun, 01 Mar 2024 00:00:00 GMT",
                "advertised_bytes": len(p3_bytes),
                "completed_bytes": len(p3_bytes),
                "content_sha256": d3,
                "footer_row_count": 100,
                "decoded_row_count": 100,
                "schema_fingerprint": "hvfhv_24col_pre2025.v1",
                "acquired_at_utc": "2026-11-10T15:00:00Z",
            }
            with open(p3_raw_dir / "acquisition_provenance.json", "w", encoding="utf-8") as f:
                json.dump(prov3_data, f)

            # Step D: Also place a corrupt unproven orphan in raw
            bad_dir = config.raw_dir / "sha256" / ("e" * 64)
            bad_dir.mkdir(parents=True, exist_ok=True)
            (bad_dir / "source.parquet").write_bytes(b"GARBAGE_NO_PROV")

            # Execute reconcile_raw_storage
            report = reconcile_raw_storage(data_root, conn)

            # Invariant 1: Finalized orphan 2024-03 is safely reconciled and registered
            expected_rev_3 = compute_revision_id("tlc/hvfhv/2024-03", d3)
            assert expected_rev_3 in report["reconciled"], f"Orphan revision {expected_rev_3} not reconciled"

            # Invariant 2: Corrupt unproven orphan is in unassigned_orphans
            assert str(bad_dir) in report["unassigned_orphans"], "Corrupt orphan must be unassigned"

            # Invariant 3: Committed revision 2024-01 remains intact
            cur = conn.cursor()
            cur.execute("SELECT content_sha256, byte_count FROM source_revisions WHERE revision_id = ?;", (committed_rev_id,))
            row = cur.fetchone()
            assert row is not None, "Committed revision 2024-01 was wiped out!"
            assert row[0] == c_hash
            assert row[1] == len(committed_bytes)

            # Total rows in source_revisions must be exactly 2 (committed 2024-01 + reconciled 2024-03)
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 2
            cur.close()

            # Invariant 4: Partial .part file from crash remains unaccepted (NOT registered in SQLite)
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions WHERE logical_source_id = 'tlc/hvfhv/2024-02';")
            assert cur.fetchone()[0] == 0, "Partial .part file was erroneously registered in SQLite!"
            cur.close()

        finally:
            conn.close()

    print("    [PASS] test_scenario_t29_crash_recovery_and_part_handling (Scenario T29 verified)")


# ==============================================================================
# TEST 5: Provenance JSON Records Exact SHA-256 and Byte Count
# ==============================================================================
def test_provenance_json_exact_sha256_and_bytes():
    """Verify that acquisition_provenance.json and source_revisions match exact SHA-256 and byte counts."""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"
    raw_payload = make_valid_synthetic_parquet(75)
    expected_sha256 = hashlib.sha256(raw_payload).hexdigest().lower()
    expected_bytes = len(raw_payload)

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            def handler(req):
                return MockResponse(
                    200,
                    content=raw_payload,
                    headers={
                        "Content-Length": str(expected_bytes),
                        "ETag": '"test-etag-12345"',
                        "Last-Modified": "Fri, 10 Jan 2025 12:00:00 GMT",
                    },
                    request=req,
                )

            client = MockClient(transport=MockTransport(handler))
            prov = acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)

            # 1. Check returned dictionary
            assert prov["content_sha256"] == expected_sha256
            assert prov["completed_bytes"] == expected_bytes
            assert prov["advertised_bytes"] == expected_bytes

            # 2. Check acquisition_provenance.json on disk
            prov_file = config.raw_dir / "sha256" / expected_sha256 / "acquisition_provenance.json"
            assert prov_file.exists(), f"Provenance JSON missing at {prov_file}"
            with open(prov_file, "r", encoding="utf-8") as f:
                disk_prov = json.load(f)

            assert disk_prov["content_sha256"] == expected_sha256
            assert disk_prov["completed_bytes"] == expected_bytes
            assert disk_prov["advertised_bytes"] == expected_bytes
            assert disk_prov["etag"] == '"test-etag-12345"'
            assert disk_prov["http_status"] == 200

            # 3. Check raw content on disk
            source_file = config.raw_dir / "sha256" / expected_sha256 / "source.parquet"
            assert source_file.exists()
            assert source_file.stat().st_size == expected_bytes
            assert hashlib.sha256(source_file.read_bytes()).hexdigest().lower() == expected_sha256

            # 4. Check SQLite control plane records
            cur = conn.cursor()
            cur.execute(
                """
                SELECT revision_id, content_sha256, byte_count, footer_row_count, decoded_row_count
                FROM source_revisions
                WHERE logical_source_id = 'tlc/hvfhv/2025-01';
                """
            )
            row = cur.fetchone()
            assert row is not None, "Revision record missing in SQLite"
            assert row[1] == expected_sha256
            assert row[2] == expected_bytes

            expected_rev_id = compute_revision_id("tlc/hvfhv/2025-01", expected_sha256)
            assert row[0] == expected_rev_id

            # Check source_observations
            cur.execute(
                """
                SELECT outcome, http_status, advertised_bytes, revision_id
                FROM source_observations
                WHERE logical_source_id = 'tlc/hvfhv/2025-01';
                """
            )
            obs_row = cur.fetchone()
            assert obs_row is not None
            assert obs_row[0] == "ACCEPTED"
            assert obs_row[1] == 200
            assert obs_row[2] == expected_bytes
            assert obs_row[3] == expected_rev_id

            cur.close()

        finally:
            conn.close()

    print("    [PASS] test_provenance_json_exact_sha256_and_bytes (disk, JSON, and SQLite verified)")


if __name__ == "__main__":
    print("=" * 80)
    print("GATE G1 ITERATION 3 EMPIRICAL CHALLENGER 2 VERIFICATION")
    print("=" * 80)
    test_http_4xx_immediate_fail_no_retry()
    test_http_404_does_not_halt_discovery()
    test_bounded_streaming_memory_large_payload()
    test_scenario_t29_crash_recovery_and_part_handling()
    test_provenance_json_exact_sha256_and_bytes()
    print("=" * 80)
    print("ALL 5 CHALLENGE TARGETS EMPIRICALLY VERIFIED: 5 PASSED, 0 FAILED")
    print("=" * 80)
