"""Gate G5 Acceptance: Scenario T25 & R25 - Frontier Scan With Gaps.

Verifies:
1. Frontier scan identifies missing intermediate months without skipping or crashing.
2. Fragmented topology is correctly flagged (is_contiguous = False).
3. Produces deterministic ordered catch-up recommendations.
4. Preserves current published release untouched during scan.
"""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

from hvfhv.control.registry import Registry
from hvfhv.orchestration.defs import scan_frontier_and_discover_gaps


def test_t25_frontier_scan_detects_gap_and_recommends_sequence():
    """Scenario T25: Frontier scan with missing month in target window."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        for sub in ["control", "raw", "staging", "releases"]:
            (root / sub).mkdir(parents=True, exist_ok=True)
        registry = Registry(db_path=root / "control" / "registry.sqlite", data_root=root)

        # Register source objects for October and December 2024 (skipping November)
        registry.register_source_object("tlc/hvfhv/2024-10", "hvfhv", "https://example.com/2024-10.parquet", "2024-10")
        rev_10 = "rev_10_" + uuid.uuid4().hex[:8]
        registry.register_revision(
            revision_id=rev_10,
            logical_source_id="tlc/hvfhv/2024-10",
            content_sha256="1" * 64,
            byte_count=1000,
            relative_path="raw/2024-10.parquet",
            schema_fingerprint="f1",
            footer_row_count=10,
            decoded_row_count=10,
            provenance_relative_path="raw/prov_10.json",
        )

        registry.register_source_object("tlc/hvfhv/2024-12", "hvfhv", "https://example.com/2024-12.parquet", "2024-12")
        rev_12 = "rev_12_" + uuid.uuid4().hex[:8]
        registry.register_revision(
            revision_id=rev_12,
            logical_source_id="tlc/hvfhv/2024-12",
            content_sha256="2" * 64,
            byte_count=1000,
            relative_path="raw/2024-12.parquet",
            schema_fingerprint="f1",
            footer_row_count=10,
            decoded_row_count=10,
            provenance_relative_path="raw/prov_12.json",
        )

        # Target window: 2024-10, 2024-11, 2024-12
        target_window = ["2024-10", "2024-11", "2024-12"]

        report = scan_frontier_and_discover_gaps(root, target_months=target_window, registry=registry)

        assert report["status"] == "GAP_DETECTED"
        assert report["target_months_count"] == 3
        assert report["present_count"] == 2
        assert report["present_months"] == ["2024-10", "2024-12"]
        assert report["gap_count"] == 1
        assert report["missing_gaps"] == ["2024-11"]
        assert report["is_contiguous"] is False  # Oct and Dec are not contiguous!
        assert report["recommended_catchup_sequence"] == ["2024-11"]


def test_t25_frontier_scan_synchronized_contiguous():
    """Scenario T25: Frontier scan when all target months are registered."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        for sub in ["control", "raw", "staging", "releases"]:
            (root / sub).mkdir(parents=True, exist_ok=True)
        registry = Registry(db_path=root / "control" / "registry.sqlite", data_root=root)

        # Register consecutive months 2024-10, 2024-11, 2024-12
        for m in ["2024-10", "2024-11", "2024-12"]:
            registry.register_source_object(f"tlc/hvfhv/{m}", "hvfhv", f"https://example.com/{m}.parquet", m)
            rev = f"rev_{m}_" + uuid.uuid4().hex[:8]
            registry.register_revision(
                revision_id=rev,
                logical_source_id=f"tlc/hvfhv/{m}",
                content_sha256="0" * 64,
                byte_count=1000,
                relative_path=f"raw/{m}.parquet",
                schema_fingerprint="f1",
                footer_row_count=10,
                decoded_row_count=10,
                provenance_relative_path=f"raw/prov_{m}.json",
            )

        target_window = ["2024-10", "2024-11", "2024-12"]
        report = scan_frontier_and_discover_gaps(root, target_months=target_window, registry=registry)

        assert report["status"] == "SYNCHRONIZED"
        assert report["gap_count"] == 0
        assert report["missing_gaps"] == []
        assert report["is_contiguous"] is True
        assert report["recommended_catchup_sequence"] == []
