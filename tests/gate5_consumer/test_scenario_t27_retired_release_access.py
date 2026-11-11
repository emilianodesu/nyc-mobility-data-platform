"""Gate G5 Acceptance: Scenario T27 - Retired Release Access Attempt.

Verifies:
1. When a release is RETIRED under retention policy and DuckDB payload unlinked:
   - Consumer access raises RetiredReleaseError immediately.
   - Out-of-cache check rejects query before executing against disk.
   - Manifest (manifest.json) and validation receipt (validation.json) remain strictly preserved.
2. Even if an entry was previously cached in memory, out-of-cache verification blocks serving.
"""

from __future__ import annotations

import json
import tempfile
import uuid
from pathlib import Path

from hvfhv.consumer.reader import ReleaseReader, RetiredReleaseError
from tests.gate5_consumer.test_scenario_t21_readonly_dashboard_queries import setup_test_published_release


def test_t27_retired_release_access_raises_error_and_preserves_manifests():
    """Scenario T27: Accessing a retired release raises RetiredReleaseError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        # 1. Warm reader cache while release is active
        reader = ReleaseReader(data_root=root, release_id=rel_id)
        res_before = reader.query_recorded_intervals(source_month="2024-12")
        assert res_before["denominators"]["total_population_count"] == 100

        # 2. Retire the release in SQLite control plane and unlink analytics.duckdb
        con = registry.get_connection()
        try:
            con.execute("UPDATE releases SET status = 'RETIRED' WHERE release_id = ?", (rel_id,))
            con.commit()
        finally:
            con.close()

        duck_file = rel_dir / "analytics.duckdb"
        assert duck_file.exists()
        duck_file.unlink()

        # 3. Manifests must be strictly preserved
        assert (rel_dir / "manifest.json").exists()
        assert (rel_dir / "validation.json").exists()

        # 4. Out-of-cache check must fail even though result was previously cached!
        try:
            reader.query_recorded_intervals(source_month="2024-12")
            assert False, "Should have raised RetiredReleaseError even with cached query!"
        except RetiredReleaseError as e:
            assert "RETIRED under retention policy" in str(e)

        # 5. Direct check_eligibility raises RetiredReleaseError
        try:
            reader.check_eligibility()
            assert False, "Should have raised RetiredReleaseError"
        except RetiredReleaseError as e:
            assert "RETIRED" in str(e)

        # 6. Direct get_reliability_summary raises RetiredReleaseError
        try:
            reader.get_reliability_summary()
            assert False, "Should have raised RetiredReleaseError"
        except RetiredReleaseError as e:
            assert "RETIRED" in str(e)
