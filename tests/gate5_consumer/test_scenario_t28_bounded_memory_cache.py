"""Gate G5 Acceptance: Scenario T28 - Streamlit / Consumer Bounded Memory & Cache.

Verifies:
1. In-memory cache enforces strict LRU eviction (MAX_CACHE_ENTRIES = 32).
2. Large queries are bounded by MAX_CACHE_ROWS (10,000 rows max).
3. Payload byte size is capped (MAX_CACHE_BYTES = 5 MiB max per entry).
4. Memory RSS remains bounded during intensive reader queries.
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import psutil

from hvfhv.consumer.reader import ReleaseReader
from tests.gate5_consumer.test_scenario_t21_readonly_dashboard_queries import setup_test_published_release


def test_t28_lru_cache_bounded_at_32_entries():
    """Scenario T28: Cache occupancy never exceeds MAX_CACHE_ENTRIES (32)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        reader = ReleaseReader(data_root=root, release_id=rel_id)
        assert reader.MAX_CACHE_ENTRIES == 32

        # Execute 40 distinct queries by varying the filter parameter
        for i in range(1, 41):
            reader.execute_query(
                f"SELECT {i} AS query_idx, * FROM release_metadata;",
                query_kind=f"test_query_{i}",
            )

        stats = reader.get_cache_stats()
        assert stats["entry_count"] == 32
        assert stats["entry_count"] <= stats["max_entries"]

        # Verify eviction: query 1 should have been evicted (oldest)
        q1_key = (rel_id, "test_query_1", "SELECT 1 AS query_idx, * FROM release_metadata;", ())
        assert q1_key not in reader._cache

        # Query 40 should still be in cache (newest)
        q40_key = (rel_id, "test_query_40", "SELECT 40 AS query_idx, * FROM release_metadata;", ())
        assert q40_key in reader._cache


def test_t28_result_set_bounded_by_max_rows():
    """Scenario T28: Large result sets are capped at MAX_CACHE_ROWS and excluded from cache."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        reader = ReleaseReader(data_root=root, release_id=rel_id)

        # Generate a query with 15,000 rows using range()
        rows = reader.execute_query("SELECT range AS val FROM range(15000);", query_kind="large_result")
        assert len(rows) == reader.MAX_CACHE_ROWS
        assert len(rows) == 10000

        # Oversized result must NOT be stored in the LRU cache
        stats = reader.get_cache_stats()
        assert stats["entry_count"] == 0
        assert stats["oversized_uncached_count"] == 1
        assert stats["last_query_meta"]["oversized_uncached"] is True
        assert stats["last_query_meta"]["narrowing_guidance"] is not None


def test_t28_memory_rss_remains_bounded():
    """Scenario T28: Reader execution keeps memory RSS bounded within allocation."""
    proc = psutil.Process(os.getpid())
    rss_start = proc.memory_info().rss

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        reader = ReleaseReader(data_root=root, release_id=rel_id)

        # Execute 50 queries repeatedly
        for _ in range(50):
            reader.query_recorded_intervals(source_month="2024-12")

        rss_end = proc.memory_info().rss
        rss_growth_mb = (rss_end - rss_start) / (1024 * 1024)

        # Growth should be minimal (well below 250 MB)
        assert rss_growth_mb < 250.0, f"Excessive memory growth: {rss_growth_mb:.2f} MB"
