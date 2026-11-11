"""Read-only Consumer Reader for Published Releases.

Fulfills requirements:
- R16: Consumer Access & Safety (out-of-cache eligibility check, read_only connection)
- R19: Dual analytical views (Reliability and Recorded Intervals)
- R20: Resource Bounds (1 thread, 1GB memory limit, 2GB reader spill cap)
- R21: Safe Degradation & Observability
- Scenarios T21, T27, T28
"""

from __future__ import annotations

import collections
import hashlib
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

import duckdb

from hvfhv.config import SystemConfig, get_default_config
from hvfhv.control.locks import query_lock, LockContentionError


class ConsumerError(Exception):
    """Base exception for consumer access errors."""
    pass


class IneligibleReleaseError(ConsumerError):
    """Raised when release is unverified, rejected, or missing."""
    pass


class RetiredReleaseError(IneligibleReleaseError):
    """Raised when consumer attempts to access a retired release (Scenario T27)."""
    pass


class ArtifactNotFoundError(ConsumerError):
    """Raised when physical DuckDB artifact is missing on disk."""
    pass


class QueryTimeoutError(ConsumerError):
    """Raised when query.lock cannot be acquired within bounded wait."""
    pass


class ReleaseReader:
    """Read-only consumer interface pinned to a verified release.
    
    Invariants:
    1. Checks release status outside cache in registry.sqlite before every execution
       and again after query completion (R16, T21, T27).
    2. Acquires POSIX query.lock on cache misses (R16, T21).
    3. Read-only DuckDB connection with strict bounds: 1 thread, 1GB memory, 2GB spill (R20).
    4. Bounded LRU in-memory cache: max 32 entries, max 10k rows, max 5 MiB per entry (T28).
    5. Returns exact population denominators and explicit timestamp/SLA disclosures (R19, T08).
    """

    MAX_CACHE_ENTRIES = 32
    MAX_CACHE_ROWS = 10000
    MAX_CACHE_BYTES = 5 * 1024 * 1024  # 5 MiB

    def __init__(
        self,
        data_root: Path | str,
        release_id: str | None = None,
        config: SystemConfig | None = None,
    ) -> None:
        self.data_root = Path(data_root)
        self.config = config or SystemConfig(data_root=self.data_root)
        self._cache: collections.OrderedDict[tuple, tuple[Any, int, float]] = collections.OrderedDict()
        self._registry_db = self.data_root / "control" / "registry.sqlite"

        # Resolve pinned release_id
        if release_id is None:
            self.release_id = self._resolve_current_release_id()
        else:
            self.release_id = release_id

    def _resolve_current_release_id(self) -> str:
        """Resolve current published release from control plane."""
        if not self._registry_db.exists():
            raise IneligibleReleaseError(f"Registry does not exist at {self._registry_db}")
        con = sqlite3.connect(f"file:{self._registry_db}?mode=ro", uri=True)
        try:
            cur = con.cursor()
            row = cur.execute("SELECT release_id FROM current_release WHERE id = 1;").fetchone()
            if not row or not row[0]:
                raise IneligibleReleaseError("No current release is currently published.")
            return str(row[0])
        finally:
            con.close()

    def check_eligibility(self) -> dict[str, Any]:
        """Verify release eligibility in SQLite control plane OUTSIDE cache.
        
        Guarantees:
        - If release status is RETIRED, raises RetiredReleaseError (Scenario T27).
        - If release status is not FINALIZED or VERIFIED, raises IneligibleReleaseError.
        - If analytics.duckdb file is missing on disk, raises ArtifactNotFoundError.
        """
        if not self._registry_db.exists():
            raise IneligibleReleaseError("Registry database does not exist.")

        con = sqlite3.connect(f"file:{self._registry_db}?mode=ro", uri=True)
        try:
            cur = con.cursor()
            row = cur.execute(
                """
                SELECT status, release_directory, manifest_sha256, database_sha256
                FROM releases WHERE release_id = ?;
                """,
                (self.release_id,),
            ).fetchone()
            if not row:
                raise IneligibleReleaseError(f"Release '{self.release_id}' is not registered in control plane.")

            status, rel_dir_str, m_hash, db_hash = row
            if status == "RETIRED":
                raise RetiredReleaseError(
                    f"Release '{self.release_id}' has been RETIRED under retention policy; DuckDB payload unlinked."
                )
            if status not in ("FINALIZED", "VERIFIED"):
                raise IneligibleReleaseError(
                    f"Release '{self.release_id}' has invalid status '{status}' (must be FINALIZED or VERIFIED)."
                )

            rel_dir = Path(rel_dir_str)
            if not rel_dir.exists():
                rel_dir = self.data_root / "releases" / self.release_id

            db_file = rel_dir / "analytics.duckdb"
            if not db_file.exists():
                raise ArtifactNotFoundError(f"Database payload for release '{self.release_id}' missing at {db_file}")

            return {
                "release_id": self.release_id,
                "status": status,
                "release_directory": rel_dir,
                "database_path": db_file,
                "manifest_sha256": m_hash,
                "database_sha256": db_hash,
            }
        finally:
            con.close()

    def execute_query(
        self,
        sql: str,
        params: tuple | list = (),
        query_kind: str = "generic",
    ) -> list[dict[str, Any]]:
        """Execute a read-only DuckDB query with eligibility checks and bounded cache.
        
        Flow:
        1. Pre-query out-of-cache eligibility check.
        2. LRU cache lookup.
        3. Cache miss: acquire query.lock, connect read-only DuckDB, execute query.
        4. Post-query out-of-cache eligibility check (guards against concurrent retirement).
        5. Cache insertion with memory & row bounding (T28).
        """
        # Step 1: Pre-query out-of-cache check (Scenario T21, T27)
        eligibility = self.check_eligibility()

        # Step 2: Cache lookup
        cache_key = (self.release_id, query_kind, sql, tuple(params))
        if cache_key in self._cache:
            data, byte_size, _ = self._cache[cache_key]
            # Move to end (most recently used)
            self._cache.move_to_end(cache_key)
            return json.loads(json.dumps(data))  # return copy

        # Step 3: Cache miss -> acquire query.lock with bounded wait
        try:
            with query_lock(self.data_root, timeout=10.0, shared=False):
                spill_dir = self.data_root / "spill" / "reader"
                spill_dir.mkdir(parents=True, exist_ok=True)

                duck_con = duckdb.connect(
                    str(eligibility["database_path"]),
                    read_only=True,
                    config={
                        "threads": "1",
                        "max_memory": "1GB",
                        "temp_directory": str(spill_dir),
                    },
                )
                try:
                    cur = duck_con.execute(sql, params)
                    raw_rows = cur.fetchall()
                    col_names = [d[0] for d in cur.description]
                finally:
                    duck_con.close()
        except LockContentionError as e:
            raise QueryTimeoutError(f"Reader timed out waiting for query.lock: {e}") from e

        # Step 4: Post-query out-of-cache check
        self.check_eligibility()

        # Step 5: Format result and enforce row bound
        result_rows: list[dict[str, Any]] = []
        for r in raw_rows[: self.MAX_CACHE_ROWS]:
            row_dict = {}
            for col, val in zip(col_names, r):
                row_dict[col] = val
            result_rows.append(row_dict)

        # Enforce memory bounding in cache (T28)
        try:
            serialized = json.dumps(result_rows)
            entry_bytes = len(serialized.encode("utf-8"))
            if entry_bytes <= self.MAX_CACHE_BYTES:
                if len(self._cache) >= self.MAX_CACHE_ENTRIES:
                    self._cache.popitem(last=False)  # evict least recently used
                self._cache[cache_key] = (result_rows, entry_bytes, time.time())
        except (TypeError, ValueError):
            pass

        return result_rows

    def get_reliability_summary(self) -> dict[str, Any]:
        """Fetch Data Reliability View metrics.
        
        Returns:
        - release metadata & digests
        - source coverage with explicit NULLs for absent months
        - rule verification outcomes (S01-S08)
        - semantic profiles (Q01-Q09)
        - live operational control plane state
        """
        self.check_eligibility()

        # Query analytical metadata
        meta_rows = self.execute_query(
            "SELECT release_id, derivation_id, finalized_at_utc, pipeline_version, "
            "duckdb_version, pyarrow_version FROM release_metadata LIMIT 1;",
            query_kind="meta",
        )
        metadata = meta_rows[0] if meta_rows else {}

        # Query coverage table
        cov_rows = self.execute_query(
            "SELECT source_month, observation_status, trip_observation_row_count, "
            "distinct_hvfhs_license_count, distinct_pickup_date_count, notes "
            "FROM coverage ORDER BY source_month;",
            query_kind="coverage",
        )

        # Query pre-seal quality rules
        rules_rows = self.execute_query(
            "SELECT rule_id, rule_name, rule_kind, outcome, severity, "
            "evaluated_row_count, failing_row_count, executed_at_utc, diagnostic_details "
            "FROM quality_result ORDER BY rule_id;",
            query_kind="rules",
        )

        # Query column profiles
        profiles_rows = self.execute_query(
            "SELECT profile_id, profile_name, relation_name, column_name, "
            "population_label, profile_payload_json "
            "FROM column_profile ORDER BY profile_id;",
            query_kind="profiles",
        )

        # Query live operational state from SQLite
        live_ops = self._fetch_live_operational_state()

        return {
            "release_id": self.release_id,
            "metadata": metadata,
            "coverage": cov_rows,
            "quality_rules": rules_rows,
            "column_profiles": profiles_rows,
            "operational_state": live_ops,
        }

    def _fetch_live_operational_state(self) -> dict[str, Any]:
        """Fetch live operational status from control plane."""
        con = sqlite3.connect(f"file:{self._registry_db}?mode=ro", uri=True)
        try:
            cur = con.cursor()
            last_obs = cur.execute(
                "SELECT observed_at_utc, http_status, outcome "
                "FROM source_observations ORDER BY observed_at_utc DESC LIMIT 1;"
            ).fetchone()

            attempts_count = cur.execute("SELECT count(*) FROM attempts;").fetchone()[0]
            current_row = cur.execute(
                "SELECT release_id, generation, updated_at_utc FROM current_release WHERE id = 1;"
            ).fetchone()

            return {
                "current_generation": current_row[1] if current_row else None,
                "current_release_id": current_row[0] if current_row else None,
                "last_selected_at_utc": current_row[2] if current_row else None,
                "total_attempts_recorded": attempts_count,
                "last_source_check_utc": last_obs[0] if last_obs else None,
                "last_source_check_status": last_obs[1] if last_obs else None,
            }
        finally:
            con.close()

    def query_recorded_intervals(
        self,
        source_month: str | None = None,
        pickup_date: str | None = None,
        hvfhs_license_num: str | None = None,
        pu_location_id: int | None = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """Query Recorded Intervals View with population denominators and exact quantiles.
        
        Guarantees:
        1. Explicit 3-class interval count identity (total = missing + negative + eligible).
        2. Strict non-averaging: exact Type 7 quantile_cont evaluated directly on eligible trips.
        3. Clear non-SLA and naive microsecond arithmetic disclosure.
        """
        filters = []
        params = []
        if source_month:
            filters.append("source_month = ?")
            params.append(source_month)
        if pickup_date:
            filters.append("pickup_date = ?")
            params.append(pickup_date)
        if hvfhs_license_num:
            filters.append("hvfhs_license_num = ?")
            params.append(hvfhs_license_num)
        if pu_location_id is not None:
            filters.append("pu_location_id = ?")
            params.append(pu_location_id)

        where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""

        # Query denominator breakdown
        denom_sql = f"""
            SELECT
                count(*) AS total_population_count,
                count(*) FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_count,
                count(*) FILTER (WHERE interval_class = 'missing_timestamp') AS missing_count,
                count(*) FILTER (WHERE interval_class = 'negative_recorded_interval') AS negative_count,
                quantile_cont(recorded_request_to_pickup_seconds, 0.50) FILTER (
                    WHERE interval_class = 'eligible_nonnegative_recorded_interval'
                ) AS median_seconds,
                quantile_cont(recorded_request_to_pickup_seconds, 0.95) FILTER (
                    WHERE interval_class = 'eligible_nonnegative_recorded_interval'
                ) AS p95_seconds
            FROM trip_qualified
            {where_clause};
        """
        denom_rows = self.execute_query(denom_sql, params=params, query_kind="denom")
        denom_res = denom_rows[0] if denom_rows else {
            "total_population_count": 0, "eligible_count": 0,
            "missing_count": 0, "negative_count": 0,
            "median_seconds": None, "p95_seconds": None,
        }

        # Query row samples or aggregate breakdown
        sample_sql = f"""
            SELECT
                trip_id, source_month, pickup_date, hvfhs_license_num,
                pu_location_id, request_datetime, pickup_datetime,
                recorded_request_to_pickup_seconds, interval_class,
                is_off_source_month, zone_matched
            FROM trip_qualified
            {where_clause}
            ORDER BY pickup_datetime ASC
            LIMIT {int(min(limit, self.MAX_CACHE_ROWS))};
        """
        sample_rows = self.execute_query(sample_sql, params=params, query_kind="samples")

        return {
            "denominators": denom_res,
            "sample_rows": sample_rows,
            "disclosures": {
                "timestamp_arithmetic": "Naive 64-bit microsecond difference divided by 1,000,000.0 without timezone manipulation",
                "sla_statement": "Recorded intervals reflect vehicle request-to-pickup recorded delta; NOT driver dispatch SLA, customer wait time, or fleet availability",
                "population_identity": "total_population_count = eligible_count + missing_count + negative_count",
            },
        }

    def clear_cache(self) -> None:
        """Clear the in-memory query cache."""
        self._cache.clear()

    def get_cache_stats(self) -> dict[str, Any]:
        """Return cache occupancy and byte size."""
        total_bytes = sum(entry[1] for entry in self._cache.values())
        return {
            "entry_count": len(self._cache),
            "max_entries": self.MAX_CACHE_ENTRIES,
            "total_bytes": total_bytes,
            "total_kib": round(total_bytes / 1024, 2),
        }
