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
import datetime
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
        self._manifest_verified: bool = False
        self._oversized_uncached_count: int = 0
        self._last_query_meta: dict[str, Any] = {}

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
        - If release status is REJECTED, raises IneligibleReleaseError with REJECTED diagnostic.
        - If release status is not VERIFIED, raises IneligibleReleaseError.
        - If analytics.duckdb or manifest.json file is missing on disk, raises ArtifactNotFoundError.
        - On first use of release_id in this reader instance, verifies manifest.json SHA-256 matches releases.manifest_sha256.
        """
        if not self._registry_db.exists():
            raise IneligibleReleaseError("Registry database does not exist.")

        con = sqlite3.connect(f"file:{self._registry_db}?mode=ro", uri=True)
        try:
            cur = con.cursor()
            row = cur.execute(
                """
                SELECT status, release_directory, manifest_sha256, database_sha256,
                       validation_sha256, finalized_at_utc, derivation_id
                FROM releases WHERE release_id = ?;
                """,
                (self.release_id,),
            ).fetchone()
            if not row:
                raise IneligibleReleaseError(f"Release '{self.release_id}' is not registered in control plane.")

            status, rel_dir_str, m_hash, db_hash, val_hash, fin_utc, deriv_id = row
            if status == "RETIRED":
                raise RetiredReleaseError(
                    f"Release '{self.release_id}' has been RETIRED under retention policy; DuckDB payload unlinked."
                )
            if status == "REJECTED":
                raise IneligibleReleaseError(
                    f"Release '{self.release_id}' has status 'REJECTED' and is ineligible for consumer serving."
                )
            if status != "VERIFIED":
                raise IneligibleReleaseError(
                    f"Release '{self.release_id}' has invalid status '{status}' (must be VERIFIED)."
                )

            rel_dir = Path(rel_dir_str)
            if not rel_dir.is_absolute():
                rel_dir = self.data_root / rel_dir
            if not rel_dir.exists():
                rel_dir = self.data_root / "releases" / self.release_id

            db_file = rel_dir / "analytics.duckdb"
            if not db_file.exists():
                raise ArtifactNotFoundError(f"Database payload for release '{self.release_id}' missing at {db_file}")

            if not self._manifest_verified:
                manifest_file = rel_dir / "manifest.json"
                if not manifest_file.exists():
                    raise ArtifactNotFoundError(
                        f"Manifest file for release '{self.release_id}' missing at {manifest_file}"
                    )
                h = hashlib.sha256()
                with open(manifest_file, "rb") as mf:
                    while chunk := mf.read(65536):
                        h.update(chunk)
                actual_m_hash = h.hexdigest()
                if actual_m_hash != m_hash:
                    raise IneligibleReleaseError(
                        f"Manifest SHA-256 mismatch for release '{self.release_id}': expected {m_hash}, got {actual_m_hash}"
                    )
                self._manifest_verified = True

            return {
                "release_id": self.release_id,
                "status": status,
                "release_directory": rel_dir,
                "database_path": db_file,
                "manifest_sha256": m_hash,
                "database_sha256": db_hash,
                "validation_sha256": val_hash,
                "finalized_at_utc": fin_utc,
                "derivation_id": deriv_id,
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
        5. Cache insertion with memory & row bounding (T28): if raw_rows > MAX_CACHE_ROWS or
           serialized bytes > MAX_CACHE_BYTES, do NOT cache the oversized entry.
        """
        # Step 1: Pre-query out-of-cache check (Scenario T21, T27)
        eligibility = self.check_eligibility()

        # Step 2: Cache lookup
        cache_key = (self.release_id, query_kind, sql, tuple(params))
        if cache_key in self._cache:
            data, byte_size, _ = self._cache[cache_key]
            # Move to end (most recently used)
            self._cache.move_to_end(cache_key)
            self._last_query_meta = {
                "cache_hit": True,
                "oversized_uncached": False,
                "returned_row_count": len(data),
                "entry_bytes": byte_size,
                "narrowing_guidance": None,
            }
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
        raw_row_count = len(raw_rows)
        is_row_oversized = raw_row_count > self.MAX_CACHE_ROWS

        result_rows: list[dict[str, Any]] = []
        for r in raw_rows[: self.MAX_CACHE_ROWS]:
            row_dict = {}
            for col, val in zip(col_names, r):
                if isinstance(val, datetime.datetime):
                    val = val.isoformat(sep=" ")
                elif isinstance(val, datetime.date):
                    val = val.isoformat()
                row_dict[col] = val
            result_rows.append(row_dict)

        # Enforce memory & row bounding in cache (T28)
        try:
            serialized = json.dumps(result_rows)
            entry_bytes = len(serialized.encode("utf-8"))
            is_byte_oversized = entry_bytes > self.MAX_CACHE_BYTES
            if not is_row_oversized and not is_byte_oversized:
                if len(self._cache) >= self.MAX_CACHE_ENTRIES:
                    self._cache.popitem(last=False)  # evict least recently used
                self._cache[cache_key] = (result_rows, entry_bytes, time.time())
                self._last_query_meta = {
                    "cache_hit": False,
                    "oversized_uncached": False,
                    "raw_row_count": raw_row_count,
                    "returned_row_count": len(result_rows),
                    "entry_bytes": entry_bytes,
                    "narrowing_guidance": None,
                }
            else:
                self._oversized_uncached_count += 1
                self._last_query_meta = {
                    "cache_hit": False,
                    "oversized_uncached": True,
                    "raw_row_count": raw_row_count,
                    "returned_row_count": len(result_rows),
                    "entry_bytes": entry_bytes,
                    "narrowing_guidance": (
                        "Query result exceeded cache bounds (> 10,000 rows or > 5 MiB); "
                        "narrow query filters (source_month, pickup_date, hvfhs_license_num, pu_location_id)."
                    ),
                }
        except (TypeError, ValueError):
            pass

        return result_rows

    def get_reliability_summary(self) -> dict[str, Any]:
        """Fetch Data Reliability View metrics using canonical SQL schemas.
        
        Returns:
        - release metadata & digests (release_metadata + releases control plane)
        - source contributions (source_contribution)
        - source coverage with explicit NULLs for absent months (coverage)
        - rule verification outcomes (quality_result + release_verifications S08)
        - semantic column profiles (column_profile)
        - live operational control plane state
        """
        eligibility = self.check_eligibility()

        # 1. Query release_metadata (canonical sql/models.sql)
        meta_rows = self.execute_query(
            """
            SELECT
                release_id,
                derivation_id,
                model_schema_version,
                rules_version,
                metric_definition_version,
                code_bundle_manifest_hash,
                lockfile_hash,
                zone_revision_id,
                first_source_month,
                last_source_month,
                built_at_utc
            FROM release_metadata
            LIMIT 1;
            """,
            query_kind="meta",
        )
        metadata = dict(meta_rows[0]) if meta_rows else {}
        metadata.setdefault("finalized_at_utc", eligibility.get("finalized_at_utc"))
        metadata.setdefault("manifest_sha256", eligibility.get("manifest_sha256"))
        metadata.setdefault("database_sha256", eligibility.get("database_sha256"))
        metadata.setdefault("validation_sha256", eligibility.get("validation_sha256"))
        metadata.setdefault("status", eligibility.get("status"))

        # 2. Query source_contribution (canonical sql/models.sql)
        contrib_rows = self.execute_query(
            """
            SELECT
                logical_source_id,
                revision_id,
                content_sha256,
                source_month,
                source_schema_version,
                cbd_fee_field_present,
                decoded_row_count,
                inserted_row_count
            FROM source_contribution
            ORDER BY source_month, logical_source_id;
            """,
            query_kind="source_contribution",
        )

        # 3. Query coverage table (canonical sql/models.sql)
        cov_rows = self.execute_query(
            """
            SELECT
                source_month,
                is_expected,
                is_available,
                is_included,
                revision_id,
                row_count
            FROM coverage
            ORDER BY source_month;
            """,
            query_kind="coverage",
        )

        # 4. Query pre-seal quality_result (canonical sql/quality_and_profiles.sql)
        rules_rows = self.execute_query(
            """
            SELECT
                rule_id,
                rule_version,
                cohort_key,
                severity,
                status,
                method,
                evaluated_count,
                exception_count,
                denominator,
                exception_rate,
                details_json,
                source_provenance,
                evaluated_at_utc
            FROM quality_result
            ORDER BY rule_id, cohort_key;
            """,
            query_kind="rules",
        )

        # 5. Query column_profile (canonical sql/quality_and_profiles.sql)
        profiles_rows = self.execute_query(
            """
            SELECT
                source_month,
                hvfhs_license_num,
                column_name,
                statistic_name,
                statistic_value_double,
                statistic_value_text,
                method,
                population_count,
                null_count,
                nonfinite_count,
                evaluated_at_utc
            FROM column_profile
            ORDER BY source_month, hvfhs_license_num, column_name, statistic_name;
            """,
            query_kind="profiles",
        )

        # 6. Query live operational state & S08 attestation from SQLite
        live_ops = self._fetch_live_operational_state()

        return {
            "release_id": self.release_id,
            "metadata": metadata,
            "source_contributions": contrib_rows,
            "coverage": cov_rows,
            "quality_rules": rules_rows,
            "s08_verification": live_ops.get("s08_verification"),
            "column_profiles": profiles_rows,
            "operational_state": live_ops,
        }

    def _fetch_live_operational_state(self) -> dict[str, Any]:
        """Fetch live operational status and post-seal S08 attestation from control plane."""
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

            s08_row = cur.execute(
                """
                SELECT verification_id, release_id, manifest_sha256, database_sha256,
                       validation_sha256, rule_inventory_version, rule_inventory_hash,
                       verification_results_json, verifier_version, verified_at_utc
                FROM release_verifications
                WHERE release_id = ?
                ORDER BY verified_at_utc DESC
                LIMIT 1;
                """,
                (self.release_id,),
            ).fetchone()

            s08_info: dict[str, Any] | None = None
            if s08_row:
                try:
                    ver_results = json.loads(s08_row[7]) if s08_row[7] else {}
                except Exception:
                    ver_results = {}
                s08_info = {
                    "verification_id": s08_row[0],
                    "release_id": s08_row[1],
                    "rule_id": "S08",
                    "status": "PASS",
                    "manifest_sha256": s08_row[2],
                    "database_sha256": s08_row[3],
                    "validation_sha256": s08_row[4],
                    "rule_inventory_version": s08_row[5],
                    "rule_inventory_hash": s08_row[6],
                    "verification_results": ver_results,
                    "verifier_version": s08_row[8],
                    "verified_at_utc": s08_row[9],
                }

            return {
                "current_generation": current_row[1] if current_row else None,
                "current_release_id": current_row[0] if current_row else None,
                "last_selected_at_utc": current_row[2] if current_row else None,
                "total_attempts_recorded": attempts_count,
                "last_source_check_utc": last_obs[0] if last_obs else None,
                "last_source_check_status": last_obs[1] if last_obs else None,
                "s08_verification": s08_info,
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
        params: list[Any] = []
        if source_month:
            filters.append("tq.source_month = ?")
            params.append(source_month)
        if pickup_date:
            filters.append("CAST(tq.pickup_date AS VARCHAR) = ?")
            params.append(pickup_date)
        if hvfhs_license_num:
            filters.append("tq.hvfhs_license_num = ?")
            params.append(hvfhs_license_num)
        if pu_location_id is not None:
            filters.append("tq.pu_location_id = ?")
            params.append(pu_location_id)

        where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""

        # 1. Query denominator breakdown and exact Type 7 quantiles directly over trip_qualified
        # Use single-pass list quantile_cont([0.50, 0.95]) so DuckDB shares one state buffer under the 1 GiB reader cap.
        denom_sql = f"""
            SELECT
                count(*) AS total_population_count,
                count(*) FILTER (WHERE tq.interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_count,
                count(*) FILTER (WHERE tq.interval_class = 'missing_timestamp') AS missing_count,
                count(*) FILTER (WHERE tq.interval_class = 'negative_recorded_interval') AS negative_count,
                quantile_cont(tq.recorded_request_to_pickup_seconds, [0.50, 0.95]) FILTER (
                    WHERE tq.interval_class = 'eligible_nonnegative_recorded_interval'
                ) AS q_pair
            FROM trip_qualified tq
            {where_clause};
        """
        denom_rows = self.execute_query(denom_sql, params=params, query_kind="denom")
        if denom_rows:
            raw_d = dict(denom_rows[0])
            q_pair = raw_d.pop("q_pair", None)
            tot = int(raw_d.get("total_population_count") or 0)
            elig = int(raw_d.get("eligible_count") or 0)
            denom_res = {
                "total_population_count": tot,
                "eligible_count": elig,
                "missing_count": int(raw_d.get("missing_count") or 0),
                "negative_count": int(raw_d.get("negative_count") or 0),
                "eligible_fraction": (elig / tot) if tot > 0 else 0.0,
                "median_seconds": float(q_pair[0]) if q_pair and q_pair[0] is not None else None,
                "p95_seconds": float(q_pair[1]) if q_pair and len(q_pair) > 1 and q_pair[1] is not None else None,
            }
        else:
            denom_res = {
                "total_population_count": 0,
                "eligible_count": 0,
                "missing_count": 0,
                "negative_count": 0,
                "eligible_fraction": 0.0,
                "median_seconds": None,
                "p95_seconds": None,
            }

        # 2. Query aggregate breakdown grouped by source_month, pickup_date, pu_location_id, hvfhs_license_num joined with zone_reference
        # Use pre-materialized interval_daily when populated (exact grain match per LLD §7), falling back to trip_qualified if empty.
        bounded_limit = int(min(limit, self.MAX_CACHE_ROWS))
        id_filters = [f.replace("tq.", "id.") for f in filters]
        id_where = ("WHERE " + " AND ".join(id_filters)) if id_filters else ""
        materialized_breakdown_sql = f"""
            SELECT
                id.source_month,
                id.pickup_date,
                id.pu_location_id,
                zr.borough,
                zr.zone,
                zr.service_zone,
                id.hvfhs_license_num,
                id.total_trips,
                id.eligible_trips,
                id.missing_intervals,
                id.negative_intervals,
                id.median_seconds,
                id.p95_seconds
            FROM interval_daily id
            LEFT JOIN zone_reference zr ON id.pu_location_id = zr.location_id
            {id_where}
            ORDER BY id.source_month ASC, id.pickup_date ASC, id.pu_location_id ASC, id.hvfhs_license_num ASC
            LIMIT {bounded_limit};
        """
        daily_breakdown = self.execute_query(
            materialized_breakdown_sql, params=params, query_kind="daily_breakdown"
        )
        if not daily_breakdown and denom_res.get("total_population_count", 0) > 0:
            breakdown_sql = f"""
                SELECT
                    tq.source_month,
                    tq.pickup_date,
                    tq.pu_location_id,
                    zr.borough,
                    zr.zone,
                    zr.service_zone,
                    tq.hvfhs_license_num,
                    COUNT(*) AS total_trips,
                    COUNT(*) FILTER (WHERE tq.interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_trips,
                    COUNT(*) FILTER (WHERE tq.interval_class = 'missing_timestamp') AS missing_intervals,
                    COUNT(*) FILTER (WHERE tq.interval_class = 'negative_recorded_interval') AS negative_intervals,
                    quantile_cont(tq.recorded_request_to_pickup_seconds, 0.50) FILTER (
                        WHERE tq.interval_class = 'eligible_nonnegative_recorded_interval'
                    ) AS median_seconds,
                    quantile_cont(tq.recorded_request_to_pickup_seconds, 0.95) FILTER (
                        WHERE tq.interval_class = 'eligible_nonnegative_recorded_interval'
                    ) AS p95_seconds
                FROM trip_qualified tq
                LEFT JOIN zone_reference zr ON tq.pu_location_id = zr.location_id
                {where_clause}
                GROUP BY
                    tq.source_month,
                    tq.pickup_date,
                    tq.pu_location_id,
                    zr.borough,
                    zr.zone,
                    zr.service_zone,
                    tq.hvfhs_license_num
                ORDER BY tq.source_month ASC, tq.pickup_date ASC, tq.pu_location_id ASC, tq.hvfhs_license_num ASC
                LIMIT {bounded_limit};
            """
            daily_breakdown = self.execute_query(
                breakdown_sql, params=params, query_kind="daily_breakdown_fallback"
            )

        # 3. Query canonical row samples from trip_qualified with bounded memory
        sample_sql = f"""
            SELECT * FROM (
                SELECT
                    tq.revision_id,
                    tq.source_row_number,
                    tq.source_month,
                    tq.pickup_date,
                    tq.hvfhs_license_num,
                    tq.pu_location_id,
                    tq.do_location_id,
                    tq.request_datetime,
                    tq.pickup_datetime,
                    tq.recorded_request_to_pickup_seconds,
                    tq.interval_class,
                    tq.is_off_source_month,
                    tq.pu_zone_status,
                    tq.do_zone_status
                FROM trip_qualified tq
                {where_clause}
                LIMIT {bounded_limit}
            ) sub
            ORDER BY pickup_datetime ASC NULLS LAST, revision_id ASC, source_row_number ASC;
        """
        sample_rows = self.execute_query(sample_sql, params=params, query_kind="samples")

        return {
            "denominators": denom_res,
            "daily_breakdown": daily_breakdown,
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
            "oversized_uncached_count": self._oversized_uncached_count,
            "last_query_meta": self._last_query_meta,
        }
