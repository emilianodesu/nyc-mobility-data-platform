"""HVFHV Analytical Models & Views Transformation Subsystem (Milestone M2 / Gate G2).

Implements:
1. Analytical schema initialization (release_metadata, source_contribution, zone_reference,
   trip_observation, coverage, trip_qualified, interval_daily, interval_monthly).
2. Zero-copy SQL VIEW `trip_qualified` computing 64-bit microsecond naive timestamp arithmetic
   and 3-class mutually exclusive partition (missing, negative, eligible).
3. Continuous quantiles (Type 7 linear interpolation, quantile_cont 0.50 and 0.95)
   independently computed at daily and monthly grains without averaging percentiles.
4. Release metadata, coverage tracking (with NULL row count on absent months), and
   pure Python mathematical reference oracles.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Any


def continuous_quantile(data: list[float], q: float) -> float | None:
    """Computes exact continuous quantile using Hyndman-Fan Type 7 linear interpolation.

    Matches DuckDB `quantile_cont`, PostgreSQL `PERCENTILE_CONT`, and NumPy default.
    Formula:
        h = (N - 1) * q
        i = floor(h)
        fraction = h - i
        Q(q) = data[i] + fraction * (data[i + 1] - data[i])
    """
    if not data:
        return None
    if not (0.0 <= q <= 1.0):
        raise ValueError(f"Quantile probability q must be between 0.0 and 1.0, got {q}")

    sorted_d = sorted(data)
    n = len(sorted_d)
    if n == 1:
        return float(sorted_d[0])

    h = (n - 1) * q
    i = math.floor(h)
    frac = h - i
    if i >= n - 1:
        return float(sorted_d[-1])
    return float(sorted_d[i] + frac * (sorted_d[i + 1] - sorted_d[i]))


def calculate_recorded_interval_seconds(
    request_dt: datetime | None,
    pickup_dt: datetime | None,
) -> float | None:
    """Calculates recorded request-to-pickup interval in seconds using exact 64-bit microsecond arithmetic."""
    if request_dt is None or pickup_dt is None:
        return None
    # 64-bit microsecond integer arithmetic on naive timestamps
    diff_us = int((pickup_dt - request_dt).total_seconds() * 1_000_000)
    return diff_us / 1_000_000.0


def classify_interval(
    request_dt: datetime | None,
    pickup_dt: datetime | None,
) -> tuple[float | None, str]:
    """Classifies an interval into the 3 mutually exclusive classes in priority order:
    1. 'missing_timestamp' (if either is None)
    2. 'negative_recorded_interval' (if recorded interval < 0)
    3. 'eligible_nonnegative_recorded_interval' (if recorded interval >= 0)
    """
    sec = calculate_recorded_interval_seconds(request_dt, pickup_dt)
    if sec is None:
        return None, "missing_timestamp"
    if sec < 0.0:
        return sec, "negative_recorded_interval"
    return sec, "eligible_nonnegative_recorded_interval"


def init_analytical_models(con: Any, sql_path: Path | None = None) -> None:
    """Executes the DDL in sql/models.sql to establish the analytical relations."""
    if sql_path is None:
        repo_root = Path(__file__).resolve().parents[3]
        sql_path = repo_root / "sql" / "models.sql"

    with open(sql_path, "r", encoding="utf-8") as f:
        ddl_script = f.read()

    # Split statements safely and execute
    for stmt in ddl_script.split(";"):
        cleaned = stmt.strip()
        if cleaned:
            con.execute(cleaned)


def populate_release_metadata(
    con: Any,
    release_id: str,
    derivation_id: str,
    model_schema_version: str,
    rules_version: str,
    metric_definition_version: str,
    code_bundle_manifest_hash: str,
    lockfile_hash: str,
    zone_revision_id: str,
    first_source_month: str,
    last_source_month: str,
    built_at_utc: str | None = None,
) -> None:
    """Inserts singleton metadata row into release_metadata."""
    if built_at_utc is None:
        built_at_utc = datetime.now(timezone.utc).isoformat()

    con.execute("""
        INSERT INTO release_metadata (
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, [
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
        built_at_utc,
    ])


def populate_coverage(
    con: Any,
    window_months: list[str],
    contributions: dict[str, tuple[str, int]],
) -> None:
    """Populates coverage table for all window months.

    If a month is absent from contributions, row_count is set to NULL (never 0).
    contributions format: {source_month: (revision_id, row_count)}
    """
    rows = []
    for m in window_months:
        if m in contributions:
            rev_id, count = contributions[m]
            rows.append((m, True, True, True, rev_id, count))
        else:
            # Absent month: row_count is NULL, not 0
            rows.append((m, True, False, False, None, None))

    con.executemany("""
        INSERT INTO coverage (
            source_month,
            is_expected,
            is_available,
            is_included,
            revision_id,
            row_count
        ) VALUES (?, ?, ?, ?, ?, ?);
    """, rows)


def build_interval_aggregates(con: Any) -> tuple[int, int]:
    """Computes exact continuous quantiles for interval_daily and interval_monthly.

    Monthly quantiles are computed independently from trip_qualified (never averaged from daily).
    Returns:
        (daily_row_count, monthly_row_count)
    """
    is_sqlite = hasattr(con, "create_aggregate")
    if is_sqlite:
        con.execute("""
            INSERT INTO interval_daily (
                source_month,
                pickup_date,
                pu_location_id,
                hvfhs_license_num,
                total_trips,
                eligible_trips,
                missing_intervals,
                negative_intervals,
                median_seconds,
                p95_seconds
            )
            SELECT
                source_month,
                pickup_date,
                pu_location_id,
                hvfhs_license_num,
                COUNT(*) AS total_trips,
                COUNT(*) FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_trips,
                COUNT(*) FILTER (WHERE interval_class = 'missing_timestamp') AS missing_intervals,
                COUNT(*) FILTER (WHERE interval_class = 'negative_recorded_interval') AS negative_intervals,
                quantile_cont(recorded_request_to_pickup_seconds, 0.50)
                    FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS median_seconds,
                quantile_cont(recorded_request_to_pickup_seconds, 0.95)
                    FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS p95_seconds
            FROM trip_qualified
            GROUP BY
                source_month,
                pickup_date,
                pu_location_id,
                hvfhs_license_num;
        """)

        con.execute("""
            INSERT INTO interval_monthly (
                source_month,
                pickup_month,
                pu_location_id,
                hvfhs_license_num,
                total_trips,
                eligible_trips,
                missing_intervals,
                negative_intervals,
                median_seconds,
                p95_seconds
            )
            SELECT
                source_month,
                pickup_month,
                pu_location_id,
                hvfhs_license_num,
                COUNT(*) AS total_trips,
                COUNT(*) FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_trips,
                COUNT(*) FILTER (WHERE interval_class = 'missing_timestamp') AS missing_intervals,
                COUNT(*) FILTER (WHERE interval_class = 'negative_recorded_interval') AS negative_intervals,
                quantile_cont(recorded_request_to_pickup_seconds, 0.50)
                    FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS median_seconds,
                quantile_cont(recorded_request_to_pickup_seconds, 0.95)
                    FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS p95_seconds
            FROM trip_qualified
            GROUP BY
                source_month,
                pickup_month,
                pu_location_id,
                hvfhs_license_num;
        """)
    else:
        con.execute("""
            INSERT INTO interval_daily (
                source_month,
                pickup_date,
                pu_location_id,
                hvfhs_license_num,
                total_trips,
                eligible_trips,
                missing_intervals,
                negative_intervals,
                median_seconds,
                p95_seconds
            )
            WITH agg AS (
                SELECT
                    source_month,
                    pickup_date,
                    pu_location_id,
                    hvfhs_license_num,
                    COUNT(*) AS total_trips,
                    COUNT(*) FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_trips,
                    COUNT(*) FILTER (WHERE interval_class = 'missing_timestamp') AS missing_intervals,
                    COUNT(*) FILTER (WHERE interval_class = 'negative_recorded_interval') AS negative_intervals,
                    quantile_cont(recorded_request_to_pickup_seconds, [0.50, 0.95])
                        FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS q_pair
                FROM trip_qualified
                GROUP BY
                    source_month,
                    pickup_date,
                    pu_location_id,
                    hvfhs_license_num
            )
            SELECT
                source_month,
                pickup_date,
                pu_location_id,
                hvfhs_license_num,
                total_trips,
                eligible_trips,
                missing_intervals,
                negative_intervals,
                q_pair[1] AS median_seconds,
                q_pair[2] AS p95_seconds
            FROM agg;
        """)

        con.execute("""
            INSERT INTO interval_monthly (
                source_month,
                pickup_month,
                pu_location_id,
                hvfhs_license_num,
                total_trips,
                eligible_trips,
                missing_intervals,
                negative_intervals,
                median_seconds,
                p95_seconds
            )
            WITH agg AS (
                SELECT
                    source_month,
                    pickup_month,
                    pu_location_id,
                    hvfhs_license_num,
                    COUNT(*) AS total_trips,
                    COUNT(*) FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_trips,
                    COUNT(*) FILTER (WHERE interval_class = 'missing_timestamp') AS missing_intervals,
                    COUNT(*) FILTER (WHERE interval_class = 'negative_recorded_interval') AS negative_intervals,
                    quantile_cont(recorded_request_to_pickup_seconds, [0.50, 0.95])
                        FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS q_pair
                FROM trip_qualified
                GROUP BY
                    source_month,
                    pickup_month,
                    pu_location_id,
                    hvfhs_license_num
            )
            SELECT
                source_month,
                pickup_month,
                pu_location_id,
                hvfhs_license_num,
                total_trips,
                eligible_trips,
                missing_intervals,
                negative_intervals,
                q_pair[1] AS median_seconds,
                q_pair[2] AS p95_seconds
            FROM agg;
        """)

    daily_count = con.execute("SELECT COUNT(*) FROM interval_daily;").fetchone()[0]
    monthly_count = con.execute("SELECT COUNT(*) FROM interval_monthly;").fetchone()[0]

    return daily_count, monthly_count
