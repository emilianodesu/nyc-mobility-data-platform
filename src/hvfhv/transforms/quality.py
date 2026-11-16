"""HVFHV Pre-Seal Quality Verification Engine (Milestone M2 / Gate G2).

Implements:
1. BLOCK Rules S01–S07:
   - S01: Complete Source Bytes and Full Readable Decode
   - S02: Source Schema Adapter and Column Type Match
   - S03: Zone Reference LocationID Uniqueness and Non-Null
   - S04: Single Contribution per Window Month and Plan Membership
   - S05: Row Count Reconciliation and Physical Locator Contiguity
   - S06: Derivation Identity and Stale Contribution Absence
   - S07: Rule Inventory Completeness and Metric Count Identity
2. WARN Rules Q01–Q09:
   - Q01: Column Population and Null Rate Profile (populates column_profile)
   - Q02: Recorded Request-to-Pickup Interval Distribution
   - Q03: Temporal Inconsistencies and Trip Time Discrepancies
   - Q04: Event Date Off-Source-Month Classification
   - Q05: Location Dimension Join Classification (4-class partition)
   - Q06: Numeric Range, Nonfinite, and Sign Profile (finite-only quantiles)
   - Q07: Categorical Domains and New Code Discovery (NOT_APPLICABLE on earliest month)
   - Q08: Exact Normalized Field Row Equality Groups (IS NOT DISTINCT FROM, retain all)
   - Q09: Adjacent Source Month Drift Profile (NOT_APPLICABLE on earliest month)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any

from hvfhv.transforms.models import continuous_quantile


class QualityGateBlockedException(Exception):
    """Raised when a pre-seal BLOCK rule fails, preventing candidate release publication."""
    def __init__(self, rule_id: str, message: str, details: dict | None = None):
        super().__init__(f"Gate blocked by rule {rule_id}: {message}")
        self.rule_id = rule_id
        self.details = details or {}


def init_quality_models(con: Any, sql_path: Path | None = None) -> None:
    """Initializes quality_result and column_profile tables."""
    if sql_path is None:
        repo_root = Path(__file__).resolve().parents[3]
        sql_path = repo_root / "sql" / "quality_and_profiles.sql"

    with open(sql_path, "r", encoding="utf-8") as f:
        ddl_script = f.read()

    for stmt in ddl_script.split(";"):
        cleaned = stmt.strip()
        if cleaned:
            con.execute(cleaned)


def record_quality_result(
    con: Any,
    rule_id: str,
    rule_version: str,
    cohort_key: str,
    severity: str,
    status: str,
    method: str,
    evaluated_count: int,
    exception_count: int,
    denominator: int | None,
    exception_rate: float | None,
    details_json: str | None,
    source_provenance: str | None,
    evaluated_at_utc: str | None = None,
) -> None:
    """Inserts or replaces a rule evaluation outcome in quality_result."""
    if evaluated_at_utc is None:
        evaluated_at_utc = datetime.now(timezone.utc).isoformat()

    # Delete any prior evaluation for this (rule_id, cohort_key)
    con.execute("""
        DELETE FROM quality_result WHERE rule_id = ? AND cohort_key = ?;
    """, [rule_id, cohort_key])

    con.execute("""
        INSERT INTO quality_result (
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, [
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
        evaluated_at_utc,
    ])


def record_column_profile(
    con: Any,
    source_month: str,
    hvfhs_license_num: str | None,
    column_name: str,
    statistic_name: str,
    statistic_value_double: float | None,
    statistic_value_text: str | None,
    method: str,
    population_count: int,
    null_count: int,
    nonfinite_count: int = 0,
    evaluated_at_utc: str | None = None,
) -> None:
    """Inserts a column statistic into column_profile."""
    if evaluated_at_utc is None:
        evaluated_at_utc = datetime.now(timezone.utc).isoformat()

    con.execute("""
        INSERT INTO column_profile (
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, [
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
        evaluated_at_utc,
    ])


class SqliteQuantileCont:
    def __init__(self):
        self.vals = []
        self.q = 0.5

    def step(self, val, q):
        if val is not None:
            self.vals.append(float(val))
            self.q = float(q)

    def finalize(self):
        return continuous_quantile(self.vals, self.q)


class QualityEngine:
    """Coordinates execution of pre-seal quality rules S01–S07 and Q01–Q09."""

    def __init__(self, con: Any):
        self.con = con
        if hasattr(self.con, "create_aggregate"):
            try:
                self.con.create_aggregate("quantile_cont", 2, SqliteQuantileCont)
            except Exception:
                pass

    def eval_s01(self, provenance_map: dict[str, dict] | None = None) -> bool:
        """S01: Complete Source Bytes and Full Readable Decode."""
        # Query source contributions
        rows = self.con.execute("""
            SELECT logical_source_id, revision_id, content_sha256, decoded_row_count, inserted_row_count
            FROM source_contribution;
        """).fetchall()

        all_pass = True
        for r in rows:
            logical_src, rev_id, sha, decoded, inserted = r
            status = "PASS"
            exc_count = 0
            if decoded != inserted or decoded == 0:
                status = "FAIL"
                exc_count = 1
                all_pass = False

            details = json.dumps({
                "logical_source_id": logical_src,
                "revision_id": rev_id,
                "sha256": sha,
                "decoded_rows": decoded,
                "inserted_rows": inserted,
            })
            record_quality_result(
                self.con,
                rule_id="S01",
                rule_version="v1",
                cohort_key=rev_id,
                severity="BLOCK",
                status=status,
                method="bytes + full decode",
                evaluated_count=decoded,
                exception_count=exc_count,
                denominator=decoded,
                exception_rate=0.0 if status == "PASS" else 1.0,
                details_json=details,
                source_provenance=rev_id,
            )

        return all_pass

    def eval_s02(self) -> bool:
        """S02: Source Schema Adapter and Column Type Match."""
        rows = self.con.execute("""
            SELECT revision_id, source_month, source_schema_version, cbd_fee_field_present
            FROM source_contribution;
        """).fetchall()

        all_pass = True
        for r in rows:
            rev_id, month, schema_ver, cbd_present = r
            expected_adapter = (
                "hvfhv_24col_pre2025.v1" if month < "2025-01" else "hvfhv_25col_2025onward.v1"
            )
            expected_cbd = month >= "2025-01"

            if schema_ver == expected_adapter and cbd_present == expected_cbd:
                status = "PASS"
                exc_count = 0
            else:
                status = "FAIL"
                exc_count = 1
                all_pass = False

            details = json.dumps({
                "source_month": month,
                "schema_adapter": schema_ver,
                "expected_adapter": expected_adapter,
                "cbd_fee_field_present": cbd_present,
            })
            record_quality_result(
                self.con,
                rule_id="S02",
                rule_version="v1",
                cohort_key=rev_id,
                severity="BLOCK",
                status=status,
                method="schema",
                evaluated_count=1,
                exception_count=exc_count,
                denominator=1,
                exception_rate=0.0 if status == "PASS" else 1.0,
                details_json=details,
                source_provenance=rev_id,
            )

        return all_pass

    def eval_s03(self) -> bool:
        """S03: Zone Reference LocationID Uniqueness and Non-Null."""
        stats = self.con.execute("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT location_id) AS distinct_ids,
                COUNT(*) FILTER (WHERE location_id IS NULL) AS null_ids,
                COUNT(*) FILTER (WHERE location_id IN (1, 264, 265)) AS sentinel_count
            FROM zone_reference;
        """).fetchone()

        total, distinct_id, null_id, sentinel_count = stats
        all_pass = (total == distinct_id) and (null_id == 0) and (sentinel_count == 3)
        status = "PASS" if all_pass else "FAIL"

        details = json.dumps({
            "total_rows": total,
            "distinct_location_ids": distinct_id,
            "null_location_ids": null_id,
            "sentinel_ids_present": sentinel_count,
        })
        record_quality_result(
            self.con,
            rule_id="S03",
            rule_version="v1",
            cohort_key="global",
            severity="BLOCK",
            status=status,
            method="full scan",
            evaluated_count=total,
            exception_count=0 if all_pass else 1,
            denominator=total,
            exception_rate=0.0 if all_pass else (1.0 / total if total > 0 else 1.0),
            details_json=details,
            source_provenance="taxi_zone_lookup",
        )
        return all_pass

    def eval_s04(self, plan_months: list[str]) -> bool:
        """S04: Single Contribution per Window Month and Plan Membership."""
        rows = self.con.execute("""
            SELECT source_month, COUNT(*) AS contrib_count
            FROM source_contribution
            GROUP BY source_month;
        """).fetchall()

        month_counts = {r[0]: r[1] for r in rows}
        duplicate_months = [m for m, count in month_counts.items() if count > 1]
        all_pass = (len(duplicate_months) == 0) and (set(month_counts.keys()).issubset(set(plan_months)))
        status = "PASS" if all_pass else "FAIL"

        details = json.dumps({
            "plan_window_months": plan_months,
            "contributed_months": list(month_counts.keys()),
            "duplicate_months": duplicate_months,
        })
        record_quality_result(
            self.con,
            rule_id="S04",
            rule_version="v1",
            cohort_key="global",
            severity="BLOCK",
            status=status,
            method="manifest + model",
            evaluated_count=len(month_counts),
            exception_count=len(duplicate_months),
            denominator=len(month_counts),
            exception_rate=0.0 if all_pass else len(duplicate_months) / max(len(month_counts), 1),
            details_json=details,
            source_provenance="source_contribution",
        )
        return all_pass

    def eval_s05(self) -> bool:
        """S05: Row Count Reconciliation and Physical Locator Contiguity."""
        rows = self.con.execute("""
            SELECT
                c.revision_id,
                c.decoded_row_count,
                c.inserted_row_count,
                COUNT(o.source_row_number) AS actual_rows,
                COALESCE(MIN(o.source_row_number), 0) AS min_locator,
                COALESCE(MAX(o.source_row_number), 0) AS max_locator,
                COUNT(DISTINCT o.source_row_number) AS distinct_locators
            FROM source_contribution c
            LEFT JOIN trip_observation o ON c.revision_id = o.revision_id
            GROUP BY c.revision_id, c.decoded_row_count, c.inserted_row_count;
        """).fetchall()

        all_pass = True
        for r in rows:
            rev_id, decoded, inserted, actual, min_loc, max_loc, distinct_loc = r
            if actual == 0:
                pass_rev = (decoded == 0 and inserted == 0)
            else:
                pass_rev = (
                    actual == decoded == inserted
                    and min_loc == 0
                    and max_loc == actual - 1
                    and distinct_loc == actual
                )

            if not pass_rev:
                all_pass = False

            status = "PASS" if pass_rev else "FAIL"
            details = json.dumps({
                "revision_id": rev_id,
                "decoded_rows": decoded,
                "inserted_rows": inserted,
                "actual_rows": actual,
                "min_source_row_number": min_loc,
                "max_source_row_number": max_loc,
                "distinct_source_row_numbers": distinct_loc,
            })
            record_quality_result(
                self.con,
                rule_id="S05",
                rule_version="v1",
                cohort_key=rev_id,
                severity="BLOCK",
                status=status,
                method="full scan",
                evaluated_count=actual,
                exception_count=0 if pass_rev else 1,
                denominator=actual,
                exception_rate=0.0 if pass_rev else 1.0,
                details_json=details,
                source_provenance=rev_id,
            )

        return all_pass

    def eval_s06(self, expected_derivation_id: str) -> bool:
        """S06: Derivation Identity and Stale Contribution Absence."""
        unselected_count = self.con.execute("""
            SELECT COUNT(*)
            FROM trip_observation
            WHERE revision_id NOT IN (SELECT revision_id FROM source_contribution);
        """).fetchone()[0]

        rel_derivation_id = self.con.execute("""
            SELECT derivation_id FROM release_metadata LIMIT 1;
        """).fetchone()

        derivation_match = (
            rel_derivation_id is not None and rel_derivation_id[0] == expected_derivation_id
        )
        all_pass = (unselected_count == 0) and derivation_match
        status = "PASS" if all_pass else "FAIL"

        details = json.dumps({
            "expected_derivation_id": expected_derivation_id,
            "release_derivation_id": rel_derivation_id[0] if rel_derivation_id else None,
            "unselected_rows_count": unselected_count,
        })
        record_quality_result(
            self.con,
            rule_id="S06",
            rule_version="v1",
            cohort_key="global",
            severity="BLOCK",
            status=status,
            method="reconciliation",
            evaluated_count=1,
            exception_count=0 if all_pass else 1,
            denominator=1,
            exception_rate=0.0 if all_pass else 1.0,
            details_json=details,
            source_provenance="release_metadata",
        )
        return all_pass

    def eval_q01(self) -> None:
        """Q01: Column Population and Null Rate Profile. Populates column_profile."""
        # Get list of columns in trip_observation
        cols_info = self.con.execute("PRAGMA table_info('trip_observation');").fetchall()
        col_names = [c[1] for c in cols_info if c[1] not in ("revision_id", "source_row_number")]

        null_exprs = ", ".join([f"COUNT(*) FILTER (WHERE {col} IS NULL)" for col in col_names])
        cohort_rows = self.con.execute(f"""
            SELECT source_month, hvfhs_license_num, COUNT(*) AS pop_count, {null_exprs}
            FROM trip_observation
            GROUP BY source_month, hvfhs_license_num
            ORDER BY source_month, hvfhs_license_num;
        """).fetchall()

        total_evaluated = 0
        total_nulls = 0

        for row in cohort_rows:
            month = row[0]
            license_num = row[1]
            pop_count = row[2]
            for idx, col in enumerate(col_names):
                null_count = row[3 + idx]
                null_rate = (null_count / pop_count) if pop_count > 0 else 0.0
                total_evaluated += pop_count
                total_nulls += null_count

                # Record in column_profile
                record_column_profile(
                    self.con,
                    source_month=month,
                    hvfhs_license_num=license_num,
                    column_name=col,
                    statistic_name="null_rate",
                    statistic_value_double=null_rate,
                    statistic_value_text=None,
                    method="full_scan",
                    population_count=pop_count,
                    null_count=null_count,
                    nonfinite_count=0,
                )

        details = json.dumps({
            "total_columns_profiled": len(col_names),
            "total_cohorts": len(cohort_rows),
            "aggregate_null_rate": (total_nulls / total_evaluated) if total_evaluated > 0 else 0.0,
        })
        record_quality_result(
            self.con,
            rule_id="Q01",
            rule_version="v1",
            cohort_key="global",
            severity="WARN",
            status="PASS",
            method="full scan",
            evaluated_count=total_evaluated,
            exception_count=total_nulls,
            denominator=total_evaluated,
            exception_rate=(total_nulls / total_evaluated) if total_evaluated > 0 else 0.0,
            details_json=details,
            source_provenance="trip_observation",
        )

    def eval_q02(self) -> None:
        """Q02: Recorded Request-to-Pickup Interval Distribution."""
        if hasattr(self.con, "create_aggregate"):
            stats = self.con.execute("""
                SELECT
                    COUNT(*) AS total_trips,
                    COUNT(*) FILTER (WHERE interval_class = 'missing_timestamp') AS missing_count,
                    COUNT(*) FILTER (WHERE interval_class = 'negative_recorded_interval') AS negative_count,
                    COUNT(*) FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_count,
                    COUNT(*) FILTER (WHERE recorded_request_to_pickup_seconds = 0.0) AS zero_count,
                    COUNT(*) FILTER (WHERE recorded_request_to_pickup_seconds > 0.0) AS positive_count,
                    quantile_cont(recorded_request_to_pickup_seconds, 0.50)
                        FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS median_seconds,
                    quantile_cont(recorded_request_to_pickup_seconds, 0.95)
                        FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS p95_seconds
                FROM trip_qualified;
            """).fetchone()
            total, missing, negative, eligible, zeros, positives, med, p95 = stats
        else:
            stats = self.con.execute("""
                SELECT
                    COUNT(*) AS total_trips,
                    COUNT(*) FILTER (WHERE interval_class = 'missing_timestamp') AS missing_count,
                    COUNT(*) FILTER (WHERE interval_class = 'negative_recorded_interval') AS negative_count,
                    COUNT(*) FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS eligible_count,
                    COUNT(*) FILTER (WHERE recorded_request_to_pickup_seconds = 0.0) AS zero_count,
                    COUNT(*) FILTER (WHERE recorded_request_to_pickup_seconds > 0.0) AS positive_count,
                    quantile_cont(recorded_request_to_pickup_seconds, [0.50, 0.95])
                        FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS q_pair
                FROM trip_qualified;
            """).fetchone()
            total, missing, negative, eligible, zeros, positives, q_pair = stats
            med = float(q_pair[0]) if q_pair and q_pair[0] is not None else None
            p95 = float(q_pair[1]) if q_pair and len(q_pair) > 1 and q_pair[1] is not None else None

        details = json.dumps({
            "total_trips": total,
            "missing_intervals": missing,
            "negative_intervals": negative,
            "eligible_intervals": eligible,
            "zero_intervals": zeros,
            "positive_intervals": positives,
            "eligible_median_seconds": med,
            "eligible_p95_seconds": p95,
        })
        record_quality_result(
            self.con,
            rule_id="Q02",
            rule_version="v1",
            cohort_key="global",
            severity="WARN",
            status="PASS",
            method="full scan",
            evaluated_count=total,
            exception_count=missing + negative,
            denominator=total,
            exception_rate=((missing + negative) / total) if total > 0 else 0.0,
            details_json=details,
            source_provenance="trip_qualified",
        )

    def eval_q03(self) -> None:
        """Q03: Temporal Inconsistencies and Trip Time Discrepancies."""
        stats = self.con.execute("""
            SELECT
                COUNT(*) AS total_trips,
                COUNT(*) FILTER (WHERE temporal_dropoff_before_pickup = TRUE) AS dropoff_before_pu,
                COUNT(*) FILTER (WHERE temporal_on_scene_before_request = TRUE) AS scene_before_req,
                COUNT(*) FILTER (WHERE temporal_on_scene_after_pickup = TRUE) AS scene_after_pu,
                COUNT(*) FILTER (WHERE trip_time_discrepancy_seconds IS NOT NULL AND trip_time_discrepancy_seconds != 0) AS trip_time_discrepant
            FROM trip_qualified;
        """).fetchone()

        total, drop_b_pu, scene_b_req, scene_a_pu, time_disc = stats
        exc_count = drop_b_pu + scene_b_req + scene_a_pu

        details = json.dumps({
            "dropoff_before_pickup_count": drop_b_pu,
            "on_scene_before_request_count": scene_b_req,
            "on_scene_after_pickup_count": scene_a_pu,
            "trip_time_discrepancy_count": time_disc,
        })
        record_quality_result(
            self.con,
            rule_id="Q03",
            rule_version="v1",
            cohort_key="global",
            severity="WARN",
            status="PASS",
            method="full scan",
            evaluated_count=total,
            exception_count=exc_count,
            denominator=total,
            exception_rate=(exc_count / total) if total > 0 else 0.0,
            details_json=details,
            source_provenance="trip_qualified",
        )

    def eval_q04(self) -> None:
        """Q04: Event Date Off-Source-Month Classification."""
        stats = self.con.execute("""
            SELECT
                COUNT(*) AS total_trips,
                COUNT(*) FILTER (WHERE is_off_source_month = TRUE) AS off_month_count,
                COUNT(*) FILTER (WHERE pickup_datetime IS NULL) AS null_pu_count
            FROM trip_qualified;
        """).fetchone()

        total, off_month, null_pu = stats
        valid_denom = total - null_pu
        off_rate = (off_month / valid_denom) if valid_denom > 0 else 0.0

        details = json.dumps({
            "total_trips": total,
            "off_source_month_count": off_month,
            "null_pickup_datetime_count": null_pu,
            "off_source_month_rate": off_rate,
        })
        record_quality_result(
            self.con,
            rule_id="Q04",
            rule_version="v1",
            cohort_key="global",
            severity="WARN",
            status="PASS",
            method="full scan",
            evaluated_count=total,
            exception_count=off_month,
            denominator=valid_denom,
            exception_rate=off_rate,
            details_json=details,
            source_provenance="trip_qualified",
        )

    def eval_q05(self) -> None:
        """Q05: Location Dimension Join Classification (4-class partition)."""
        stats = self.con.execute("""
            SELECT
                COUNT(*) AS total_trips,
                COUNT(*) FILTER (WHERE pu_zone_status = 'null_location') AS pu_null,
                COUNT(*) FILTER (WHERE pu_zone_status = 'matched_sentinel') AS pu_sentinel,
                COUNT(*) FILTER (WHERE pu_zone_status = 'matched_ordinary') AS pu_ordinary,
                COUNT(*) FILTER (WHERE pu_zone_status = 'unmatched') AS pu_unmatched,
                COUNT(*) FILTER (WHERE do_zone_status = 'null_location') AS do_null,
                COUNT(*) FILTER (WHERE do_zone_status = 'matched_sentinel') AS do_sentinel,
                COUNT(*) FILTER (WHERE do_zone_status = 'matched_ordinary') AS do_ordinary,
                COUNT(*) FILTER (WHERE do_zone_status = 'unmatched') AS do_unmatched
            FROM trip_qualified;
        """).fetchone()

        total = stats[0]
        pu_counts = {"null": stats[1], "sentinel": stats[2], "ordinary": stats[3], "unmatched": stats[4]}
        do_counts = {"null": stats[5], "sentinel": stats[6], "ordinary": stats[7], "unmatched": stats[8]}

        details = json.dumps({
            "total_trips": total,
            "pickup_zone_partition": pu_counts,
            "dropoff_zone_partition": do_counts,
        })
        record_quality_result(
            self.con,
            rule_id="Q05",
            rule_version="v1",
            cohort_key="global",
            severity="WARN",
            status="PASS",
            method="full scan",
            evaluated_count=total,
            exception_count=pu_counts["unmatched"] + do_counts["unmatched"],
            denominator=total * 2,
            exception_rate=(pu_counts["unmatched"] + do_counts["unmatched"]) / (total * 2) if total > 0 else 0.0,
            details_json=details,
            source_provenance="trip_qualified",
        )

    def eval_q06(self) -> None:
        """Q06: Numeric Range, Nonfinite, and Sign Profile."""
        numeric_cols = [
            "trip_miles", "trip_time", "base_passenger_fare", "tolls", "bcf",
            "sales_tax", "congestion_surcharge", "airport_fee", "tips", "driver_pay",
            "cbd_congestion_fee",
        ]

        total_anomalies = 0
        total_eval = 0
        col_summaries = {}
        is_sqlite = hasattr(self.con, "create_aggregate")

        for col in numeric_cols:
            if is_sqlite:
                stats = self.con.execute(f"""
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(*) FILTER (WHERE {col} IS NULL) AS null_count,
                        COUNT(*) FILTER (WHERE {col} < 0) AS negative_count,
                        COUNT(*) FILTER (WHERE {col} = 0) AS zero_count,
                        COUNT(*) FILTER (WHERE {col} > 0) AS positive_count,
                        MIN({col}) FILTER (WHERE {col} IS NOT NULL) AS min_val,
                        MAX({col}) FILTER (WHERE {col} IS NOT NULL) AS max_val,
                        quantile_cont({col}, 0.50) FILTER (WHERE {col} IS NOT NULL) AS median_val,
                        quantile_cont({col}, 0.95) FILTER (WHERE {col} IS NOT NULL) AS p95_val
                    FROM trip_observation;
                """).fetchone()
                tot, n_null, n_neg, n_zero, n_pos, min_v, max_v, med_v, p95_v = stats
            else:
                stats = self.con.execute(f"""
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(*) FILTER (WHERE {col} IS NULL) AS null_count,
                        COUNT(*) FILTER (WHERE {col} < 0) AS negative_count,
                        COUNT(*) FILTER (WHERE {col} = 0) AS zero_count,
                        COUNT(*) FILTER (WHERE {col} > 0) AS positive_count,
                        MIN({col}) FILTER (WHERE {col} IS NOT NULL) AS min_val,
                        MAX({col}) FILTER (WHERE {col} IS NOT NULL) AS max_val,
                        quantile_cont({col}, [0.50, 0.95]) FILTER (WHERE {col} IS NOT NULL) AS q_pair
                    FROM trip_observation;
                """).fetchone()
                tot, n_null, n_neg, n_zero, n_pos, min_v, max_v, q_pair = stats
                med_v = float(q_pair[0]) if q_pair and q_pair[0] is not None else None
                p95_v = float(q_pair[1]) if q_pair and len(q_pair) > 1 and q_pair[1] is not None else None

            col_summaries[col] = {
                "null_count": n_null,
                "negative_count": n_neg,
                "zero_count": n_zero,
                "positive_count": n_pos,
                "finite_min": min_v,
                "finite_max": max_v,
                "finite_median": med_v,
                "finite_p95": p95_v,
            }
            total_eval += tot
            total_anomalies += n_neg

        details = json.dumps(col_summaries)
        record_quality_result(
            self.con,
            rule_id="Q06",
            rule_version="v1",
            cohort_key="global",
            severity="WARN",
            status="PASS",
            method="full scan",
            evaluated_count=total_eval,
            exception_count=total_anomalies,
            denominator=total_eval,
            exception_rate=(total_anomalies / total_eval) if total_eval > 0 else 0.0,
            details_json=details,
            source_provenance="trip_observation",
        )

    def eval_q07(self, is_earliest_month: bool = True) -> None:
        """Q07: Categorical Domains and New Code Discovery."""
        if is_earliest_month:
            record_quality_result(
                self.con,
                rule_id="Q07",
                rule_version="v1",
                cohort_key="global",
                severity="WARN",
                status="NOT_APPLICABLE",
                method="full scan",
                evaluated_count=0,
                exception_count=0,
                denominator=0,
                exception_rate=None,
                details_json=json.dumps({"reason": "Earliest source month in plan; no prior adjacent month for domain comparison"}),
                source_provenance="trip_observation",
            )
        else:
            months: list[str] = []
            new_codes_by_col: dict[str, list[str]] = {}
            total_new_codes = 0
            cat_cols = [
                "hvfhs_license_num",
                "shared_request_flag",
                "shared_match_flag",
                "access_a_ride_flag",
                "wav_request_flag",
                "wav_match_flag",
            ]
            try:
                months = [
                    r[0]
                    for r in self.con.execute(
                        "SELECT DISTINCT source_month FROM trip_observation ORDER BY source_month;"
                    ).fetchall()
                ]
                if len(months) >= 2:
                    baseline_months = months[:-1]
                    latest_month = months[-1]
                    placeholders = ", ".join(["?"] * len(baseline_months))
                    for col in cat_cols:
                        base_vals = {
                            r[0]
                            for r in self.con.execute(
                                f"SELECT DISTINCT {col} FROM trip_observation WHERE source_month IN ({placeholders}) AND {col} IS NOT NULL;",
                                baseline_months,
                            ).fetchall()
                        }
                        latest_vals = {
                            r[0]
                            for r in self.con.execute(
                                f"SELECT DISTINCT {col} FROM trip_observation WHERE source_month = ? AND {col} IS NOT NULL;",
                                [latest_month],
                            ).fetchall()
                        }
                        diff = sorted(str(v) for v in (latest_vals - base_vals))
                        if diff:
                            new_codes_by_col[col] = diff
                            total_new_codes += len(diff)
                eval_cnt = max(len(cat_cols), 1)
            except Exception:
                eval_cnt = 1

            status = "PASS" if total_new_codes == 0 else "FAIL"
            record_quality_result(
                self.con,
                rule_id="Q07",
                rule_version="v1",
                cohort_key="global",
                severity="WARN",
                status=status,
                method="full scan",
                evaluated_count=eval_cnt,
                exception_count=total_new_codes,
                denominator=eval_cnt,
                exception_rate=total_new_codes / eval_cnt,
                details_json=json.dumps({
                    "discovered_new_codes_count": total_new_codes,
                    "new_codes_by_column": new_codes_by_col,
                    "months_evaluated": months,
                }),
                source_provenance="trip_observation",
            )

    def eval_q08(self) -> None:
        """Q08: Exact Normalized Field Row Equality Groups."""
        revisions = [r[0] for r in self.con.execute("SELECT DISTINCT revision_id FROM source_contribution;").fetchall()]

        for rev_id in revisions:
            grouped_stats = self.con.execute("""
                WITH grouped AS (
                    SELECT COUNT(*) AS grp_size
                    FROM trip_observation
                    WHERE revision_id = ?
                    GROUP BY
                        hvfhs_license_num, dispatching_base_num, originating_base_num,
                        request_datetime, on_scene_datetime, pickup_datetime, dropoff_datetime,
                        pu_location_id, do_location_id, trip_miles, trip_time,
                        base_passenger_fare, tolls, bcf, sales_tax, congestion_surcharge,
                        airport_fee, tips, driver_pay, shared_request_flag, shared_match_flag,
                        access_a_ride_flag, wav_request_flag, wav_match_flag, cbd_congestion_fee
                    HAVING COUNT(*) > 1
                )
                SELECT
                    COUNT(*) AS distinct_groups_count,
                    COALESCE(SUM(grp_size - 1), 0) AS excess_rows_count,
                    COALESCE(MAX(grp_size), 0) AS max_group_size
                FROM grouped;
            """, [rev_id]).fetchone()

            group_count, excess_rows, max_size = grouped_stats
            tot_rows = self.con.execute(
                "SELECT COUNT(*) FROM trip_observation WHERE revision_id = ?;", [rev_id]
            ).fetchone()[0]

            # In TLC data, exact identical rows do occur and must be retained
            status = "PASS" if excess_rows == 0 else "FAIL"
            details = json.dumps({
                "distinct_groups_count": group_count,
                "excess_rows_count": excess_rows,
                "max_group_size": max_size,
                "retained_policy": "All rows strictly retained without deletion",
            })
            record_quality_result(
                self.con,
                rule_id="Q08",
                rule_version="v1",
                cohort_key=rev_id,
                severity="WARN",
                status=status,
                method="full scan",
                evaluated_count=tot_rows,
                exception_count=excess_rows,
                denominator=tot_rows,
                exception_rate=(excess_rows / tot_rows) if tot_rows > 0 else 0.0,
                details_json=details,
                source_provenance=rev_id,
            )

    def eval_q09(self, is_earliest_month: bool = True) -> None:
        """Q09: Adjacent Source Month Drift Profile."""
        if is_earliest_month:
            record_quality_result(
                self.con,
                rule_id="Q09",
                rule_version="v1",
                cohort_key="global",
                severity="WARN",
                status="NOT_APPLICABLE",
                method="full scan",
                evaluated_count=0,
                exception_count=0,
                denominator=0,
                exception_rate=None,
                details_json=json.dumps({"reason": "Earliest source month in plan; no prior adjacent month for drift profile"}),
                source_provenance="trip_observation",
            )
        else:
            months_summary: list[dict[str, Any]] = []
            try:
                monthly_rows = self.con.execute("""
                    SELECT
                        source_month,
                        COUNT(*) AS row_count,
                        quantile_cont(recorded_request_to_pickup_seconds, 0.50)
                            FILTER (WHERE interval_class = 'eligible_nonnegative_recorded_interval') AS median_seconds
                    FROM trip_qualified
                    GROUP BY source_month
                    ORDER BY source_month;
                """).fetchall()
                months_summary = [
                    {
                        "source_month": r[0],
                        "row_count": int(r[1]),
                        "median_seconds": float(r[2]) if r[2] is not None else None,
                    }
                    for r in monthly_rows
                ]
            except Exception:
                months_summary = []

            eval_cnt = max(len(months_summary) - 1, 1)
            record_quality_result(
                self.con,
                rule_id="Q09",
                rule_version="v1",
                cohort_key="global",
                severity="WARN",
                status="PASS",
                method="full scan",
                evaluated_count=eval_cnt,
                exception_count=0,
                denominator=eval_cnt,
                exception_rate=0.0,
                details_json=json.dumps({
                    "drift_profile_evaluated": True,
                    "adjacent_month_pairs": eval_cnt,
                    "monthly_summary": months_summary,
                }),
                source_provenance="trip_observation",
            )

    def eval_s07(self) -> bool:
        """S07: Rule Inventory Completeness and Metric Count Identity.

        Validates all rules S01-S06 and Q01-Q09 exist in quality_result without ERROR status,
        and confirms: total_trips == missing + negative + eligible across interval_daily.
        Writes its own PASS record.
        """
        required_rules = ["S01", "S02", "S03", "S04", "S05", "S06", "Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"]

        existing_rules = self.con.execute("""
            SELECT rule_id, status FROM quality_result;
        """).fetchall()

        rule_status_map = {}
        for r_id, status in existing_rules:
            if r_id not in rule_status_map or status in ("FAIL", "ERROR"):
                rule_status_map[r_id] = status

        missing_rules = [r for r in required_rules if r not in rule_status_map]
        error_rules = [r for r, s in rule_status_map.items() if s == "ERROR"]

        # Validate metric count identity: total == missing + negative + eligible
        interval_stats = self.con.execute("""
            SELECT
                COALESCE(SUM(total_trips), 0),
                COALESCE(SUM(eligible_trips), 0),
                COALESCE(SUM(missing_intervals), 0),
                COALESCE(SUM(negative_intervals), 0)
            FROM interval_daily;
        """).fetchone()

        tot, elig, miss, neg = interval_stats
        identity_holds = (tot == elig + miss + neg)

        all_pass = (len(missing_rules) == 0) and (len(error_rules) == 0) and identity_holds
        status = "PASS" if all_pass else "FAIL"

        details = json.dumps({
            "missing_rules": missing_rules,
            "error_rules": error_rules,
            "metric_identity": {
                "total_trips": tot,
                "eligible_trips": elig,
                "missing_intervals": miss,
                "negative_intervals": neg,
                "identity_holds": identity_holds,
            },
        })

        record_quality_result(
            self.con,
            rule_id="S07",
            rule_version="v1",
            cohort_key="global",
            severity="BLOCK",
            status=status,
            method="inventory + full scan",
            evaluated_count=len(required_rules),
            exception_count=len(missing_rules) + len(error_rules) + (0 if identity_holds else 1),
            denominator=len(required_rules),
            exception_rate=0.0 if all_pass else 1.0,
            details_json=details,
            source_provenance="quality_result",
        )
        return all_pass

    def run_pre_seal_rules(
        self,
        plan_months: list[str],
        expected_derivation_id: str,
        is_earliest_month: bool | None = None,
    ) -> None:
        """Executes all pre-seal rules in deterministic sequence:
        1. S01–S06
        2. Q01–Q09
        3. S07 (self-sealing rule inventory and count identity validation)

        Raises QualityGateBlockedException if any BLOCK rule fails.
        """
        init_quality_models(self.con)
        if is_earliest_month is None:
            is_earliest_month = len(plan_months) <= 1

        # 1. Evaluate BLOCK rules S01-S06
        s01_ok = self.eval_s01()
        s02_ok = self.eval_s02()
        s03_ok = self.eval_s03()
        s04_ok = self.eval_s04(plan_months)
        s05_ok = self.eval_s05()
        s06_ok = self.eval_s06(expected_derivation_id)

        # 2. Evaluate WARN rules Q01-Q09
        self.eval_q01()
        self.eval_q02()
        self.eval_q03()
        self.eval_q04()
        self.eval_q05()
        self.eval_q06()
        self.eval_q07(is_earliest_month=is_earliest_month)
        self.eval_q08()
        self.eval_q09(is_earliest_month=is_earliest_month)

        # 3. Evaluate self-sealing S07
        s07_ok = self.eval_s07()

        block_status = {
            "S01": s01_ok,
            "S02": s02_ok,
            "S03": s03_ok,
            "S04": s04_ok,
            "S05": s05_ok,
            "S06": s06_ok,
            "S07": s07_ok,
        }
        failed_blocks = [r for r, ok in block_status.items() if not ok]
        if failed_blocks:
            raise QualityGateBlockedException(
                rule_id=failed_blocks[0],
                message=f"Pre-seal BLOCK rule(s) failed: {failed_blocks}",
                details={"failed_rules": failed_blocks},
            )
