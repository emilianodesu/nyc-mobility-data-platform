"""Streamlit Read-Only Analytical Dashboard for Published Releases.

dual views:
1. Data Reliability View (coverage, rules S01-S08, profiles Q01-Q09, operational state)
2. Recorded Intervals View (exact continuous quantiles, denominators, non-SLA disclosure)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from hvfhv.config import get_default_config
from hvfhv.consumer.reader import ReleaseReader, IneligibleReleaseError, RetiredReleaseError


def get_data_root() -> Path:
    env_root = os.environ.get("HVFHV_DATA_ROOT")
    if env_root:
        return Path(env_root).resolve()
    # Default to .data in project directory
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / ".data"


def render_headless_summary(reader: ReleaseReader) -> dict[str, Any]:
    """Execute both views headlessly for automated verification and CLI display."""
    rel_summary = reader.get_reliability_summary()
    intervals = reader.query_recorded_intervals(limit=50)
    return {
        "reliability": rel_summary,
        "intervals": intervals,
    }


def main():
    """Streamlit application entry point."""
    try:
        import streamlit as st
    except ImportError:
        print("Streamlit is not installed in this environment. Run via headless mode or install streamlit.")
        sys.exit(0)

    st.set_page_config(
        page_title="NYC TLC HVFHV Data Reliability Platform",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    data_root = get_data_root()
    st.sidebar.title("HVFHV Reliability Platform")
    st.sidebar.markdown(f"**Data Root**: `{data_root}`")

    # Connect to Reader
    try:
        reader = ReleaseReader(data_root=data_root)
        eligibility = reader.check_eligibility()
        st.sidebar.success(f"Pinned Release: `{reader.release_id}` ({eligibility['status']})")
    except RetiredReleaseError as e:
        st.error(f"Selected Release is RETIRED: {e}")
        st.sidebar.error("Release RETIRED")
        return
    except IneligibleReleaseError as e:
        st.warning(f"No active verified release: {e}")
        st.sidebar.warning("No Verified Release")
        return
    except Exception as e:
        st.error(f"Error accessing release: {e}")
        return

    view_mode = st.sidebar.radio(
        "Select Analytical View",
        ["Data Reliability View", "Recorded Intervals View"],
    )

    if view_mode == "Data Reliability View":
        with st.container(key="view_data_reliability"):
            st.title("Data Reliability & Quality Overview")
            st.caption("Release provenance, coverage truth, pre-seal quality rules (S01–S08), and semantic profiles (Q01–Q09).")

            summary = reader.get_reliability_summary()
            meta = summary.get("metadata", {})
            ops = summary.get("operational_state", {})

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Current Generation", str(ops.get("current_generation", "N/A")))
            col2.metric(
                "Window Covered",
                f"{meta.get('first_source_month', 'N/A')} → {meta.get('last_source_month', 'N/A')}",
            )
            col3.metric("Model Schema Version", str(meta.get("model_schema_version", "v1")))
            col4.metric("Release Status", str(eligibility.get("status", "VERIFIED")))

            built_at = str(meta.get("built_at_utc") or meta.get("finalized_at_utc") or "N/A")
            st.markdown(
                f"- **Release ID**: `{meta.get('release_id', reader.release_id)}`  \n"
                f"- **Derivation ID**: `{meta.get('derivation_id', 'N/A')}`  \n"
                f"- **Built / Finalized (UTC)**: `{built_at}`"
            )

            st.subheader("Source Contributions")
            contribs = summary.get("source_contributions", [])
            if contribs:
                st.dataframe(contribs, width="stretch")
            else:
                st.info("No source contributions available in release.")

            st.subheader("Source Month Coverage")
            st.caption("Explicit representation of months; absent source months have NULL row counts (never 0).")
            cov_data = summary.get("coverage", [])
            if cov_data:
                st.dataframe(cov_data, width="stretch")
            else:
                st.info("No coverage records available in release.")

            st.subheader("Quality Rules & Post-Seal S08 Verification")
            st.caption("Pre-seal rules (S01–S07, Q01–Q09) and post-seal S08 attestation.")
            rules = list(summary.get("quality_rules", []))
            s08 = summary.get("s08_verification")
            if s08:
                st.success(f"Post-Seal S08 Verified at {s08.get('verified_at_utc')} (Verification ID: {s08.get('verification_id')})")
            if rules:
                st.dataframe(rules, width="stretch")
            else:
                st.info("No quality rules recorded.")

            st.subheader("Full-Scan Column Profiles")
            profiles = summary.get("column_profiles", [])
            if profiles:
                st.dataframe(profiles, width="stretch")
            else:
                st.info("No column profiles recorded.")

            st.subheader("Live Operational Control Plane State")
            last_check_raw = str(ops.get("last_source_check_utc") or "N/A")
            last_check_short = last_check_raw[:16].replace("T", " ") + "Z" if len(last_check_raw) >= 16 else last_check_raw
            c1, c2, c3 = st.columns(3)
            c1.metric("Current Generation", ops.get("current_generation", "N/A"))
            c2.metric("Total Materialization Attempts", ops.get("total_attempts_recorded", "N/A"))
            c3.metric("Last Source Check (UTC)", last_check_short)
            st.caption(f"Full Last Source Check Timestamp: `{last_check_raw}`")

    elif view_mode == "Recorded Intervals View":
        with st.container(key="view_recorded_intervals"):
            st.title("Recorded Request-to-Pickup Intervals")
            st.caption(
                "Exact Type 7 continuous quantiles evaluated on eligible trips. "
                "Intervals are naive 64-bit microsecond differences without SLA or wait-time claims."
            )

            st.sidebar.markdown("---")
            st.sidebar.subheader("Filter Populations")
            f_month = st.sidebar.text_input("Source Month (YYYY-MM)", value="")
            f_date = st.sidebar.text_input("Pickup Date (YYYY-MM-DD)", value="")
            f_license = st.sidebar.text_input("HVFHS License (e.g. HV0003)", value="")
            f_pu = st.sidebar.number_input("PU Location ID (1-265)", min_value=0, max_value=265, value=0)

            query_pu = int(f_pu) if f_pu > 0 else None
            query_month = f_month.strip() if f_month.strip() else None
            query_date = f_date.strip() if f_date.strip() else None
            query_license = f_license.strip() if f_license.strip() else None

            result = reader.query_recorded_intervals(
                source_month=query_month,
                pickup_date=query_date,
                hvfhs_license_num=query_license,
                pu_location_id=query_pu,
                limit=500,
            )

            denoms = result["denominators"]

            # Denominator KPI cards
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Trips", f"{denoms.get('total_population_count', 0):,}")
            k2.metric("Eligible Non-negative", f"{denoms.get('eligible_count', 0):,}")
            k3.metric("Missing Timestamps", f"{denoms.get('missing_count', 0):,}")
            k4.metric("Negative Recorded Intervals", f"{denoms.get('negative_count', 0):,}")

            # Quantile KPI cards
            q1, q2, q3 = st.columns(3)
            med = denoms.get("median_seconds")
            p95 = denoms.get("p95_seconds")
            elig_frac = denoms.get("eligible_fraction", 0.0)
            q1.metric("Exact Median Interval (p50)", f"{med:.2f} s" if med is not None else "N/A")
            q2.metric("Exact 95th Percentile Interval (p95)", f"{p95:.2f} s" if p95 is not None else "N/A")
            q3.metric("Eligible Fraction", f"{elig_frac * 100.0:.2f}%" if elig_frac is not None else "N/A")

            st.subheader("Daily Aggregate Breakdown by Pickup Location & License")
            breakdown = result.get("daily_breakdown", [])
            if breakdown:
                st.dataframe(breakdown, width="stretch")

            st.subheader("Sample Trip Observations")
            rows = result.get("sample_rows", [])
            if rows:
                st.dataframe(rows, width="stretch")
            else:
                st.info("No trip records matching criteria.")

            st.markdown("---")
            st.caption("### Invariant Disclosures")
            for k, v in result.get("disclosures", {}).items():
                st.markdown(f"- **{k}**: {v}")


if __name__ == "__main__":
    main()
