"""Frontier discovery for NYC TLC HVFHV and Zone references.

Implements non-halting frontier discovery across active window, forward months,
and taxi zone lookup, enforcing 12 consecutive month completeness and host allowlists.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime
from enum import Enum
import urllib.parse
from typing import Any, Sequence
import uuid

try:
    import httpx
except ImportError:
    httpx = None


OFFICIAL_HOST_ALLOWLIST = frozenset({
    "d37ci6vzurychx.cloudfront.net",
    "www.nyc.gov",
    "nyc.gov",
})

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


class ObservationOutcome(str, Enum):
    AVAILABLE = "AVAILABLE"        # HTTP 200 / 206
    UNAVAILABLE = "UNAVAILABLE"    # HTTP 404
    FAILED = "FAILED"              # HTTP 429, 5xx, timeout, network error
    ACCEPTED = "ACCEPTED"
    REUSED = "REUSED"


@dataclass(frozen=True)
class DiscoveryObservation:
    observation_id: str
    logical_source_id: str
    url: str
    method: str
    observed_time: str
    http_status: int | None
    etag: str | None
    last_modified: str | None
    advertised_bytes: int | None
    outcome: ObservationOutcome | str
    error_message: str | None = None


@dataclass(frozen=True)
class ProposedWindow:
    end_month: str
    months: list[str]            # Exactly 12 consecutive months
    is_complete: bool
    available_months: list[str]
    missing_months: list[str]


@dataclass(frozen=True)
class AcquisitionProposal:
    logical_source_id: str
    url: str
    reason: str  # "NEW_MONTH", "REVISED_UPSTREAM", "MISSING_LOCAL", "ZONE_REFERENCE", "FORCED_REVALIDATION"
    observation: DiscoveryObservation


@dataclass(frozen=True)
class DiscoveryPlan:
    checked_at_utc: str
    active_window: list[str]
    frontier_months_scanned: list[str]
    observations: list[DiscoveryObservation]
    proposed_window: ProposedWindow | None
    pending_gaps: list[str]
    proposals: list[AcquisitionProposal]
    zone_proposal: AcquisitionProposal | None


def format_period(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def parse_period(period_str: str) -> tuple[int, int]:
    parts = period_str.strip().split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid period format: '{period_str}'. Expected 'YYYY-MM'.")
    return int(parts[0]), int(parts[1])


def add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    total_months = year * 12 + (month - 1) + delta
    new_year = total_months // 12
    new_month = (total_months % 12) + 1
    return new_year, new_month


def period_range(start_period: str, end_period: str) -> list[str]:
    sy, sm = parse_period(start_period)
    ey, em = parse_period(end_period)
    periods: list[str] = []
    cy, cm = sy, sm
    while (cy < ey) or (cy == ey and cm <= em):
        periods.append(format_period(cy, cm))
        cy, cm = add_months(cy, cm, 1)
    return periods


def get_completed_calendar_month(now: datetime.datetime | None = None) -> str:
    """Return the last fully completed calendar month in UTC."""
    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)
    y, m = add_months(now.year, now.month, -1)
    return format_period(y, m)


def build_monthly_url(period: str) -> str:
    return f"{TLC_BASE_URL}/fhvhv_tripdata_{period}.parquet"


def build_logical_source_id(period: str) -> str:
    return f"tlc/hvfhv/{period}"


def check_source_metadata(
    client: Any,
    url: str,
    logical_source_id: str,
) -> DiscoveryObservation:
    """Perform bounded HTTP metadata check with allowlist validation."""
    parsed = urllib.parse.urlparse(url)
    obs_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if parsed.hostname not in OFFICIAL_HOST_ALLOWLIST:
        return DiscoveryObservation(
            observation_id=str(uuid.uuid4()),
            logical_source_id=logical_source_id,
            url=url,
            method="HEAD",
            observed_time=obs_time,
            http_status=None,
            etag=None,
            last_modified=None,
            advertised_bytes=None,
            outcome=ObservationOutcome.FAILED,
            error_message=f"Host '{parsed.hostname}' not in official allowlist",
        )

    headers = {"Accept-Encoding": "identity", "User-Agent": "HVFHV-Platform/0.1.0"}
    try:
        response = client.head(url, headers=headers)
        if response.status_code == 405:
            response = client.get(url, headers={**headers, "Range": "bytes=0-0"})

        etag = response.headers.get("ETag") or response.headers.get("etag")
        last_modified = response.headers.get("Last-Modified") or response.headers.get("last-modified")
        content_length_str = response.headers.get("Content-Length") or response.headers.get("content-length")
        advertised_bytes = int(content_length_str) if content_length_str and content_length_str.isdigit() else None

        if response.status_code in (200, 206):
            outcome = ObservationOutcome.AVAILABLE
        elif response.status_code == 404:
            outcome = ObservationOutcome.UNAVAILABLE
        else:
            outcome = ObservationOutcome.FAILED

        req_method = getattr(getattr(response, "request", None), "method", "HEAD")

        return DiscoveryObservation(
            observation_id=str(uuid.uuid4()),
            logical_source_id=logical_source_id,
            url=url,
            method=req_method,
            observed_time=obs_time,
            http_status=response.status_code,
            etag=etag,
            last_modified=last_modified,
            advertised_bytes=advertised_bytes,
            outcome=outcome,
            error_message=None if outcome != ObservationOutcome.FAILED else f"HTTP {response.status_code}",
        )
    except Exception as exc:
        return DiscoveryObservation(
            observation_id=str(uuid.uuid4()),
            logical_source_id=logical_source_id,
            url=url,
            method="HEAD",
            observed_time=obs_time,
            http_status=None,
            etag=None,
            last_modified=None,
            advertised_bytes=None,
            outcome=ObservationOutcome.FAILED,
            error_message=str(exc),
        )


def discover_frontier(
    active_window: Sequence[str],
    registered_revisions: dict[str, dict[str, Any]],
    client: Any = None,
    observation_cutoff: str | None = None,
    force_revalidation: bool = False,
    bootstrap_scan_months: int = 24,
) -> DiscoveryPlan:
    """Discover upstream frontier across active window, forward months, and zones.

    Guarantees:
    - Never halts on 404 (Scenario T25, Walkthrough W04).
    - Checks zone reference and active-window objects.
    - Proposes 12 consecutive months; records pending gaps on missing months.
    """
    cutoff = observation_cutoff or get_completed_calendar_month()
    checked_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    all_observations: list[DiscoveryObservation] = []
    proposals: list[AcquisitionProposal] = []

    should_close_client = False
    if client is None:
        if httpx is not None:
            client = httpx.Client(
                timeout=httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0),
                follow_redirects=False,
                verify=True,
            )
            should_close_client = True
        else:
            raise RuntimeError("HTTP client not provided and httpx is not installed.")

    try:
        # --- 1. Active Window Revalidation ---
        for period in active_window:
            ls_id = build_logical_source_id(period)
            url = build_monthly_url(period)
            obs = check_source_metadata(client, url, ls_id)
            all_observations.append(obs)

            reg = registered_revisions.get(ls_id)
            if obs.outcome == ObservationOutcome.AVAILABLE:
                if reg is None:
                    proposals.append(AcquisitionProposal(ls_id, url, "MISSING_LOCAL", obs))
                elif force_revalidation:
                    proposals.append(AcquisitionProposal(ls_id, url, "FORCED_REVALIDATION", obs))
                elif obs.etag and reg.get("etag") and obs.etag != reg.get("etag"):
                    proposals.append(AcquisitionProposal(ls_id, url, "REVISED_UPSTREAM", obs))

        # --- 2. Forward Frontier Scanning ---
        if active_window:
            latest_active = max(active_window)
            start_frontier = format_period(*add_months(*parse_period(latest_active), 1))
        else:
            # Bootstrap: scan backwards from cutoff by bootstrap_scan_months
            cy, cm = parse_period(cutoff)
            sy, sm = add_months(cy, cm, -(bootstrap_scan_months - 1))
            start_frontier = format_period(sy, sm)

        frontier_periods = period_range(start_frontier, cutoff) if start_frontier <= cutoff else []

        available_periods: set[str] = set()
        # Include already known available active periods
        for p in active_window:
            ls_id = build_logical_source_id(p)
            if any(o.logical_source_id == ls_id and o.outcome == ObservationOutcome.AVAILABLE for o in all_observations):
                available_periods.add(p)

        for period in frontier_periods:
            ls_id = build_logical_source_id(period)
            url = build_monthly_url(period)
            obs = check_source_metadata(client, url, ls_id)
            all_observations.append(obs)

            if obs.outcome == ObservationOutcome.AVAILABLE:
                available_periods.add(period)
                proposals.append(AcquisitionProposal(ls_id, url, "NEW_MONTH", obs))
            # Critical: if obs.outcome is UNAVAILABLE (404), DO NOT BREAK! Continue scanning.

        # --- 3. Taxi Zone Lookup Check ---
        zone_ls_id = "tlc/taxi_zones"
        zone_obs = check_source_metadata(client, ZONE_LOOKUP_URL, zone_ls_id)
        all_observations.append(zone_obs)
        zone_proposal: AcquisitionProposal | None = None
        reg_zone = registered_revisions.get(zone_ls_id)
        if zone_obs.outcome == ObservationOutcome.AVAILABLE:
            if reg_zone is None:
                zone_proposal = AcquisitionProposal(zone_ls_id, ZONE_LOOKUP_URL, "ZONE_REFERENCE", zone_obs)
            elif force_revalidation or (zone_obs.etag and reg_zone.get("etag") and zone_obs.etag != reg_zone.get("etag")):
                zone_proposal = AcquisitionProposal(zone_ls_id, ZONE_LOOKUP_URL, "REVISED_UPSTREAM", zone_obs)

        # --- 4. 12 Consecutive Month Proposed Window ---
        if not available_periods:
            proposed_window = None
            pending_gaps: list[str] = []
        else:
            latest_available = max(available_periods)
            ly, lm = parse_period(latest_available)
            sy, sm = add_months(ly, lm, -11)
            target_12_months = period_range(format_period(sy, sm), latest_available)

            missing_months = [m for m in target_12_months if m not in available_periods]
            is_complete = len(missing_months) == 0 and len(target_12_months) == 12

            proposed_window = ProposedWindow(
                end_month=latest_available,
                months=target_12_months,
                is_complete=is_complete,
                available_months=[m for m in target_12_months if m in available_periods],
                missing_months=missing_months,
            )
            pending_gaps = missing_months

        return DiscoveryPlan(
            checked_at_utc=checked_at,
            active_window=list(active_window),
            frontier_months_scanned=frontier_periods,
            observations=all_observations,
            proposed_window=proposed_window,
            pending_gaps=pending_gaps,
            proposals=proposals,
            zone_proposal=zone_proposal,
        )
    finally:
        if should_close_client:
            client.close()
