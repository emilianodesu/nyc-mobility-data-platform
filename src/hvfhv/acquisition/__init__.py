"""Acquisition subpackage for HVFHV platform."""

from hvfhv.acquisition.acquire import (
    AcquisitionError,
    CorruptionError,
    SecurityError,
    SourceUnavailableError,
    TransportError,
    UnsupportedSchemaError,
    acquire_source,
    identify_parquet_schema,
    reconcile_raw_storage,
    validate_url,
)
from hvfhv.acquisition.discover import (
    AcquisitionProposal,
    DiscoveryObservation,
    DiscoveryPlan,
    ObservationOutcome,
    ProposedWindow,
    check_source_metadata,
    discover_frontier,
)

__all__ = [
    "AcquisitionError",
    "AcquisitionProposal",
    "CorruptionError",
    "DiscoveryObservation",
    "DiscoveryPlan",
    "ObservationOutcome",
    "ProposedWindow",
    "SecurityError",
    "SourceUnavailableError",
    "TransportError",
    "UnsupportedSchemaError",
    "acquire_source",
    "check_source_metadata",
    "discover_frontier",
    "identify_parquet_schema",
    "reconcile_raw_storage",
    "validate_url",
]
