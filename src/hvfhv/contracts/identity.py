"""Deterministic canonical identity hashing for HVFHV data platform.

Implements RFC 8785 compliant canonical JSON serialization, revision hashing,
and frozen input plan derivation hashing.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence


def canonical_json_dumps(obj: Any) -> str:
    """Serialize a Python data structure to a deterministic canonical JSON string.

    Invariants (RFC 8785 inspired):
    - UTF-8 representation without BOM.
    - Object keys sorted lexicographically at all nesting depths.
    - Compact separators: (',', ':') with zero extraneous whitespace.
    - Strict rejection of NaN, Infinity, and -Infinity (raises ValueError).
    - Explicit null semantics: None serializes to null.
    - Dictionary keys must be strings.
    """
    def _validate_types(val: Any) -> None:
        if isinstance(val, float):
            if math.isnan(val) or math.isinf(val):
                raise ValueError(f"Canonical JSON does not permit non-finite floats: {val}")
        elif isinstance(val, dict):
            for k, v in val.items():
                if not isinstance(k, str):
                    raise TypeError(f"Canonical JSON dictionary keys must be strings, got {type(k).__name__}: {k!r}")
                _validate_types(v)
        elif isinstance(val, (list, tuple)):
            for v in val:
                _validate_types(v)

    _validate_types(obj)

    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def canonical_json_bytes(obj: Any) -> bytes:
    """Serialize a Python data structure to deterministic canonical UTF-8 bytes."""
    return canonical_json_dumps(obj).encode("utf-8")


def compute_revision_id(logical_source_id: str, content_sha256: str) -> str:
    """Compute revision_id as SHA-256 digest of canonical (logical_source_id, content_sha256).

    Args:
        logical_source_id: e.g. 'tlc/hvfhv/2024-12' or 'tlc/taxi_zones'
        content_sha256: 64-character lowercase hexadecimal SHA-256 digest of raw bytes

    Returns:
        64-character lowercase hexadecimal SHA-256 string.
    """
    if not logical_source_id or not logical_source_id.strip():
        raise ValueError("logical_source_id must be a non-empty string")

    clean_sha = content_sha256.strip().lower()
    if len(clean_sha) != 64 or not all(c in "0123456789abcdef" for c in clean_sha):
        raise ValueError(f"content_sha256 must be a valid 64-char hex digest, got: {content_sha256!r}")

    payload = {
        "content_sha256": clean_sha,
        "logical_source_id": logical_source_id.strip(),
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


@dataclass(frozen=True)
class SourceRevisionEntry:
    """Logical source to accepted revision mapping."""
    logical_source_id: str
    revision_id: str


@dataclass(frozen=True)
class FrozenInputPlan:
    """Immutable input specification defining derivation identity."""
    source_revisions: Sequence[SourceRevisionEntry]
    window_months: Sequence[str]
    zone_revision_id: str
    schema_adapter_version: str
    rules_hash: str
    metric_definition_hash: str
    code_bundle_manifest_hash: str
    lockfile_hash: str
    model_schema_version: str
    semantic_configuration: Mapping[str, Any]


def compute_derivation_id(plan: FrozenInputPlan) -> str:
    """Compute derivation_id as SHA-256 digest of canonical FrozenInputPlan.

    Normalizes:
    - source_revisions sorted by logical_source_id
    - window_months sorted chronologically
    - excludes timestamps, local file paths, attempt_ids, and expected_parent_release_id

    Returns:
        64-character lowercase hexadecimal SHA-256 string.
    """
    # Normalize source revisions: sort by logical_source_id
    sorted_sources = sorted(
        [
            {"logical_source_id": entry.logical_source_id, "revision_id": entry.revision_id}
            for entry in plan.source_revisions
        ],
        key=lambda x: x["logical_source_id"],
    )

    # Normalize window months: sort chronologically
    sorted_months = sorted(list(plan.window_months))

    payload = {
        "code_bundle_manifest_hash": plan.code_bundle_manifest_hash,
        "lockfile_hash": plan.lockfile_hash,
        "metric_definition_hash": plan.metric_definition_hash,
        "model_schema_version": plan.model_schema_version,
        "rules_hash": plan.rules_hash,
        "schema_adapter_version": plan.schema_adapter_version,
        "semantic_configuration": dict(plan.semantic_configuration),
        "source_revisions": sorted_sources,
        "window_months": sorted_months,
        "zone_revision_id": plan.zone_revision_id,
    }

    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def compute_code_bundle_manifest_hash(repo_root: Path) -> tuple[str, dict[str, str]]:
    """Compute SHA-256 hash of all executable source and contract files.

    Scans src/**/*.py, sql/**/*.sql, contracts/**/*.json.
    Produces a relative posix path -> sha256 manifest dictionary, and computes its
    canonical JSON hash.
    """
    manifest: dict[str, str] = {}
    scan_patterns = [
        "src/**/*.py",
        "sql/**/*.sql",
        "contracts/**/*.json",
    ]

    for pattern in scan_patterns:
        for file_path in sorted(repo_root.glob(pattern)):
            if file_path.is_file() and "__pycache__" not in file_path.parts:
                rel_path = file_path.relative_to(repo_root).as_posix()
                content = file_path.read_bytes()
                manifest[rel_path] = hashlib.sha256(content).hexdigest()

    manifest_hash = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    return manifest_hash, manifest
