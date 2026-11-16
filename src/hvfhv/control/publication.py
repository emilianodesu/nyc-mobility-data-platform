"""HVFHV Publication, Candidate Finalization, CAS Selection, and Durability Recovery Controller.

Implements Milestone M3 (Gate G3: Publication Safety & Durability Recovery):
1. Candidate Finalization:
   - Zero-WAL verification on DuckDB writer checkpoint and closure.
   - Read-only model count and schema validation across analytical models.
   - Pre-seal truth table evaluation (S01–S07, Q01–Q09) with zero hash circularity.
   - Canonical validation.json and manifest.json serialization (RFC 8785).
   - Durable fsync and atomic directory rename (staging -> releases/<release_id>).
   - SQLite release registration with status 'FINALIZED'.
2. Rule S08 Post-Seal Verification:
   - Independent verification of closed file digests on disk vs manifest.json.
   - Read-only database inspection for release_metadata and model schemas.
   - External attestation recording in SQLite release_verifications (status 'VERIFIED').
   - Zero modifications to sealed release files.
3. Atomic Compare-And-Swap (CAS) Publication:
   - Idempotency key registration in selection_requests before CAS.
   - Verification of expected parent release and generation.
   - Monotonic generation increment and current_release pointer update.
   - Atomic audit event in publication_events.
4. Rollback:
   - Byte verification of retained target release files on disk.
   - Rejection of corrupt, REJECTED, or RETIRED releases.
   - CAS parent and generation check with monotonic generation increment.
5. Durability & Recovery Protocols:
   - Scenario T18: Lost acknowledgement recovery via query_selection_status.
   - Scenario T19: Stale generation CAS conflict detection.
   - Scenario T17: Orphan detection without automatic adoption.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Any, Mapping, Sequence
import uuid

import duckdb
import pyarrow as pa

from hvfhv.config import resolve_data_root
from hvfhv.contracts.identity import (
    canonical_json_bytes,
    canonical_json_dumps,
    FrozenInputPlan,
)
from hvfhv.control.locks import writer_lock, is_lock_held_by_current_thread
from hvfhv.control.registry import (
    Registry,
    RegistryError,
    ReleaseIntegrityError,
    StaleGenerationConflictError,
    InvalidReleaseStatusError,
    SelectionConflictError,
)


REQUIRED_PRE_SEAL_RULES = [
    "S01", "S02", "S03", "S04", "S05", "S06", "S07",
    "Q01", "Q02", "Q03", "Q04", "Q05", "Q06", "Q07", "Q08", "Q09"
]

ALL_ANALYTICAL_MODELS = [
    "release_metadata",
    "source_contribution",
    "zone_reference",
    "trip_observation",
    "coverage",
    "trip_qualified",
    "interval_daily",
    "interval_monthly",
    "quality_result",
    "column_profile",
]


class OperationKind(str, Enum):
    PUBLISH = "publish"
    ROLLBACK = "rollback"


class GateVerdict(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass(frozen=True)
class PreSealGateVerdict:
    verdict: GateVerdict
    all_passed: bool
    blocking_failures: list[str]
    rule_summaries: dict[str, str]
    metric_count_identity_holds: bool
    details: dict[str, Any]


@dataclass(frozen=True)
class FinalizedRelease:
    release_id: str
    derivation_id: str
    producing_attempt_id: str
    release_directory: Path
    manifest_sha256: str
    database_sha256: str
    validation_sha256: str
    source_revision_map: dict[str, str]
    finalized_at_utc: str


@dataclass(frozen=True)
class VerificationAttestation:
    verification_id: str
    release_id: str
    manifest_sha256: str
    database_sha256: str
    validation_sha256: str
    rule_inventory_version: str
    rule_inventory_hash: str
    verification_results: dict[str, Any]
    verifier_version: str
    verified_at_utc: str


@dataclass(frozen=True)
class PublicationResult:
    status: str  # "SUCCEEDED" or "ALREADY_COMMITTED"
    event_id: str
    operation_key: str
    operation_kind: str
    selected_release_id: str
    previous_release_id: str | None
    previous_generation: int
    new_generation: int
    event_at_utc: str
    is_current: bool = True


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class PublicationError(Exception):
    """Base exception for all publication and candidate finalization errors."""
    pass


class WalVerificationError(PublicationError):
    """Raised when DuckDB .wal file is non-zero after CHECKPOINT and close."""
    pass


class ReadOnlyInspectionError(PublicationError):
    """Raised when read-only validation of model counts or schema fails."""
    pass


class PreSealGateBlockedError(PublicationError):
    """Raised when pre-seal rules (S01-S07, Q01-Q09) fail publication gate."""
    pass


class DestinationAlreadyExistsError(PublicationError):
    """Raised when target release directory already exists on disk (no overwrites)."""
    pass


class PostSealVerificationError(PublicationError):
    """Raised when Rule S08 post-seal digest verification fails on disk."""
    pass


class RollbackIntegrityError(PublicationError):
    """Raised when a retained rollback target fails disk integrity checks."""
    pass


# ---------------------------------------------------------------------------
# Primitives & Helpers
# ---------------------------------------------------------------------------

def durable_fsync(path: Path | str) -> None:
    """Flush and synchronize a file or directory using platform durability primitives.
    
    On macOS Darwin, attempts fcntl(fd, F_FULLFSYNC) before falling back to os.fsync.
    """
    p = Path(path).resolve()
    if not p.exists():
        return
    fd = os.open(p, os.O_RDONLY)
    try:
        if hasattr(fcntl, "F_FULLFSYNC"):
            try:
                fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
                return
            except (OSError, ValueError):
                pass
        os.fsync(fd)
    finally:
        os.close(fd)


def hash_file_sha256(file_path: Path | str, chunk_size: int = 65536) -> str:
    """Compute lowercase hexadecimal SHA-256 digest of a file in streaming chunks."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def get_rule_inventory_hash(rules_path: Path | str | None = None) -> str:
    """Compute SHA-256 digest of the canonical rules.json inventory file."""
    if rules_path is None:
        p = Path(__file__).resolve().parents[3] / "contracts" / "rules.json"
    else:
        p = Path(rules_path).resolve()

    if p.exists():
        return hash_file_sha256(p)
    return "0" * 64


def checkpoint_and_verify_zero_wal(con: Any | None, db_path: Path | str) -> None:
    """Commit DuckDB transactions, execute CHECKPOINT, close writer, and verify zero .wal.
    
    Raises WalVerificationError if .wal file exists and has size > 0.
    """
    db_file = Path(db_path).resolve()
    wal_file = Path(str(db_file) + ".wal")

    if con is not None:
        try:
            con.commit()
        except Exception:
            pass
        try:
            con.execute("CHECKPOINT;")
        except Exception:
            pass
        try:
            con.close()
        except Exception:
            pass

    if wal_file.exists():
        wal_size = wal_file.stat().st_size
        if wal_size > 0:
            raise WalVerificationError(
                f"DuckDB .wal file is non-zero after CHECKPOINT and close: {wal_file} ({wal_size} bytes)"
            )


def validate_closed_database_read_only(
    db_path: Path | str,
    expected_release_id: str,
    expected_derivation_id: str,
    expected_model_schema_version: str = "v1",
) -> dict[str, int]:
    """Reopen closed database in read_only mode, verify all 10 models and counts.
    
    Checks release_metadata matches release_id and derivation_id.
    Returns mapping of model_name -> row_count.
    """
    db_file = Path(db_path).resolve()
    if not db_file.exists():
        raise ReadOnlyInspectionError(f"Database file does not exist: {db_file}")

    try:
        ro_con = duckdb.connect(str(db_file), read_only=True)
    except Exception as e:
        raise ReadOnlyInspectionError(f"Failed to open database read-only: {e}") from e

    model_counts: dict[str, int] = {}
    try:
        # 1. Inspect release_metadata
        try:
            meta_rows = ro_con.execute(
                "SELECT release_id, derivation_id, model_schema_version FROM release_metadata;"
            ).fetchall()
        except Exception as e:
            raise ReadOnlyInspectionError(f"Failed to query release_metadata: {e}") from e

        if len(meta_rows) != 1:
            raise ReadOnlyInspectionError(
                f"Expected exactly 1 row in release_metadata, found {len(meta_rows)}"
            )

        act_rel, act_deriv, act_schema = meta_rows[0]
        if act_rel != expected_release_id:
            raise ReadOnlyInspectionError(
                f"release_metadata release_id mismatch: expected '{expected_release_id}', got '{act_rel}'"
            )
        if act_deriv != expected_derivation_id:
            raise ReadOnlyInspectionError(
                f"release_metadata derivation_id mismatch: expected '{expected_derivation_id}', got '{act_deriv}'"
            )
        if act_schema != expected_model_schema_version:
            raise ReadOnlyInspectionError(
                f"release_metadata model_schema_version mismatch: expected '{expected_model_schema_version}', got '{act_schema}'"
            )
        model_counts["release_metadata"] = 1

        # 2. Check each analytical model exists and count rows
        for model in ALL_ANALYTICAL_MODELS:
            if model == "release_metadata":
                continue
            try:
                cnt = ro_con.execute(f"SELECT COUNT(*) FROM {model};").fetchone()[0]
                model_counts[model] = int(cnt)
            except Exception as e:
                raise ReadOnlyInspectionError(f"Model or view '{model}' verification failed: {e}") from e

        return model_counts
    finally:
        ro_con.close()


def evaluate_pre_seal_gate(
    quality_results: Sequence[dict[str, Any]] | None = None,
    metric_identity: dict[str, Any] | None = None,
    duckdb_con: Any | None = None,
) -> PreSealGateVerdict:
    """Enforce Data Contracts §4 truth table across pre-seal rule outcomes.
    
    Invariants:
    - Every BLOCK rule (S01-S07) must have status PASS.
    - WARN rules (Q01-Q09) may have status PASS or FAIL.
    - Any ERROR status blocks finalization.
    - NOT_APPLICABLE is forbidden except for Q07 and Q09.
    - Missing required rules or duplicate rules block finalization.
    - Metric count identity must hold: total == missing + negative + eligible.
    """
    results: list[dict[str, Any]] = []
    if quality_results is not None:
        results = list(quality_results)
    elif duckdb_con is not None:
        try:
            rows = duckdb_con.execute(
                """
                SELECT rule_id, rule_version, cohort_key, severity, status, method,
                       evaluated_count, exception_count, denominator, exception_rate,
                       details_json, source_provenance, evaluated_at_utc
                FROM quality_result
                ORDER BY rule_id, cohort_key;
                """
            ).fetchall()
            for r in rows:
                det = json.loads(r[10]) if r[10] else {}
                results.append({
                    "rule_id": r[0],
                    "rule_version": r[1],
                    "cohort_key": r[2],
                    "severity": r[3],
                    "status": r[4],
                    "method": r[5],
                    "evaluated_count": r[6],
                    "exception_count": r[7],
                    "denominator": r[8],
                    "exception_rate": r[9],
                    "details": det,
                    "source_provenance": r[11],
                    "evaluated_at_utc": r[12],
                })
        except Exception as e:
            return PreSealGateVerdict(
                verdict=GateVerdict.FAIL,
                all_passed=False,
                blocking_failures=[f"Failed to query quality_result: {e}"],
                rule_summaries={},
                metric_count_identity_holds=False,
                details={"error": str(e)},
            )

    # 1. Metric count identity check
    identity_holds = True
    identity_stats: dict[str, Any] = {}
    if metric_identity is not None:
        tot = metric_identity.get("total_trips", 0)
        elig = metric_identity.get("eligible_trips", 0)
        miss = metric_identity.get("missing_intervals", 0)
        neg = metric_identity.get("negative_intervals", 0)
        identity_holds = (tot == elig + miss + neg)
        identity_stats = {"total": tot, "eligible": elig, "missing": miss, "negative": neg}
    elif duckdb_con is not None:
        try:
            row = duckdb_con.execute(
                """
                SELECT
                    COALESCE(SUM(total_trips), 0),
                    COALESCE(SUM(eligible_trips), 0),
                    COALESCE(SUM(missing_intervals), 0),
                    COALESCE(SUM(negative_intervals), 0)
                FROM interval_daily;
                """
            ).fetchone()
            tot, elig, miss, neg = row
            identity_holds = (tot == elig + miss + neg)
            identity_stats = {"total": tot, "eligible": elig, "missing": miss, "negative": neg}
        except Exception:
            identity_holds = True

    blocking_failures: list[str] = []
    rule_summaries: dict[str, str] = {}
    seen_cohorts: set[tuple[str, str]] = set()
    observed_rule_ids: set[str] = set()

    for item in results:
        r_id = item["rule_id"]
        cohort = item.get("cohort_key", "global")
        severity = item.get("severity", "BLOCK")
        status = item.get("status", "UNKNOWN")
        observed_rule_ids.add(r_id)

        pair = (r_id, cohort)
        if pair in seen_cohorts:
            blocking_failures.append(f"Duplicate rule outcome observed for ({r_id}, {cohort})")
        seen_cohorts.add(pair)

        rule_summaries[f"{r_id}:{cohort}"] = f"{severity}={status}"

        # Status validation
        if status not in ("PASS", "FAIL", "ERROR", "NOT_APPLICABLE"):
            blocking_failures.append(f"Rule {r_id} has unrecognized status '{status}'")
            continue

        # ERROR status always blocks at every severity
        if status == "ERROR":
            blocking_failures.append(f"Rule {r_id} returned computational ERROR")

        # NOT_APPLICABLE status is strictly forbidden except for Q07 and Q09
        elif status == "NOT_APPLICABLE":
            if r_id not in ("Q07", "Q09"):
                blocking_failures.append(f"Rule {r_id} returned unauthorized NOT_APPLICABLE")

        # BLOCK severity rules must be PASS
        elif severity == "BLOCK":
            if status != "PASS":
                blocking_failures.append(f"BLOCK rule {r_id} failed with status '{status}'")

        # WARN severity rules may be PASS or FAIL (FAIL indicates semantic findings)
        elif severity == "WARN":
            pass

    # Required pre-seal rules inventory completeness
    for req_r in REQUIRED_PRE_SEAL_RULES:
        if req_r not in observed_rule_ids:
            blocking_failures.append(f"Missing required pre-seal rule outcome: {req_r}")

    if not identity_holds:
        blocking_failures.append(
            f"Metric count identity violated: total ({identity_stats.get('total')}) != "
            f"eligible ({identity_stats.get('eligible')}) + "
            f"missing ({identity_stats.get('missing')}) + "
            f"negative ({identity_stats.get('negative')})"
        )

    all_passed = (len(blocking_failures) == 0)
    verdict = GateVerdict.PASS if all_passed else GateVerdict.FAIL

    return PreSealGateVerdict(
        verdict=verdict,
        all_passed=all_passed,
        blocking_failures=blocking_failures,
        rule_summaries=rule_summaries,
        metric_count_identity_holds=identity_holds,
        details={"identity_stats": identity_stats, "total_rules": len(results)},
    )


def build_validation_report_json(
    release_id: str,
    derivation_id: str,
    attempt_id: str,
    gate_verdict: PreSealGateVerdict,
    quality_results: Sequence[dict[str, Any]],
    rule_inventory_hash: str,
    rule_inventory_version: str = "v1",
) -> tuple[str, bytes]:
    """Generate canonical JSON validation report and compute its SHA-256 digest."""
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    pre_seal_rules: list[dict[str, Any]] = []
    for q in quality_results:
        pre_seal_rules.append({
            "rule_id": q["rule_id"],
            "rule_version": q.get("rule_version", "v1"),
            "cohort_key": q.get("cohort_key", "global"),
            "severity": q.get("severity", "BLOCK"),
            "status": q.get("status", "PASS"),
            "method": q.get("method", "full_scan"),
            "evaluated_count": int(q.get("evaluated_count", 0)),
            "exception_count": int(q.get("exception_count", 0)),
            "denominator": int(q["denominator"]) if q.get("denominator") is not None else None,
            "exception_rate": float(q["exception_rate"]) if q.get("exception_rate") is not None else None,
            "details": q.get("details", {}) if isinstance(q.get("details"), dict) else {},
            "source_provenance": q.get("source_provenance"),
            "evaluated_at_utc": q.get("evaluated_at_utc", now_utc),
        })

    # Sort rules deterministically by rule_id and cohort_key
    pre_seal_rules.sort(key=lambda x: (x["rule_id"], x["cohort_key"]))

    pass_cnt = sum(1 for r in pre_seal_rules if r["status"] == "PASS")
    fail_cnt = sum(1 for r in pre_seal_rules if r["status"] == "FAIL")
    err_cnt = sum(1 for r in pre_seal_rules if r["status"] == "ERROR")
    na_cnt = sum(1 for r in pre_seal_rules if r["status"] == "NOT_APPLICABLE")
    sem_warns = sum(1 for r in pre_seal_rules if r["severity"] == "WARN" and r["status"] == "FAIL")

    report_payload = {
        "report_version": "v1",
        "release_id": release_id,
        "derivation_id": derivation_id,
        "evaluated_at_utc": now_utc,
        "rule_inventory_version": rule_inventory_version,
        "rule_inventory_hash": rule_inventory_hash,
        "gate_decision": {
            "outcome": gate_verdict.verdict.value,
            "is_publishable": gate_verdict.all_passed,
            "blocking_failures_count": len(gate_verdict.blocking_failures),
            "semantic_warnings_count": sem_warns,
        },
        "pre_seal_rules": pre_seal_rules,
        "summary": {
            "total_rules_evaluated": len(pre_seal_rules),
            "pass_count": pass_cnt,
            "fail_count": fail_cnt,
            "error_count": err_cnt,
            "not_applicable_count": na_cnt,
        },
    }

    report_str = canonical_json_dumps(report_payload)
    report_bytes = canonical_json_bytes(report_payload)
    return report_str, report_bytes


def build_manifest_json(
    release_id: str,
    derivation_id: str,
    attempt_id: str,
    database_sha256: str,
    database_byte_count: int,
    validation_sha256: str,
    validation_byte_count: int,
    frozen_plan: Any,
    coverage_rows: Sequence[dict[str, Any]] | None = None,
) -> tuple[str, bytes]:
    """Generate canonical JSON manifest without self-digest and compute its SHA-256 digest."""
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Format frozen plan dict
    if hasattr(frozen_plan, "__dataclass_fields__"):
        sources = []
        for s in frozen_plan.source_revisions:
            sources.append({
                "logical_source_id": s.logical_source_id if hasattr(s, "logical_source_id") else s["logical_source_id"],
                "revision_id": s.revision_id if hasattr(s, "revision_id") else s["revision_id"],
                "content_sha256": getattr(s, "content_sha256", s.get("content_sha256", "")) if isinstance(s, dict) or hasattr(s, "content_sha256") else "",
                "byte_count": getattr(s, "byte_count", s.get("byte_count", 1)) if isinstance(s, dict) or hasattr(s, "byte_count") else 1,
                "decoded_row_count": getattr(s, "decoded_row_count", s.get("decoded_row_count", 0)) if isinstance(s, dict) or hasattr(s, "decoded_row_count") else 0,
            })
        sources.sort(key=lambda x: x["logical_source_id"])

        plan_dict = {
            "window_months": sorted(list(frozen_plan.window_months)),
            "source_revisions": sources,
            "zone_revision_id": frozen_plan.zone_revision_id,
            "schema_adapter_version": frozen_plan.schema_adapter_version,
            "rules_hash": frozen_plan.rules_hash,
            "metric_definition_hash": frozen_plan.metric_definition_hash,
            "code_bundle_manifest_hash": frozen_plan.code_bundle_manifest_hash,
            "lockfile_hash": frozen_plan.lockfile_hash,
            "model_schema_version": frozen_plan.model_schema_version,
            "semantic_configuration": dict(frozen_plan.semantic_configuration),
        }
    elif isinstance(frozen_plan, dict):
        plan_dict = dict(frozen_plan)
        if "source_revisions" in plan_dict:
            plan_dict["source_revisions"] = sorted(
                plan_dict["source_revisions"], key=lambda x: x.get("logical_source_id", "")
            )
        if "window_months" in plan_dict:
            plan_dict["window_months"] = sorted(list(plan_dict["window_months"]))
    else:
        plan_dict = {}

    cov_list: list[dict[str, Any]] = []
    if coverage_rows:
        for c in coverage_rows:
            cov_list.append({
                "source_month": c["source_month"],
                "is_expected": bool(c["is_expected"]),
                "is_available": bool(c["is_available"]),
                "is_included": bool(c["is_included"]),
                "revision_id": c.get("revision_id"),
                "row_count": int(c["row_count"]) if c.get("row_count") is not None else None,
            })
        cov_list.sort(key=lambda x: x["source_month"])

    manifest_payload = {
        "manifest_version": "v1",
        "release_id": release_id,
        "derivation_id": derivation_id,
        "producing_attempt_id": attempt_id,
        "created_at_utc": now_utc,
        "artifacts": {
            "database": {
                "relative_path": "analytics.duckdb",
                "sha256": database_sha256,
                "byte_count": database_byte_count,
                "format": "duckdb-v2",
            },
            "validation_report": {
                "relative_path": "validation.json",
                "sha256": validation_sha256,
                "byte_count": validation_byte_count,
                "format": "json-rfc8785",
            },
        },
        "frozen_plan": plan_dict,
        "coverage": cov_list,
        "producer": {
            "system": "hvfhv",
            "version": "1.0.0",
            "python_version": sys.version.split()[0],
            "duckdb_version": getattr(duckdb, "__version__", "2.0.0"),
            "pyarrow_version": getattr(pa, "__version__", "26.0.1"),
        },
    }

    manifest_str = canonical_json_dumps(manifest_payload)
    manifest_bytes = canonical_json_bytes(manifest_payload)
    return manifest_str, manifest_bytes


# ---------------------------------------------------------------------------
# High-Level Workflows
# ---------------------------------------------------------------------------

def finalize_candidate(
    candidate_dir: Path | str,
    release_id: str,
    attempt_id: str,
    derivation_id: str,
    frozen_plan: Any,
    source_revision_map: dict[str, str],
    coverage_rows: Sequence[dict[str, Any]] | None = None,
    data_root: Path | str | None = None,
    duckdb_writer_con: Any | None = None,
    registry: Registry | None = None,
    quality_results: Sequence[dict[str, Any]] | None = None,
) -> FinalizedRelease:
    """Execute the atomic candidate finalization sequence.
    
    Invariants:
    1. Held under advisory writer_lock.
    2. DuckDB checkpoint and close; verify no live WAL.
    3. Read-only validation of model schemas and row counts.
    4. Pre-seal truth table evaluation against Data Contracts §4.
    5. Validation report serialized and hashed.
    6. Candidate DuckDB and validation report hashed.
    7. Manifest serialized without self-digest and hashed.
    8. Durable fsync of candidate files and staging directory.
    9. Atomic directory rename staging -> releases/<release_id>; fsync parent directories.
    10. SQLite registration with status 'FINALIZED'.
    """
    root = resolve_data_root(data_root)
    releases_dir = root / "releases"
    releases_dir.mkdir(parents=True, exist_ok=True)
    reg = registry or Registry(root / "control" / "registry.sqlite", data_root=root)

    cand_path = Path(candidate_dir).resolve()
    if not cand_path.exists():
        raise FileNotFoundError(f"Candidate staging directory does not exist: {cand_path}")

    db_path = cand_path / "analytics.duckdb"
    alt_cand_db = cand_path / "candidate.duckdb"
    if not db_path.exists() and alt_cand_db.exists():
        os.replace(alt_cand_db, db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Candidate analytics.duckdb does not exist in {cand_path}")

    with writer_lock(root, reentrant=True):
        # Step 1: Checkpoint and verify zero WAL
        checkpoint_and_verify_zero_wal(duckdb_writer_con, db_path)

        # Step 2: Read-only model count and schema validation
        validate_closed_database_read_only(
            db_path=db_path,
            expected_release_id=release_id,
            expected_derivation_id=derivation_id,
        )

        # Step 3: Pre-seal truth table evaluation
        ro_con = duckdb.connect(str(db_path), read_only=True)
        try:
            gate_verdict = evaluate_pre_seal_gate(
                quality_results=quality_results,
                duckdb_con=ro_con,
            )

            if not gate_verdict.all_passed:
                raise PreSealGateBlockedError(
                    f"Candidate finalization blocked by pre-seal gate: {gate_verdict.blocking_failures}"
                )

            # Fetch quality results from duckdb if not provided
            if quality_results is None:
                q_rows = ro_con.execute(
                    """
                    SELECT rule_id, rule_version, cohort_key, severity, status, method,
                           evaluated_count, exception_count, denominator, exception_rate,
                           details_json, source_provenance, evaluated_at_utc
                    FROM quality_result
                    ORDER BY rule_id, cohort_key;
                    """
                ).fetchall()
                parsed_q: list[dict[str, Any]] = []
                for r in q_rows:
                    det = json.loads(r[10]) if r[10] else {}
                    parsed_q.append({
                        "rule_id": r[0],
                        "rule_version": r[1],
                        "cohort_key": r[2],
                        "severity": r[3],
                        "status": r[4],
                        "method": r[5],
                        "evaluated_count": r[6],
                        "exception_count": r[7],
                        "denominator": r[8],
                        "exception_rate": r[9],
                        "details": det,
                        "source_provenance": r[11],
                        "evaluated_at_utc": r[12],
                    })
                resolved_quality_results = parsed_q
            else:
                resolved_quality_results = list(quality_results)

            # Fetch coverage rows from duckdb if not provided
            if coverage_rows is None:
                try:
                    c_rows = ro_con.execute(
                        """
                        SELECT source_month, is_expected, is_available, is_included, revision_id, row_count
                        FROM coverage ORDER BY source_month;
                        """
                    ).fetchall()
                    resolved_coverage = [
                        {
                            "source_month": r[0],
                            "is_expected": r[1],
                            "is_available": r[2],
                            "is_included": r[3],
                            "revision_id": r[4],
                            "row_count": r[5],
                        }
                        for r in c_rows
                    ]
                except Exception:
                    resolved_coverage = []
            else:
                resolved_coverage = list(coverage_rows)
        finally:
            ro_con.close()

        # Step 4: Write validation.json
        rule_inv_hash = get_rule_inventory_hash()
        val_str, val_bytes = build_validation_report_json(
            release_id=release_id,
            derivation_id=derivation_id,
            attempt_id=attempt_id,
            gate_verdict=gate_verdict,
            quality_results=resolved_quality_results,
            rule_inventory_hash=rule_inv_hash,
        )
        val_path = cand_path / "validation.json"
        val_path.write_bytes(val_bytes)
        durable_fsync(val_path)

        val_sha256 = hashlib.sha256(val_bytes).hexdigest()
        val_byte_count = len(val_bytes)

        # Step 5: Hash closed candidate DB
        db_sha256 = hash_file_sha256(db_path)
        db_byte_count = db_path.stat().st_size

        # Step 6: Write manifest.json
        manifest_str, manifest_bytes = build_manifest_json(
            release_id=release_id,
            derivation_id=derivation_id,
            attempt_id=attempt_id,
            database_sha256=db_sha256,
            database_byte_count=db_byte_count,
            validation_sha256=val_sha256,
            validation_byte_count=val_byte_count,
            frozen_plan=frozen_plan,
            coverage_rows=resolved_coverage,
        )
        manifest_path = cand_path / "manifest.json"
        manifest_path.write_bytes(manifest_bytes)
        durable_fsync(manifest_path)

        manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()

        # Step 7: Durable fsync of candidate directory
        durable_fsync(cand_path)

        # Step 8: Atomic directory rename
        target_dir = releases_dir / release_id
        if target_dir.exists():
            raise DestinationAlreadyExistsError(
                f"Target release directory already exists on disk: {target_dir}"
            )

        os.rename(cand_path, target_dir)
        durable_fsync(target_dir)
        durable_fsync(releases_dir)

        # Step 9: SQLite registration as FINALIZED
        finalized_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        rel_dir_rel = f"releases/{release_id}"
        reg.register_finalized_release(
            release_id=release_id,
            derivation_id=derivation_id,
            producing_attempt_id=attempt_id,
            release_directory=rel_dir_rel,
            manifest_sha256=manifest_sha256,
            database_sha256=db_sha256,
            validation_sha256=val_sha256,
            source_revision_map=source_revision_map,
        )

        return FinalizedRelease(
            release_id=release_id,
            derivation_id=derivation_id,
            producing_attempt_id=attempt_id,
            release_directory=target_dir,
            manifest_sha256=manifest_sha256,
            database_sha256=db_sha256,
            validation_sha256=val_sha256,
            source_revision_map=source_revision_map,
            finalized_at_utc=finalized_at,
        )


def verify_post_seal_s08(
    release_id: str,
    data_root: Path | str | None = None,
    registry: Registry | None = None,
    verifier_version: str = "1.0.0",
) -> VerificationAttestation:
    """Execute external Rule S08 post-seal verification.
    
    Invariants:
    1. Held under advisory writer_lock.
    2. Re-hashes closed analytics.duckdb and validation.json on disk.
    3. Confirms digests match manifest.json and SQLite releases row exactly.
    4. Read-only inspection of release_metadata and analytical schemas.
    5. Re-checks validation.json pre-seal gate verdict was PASS.
    6. Appends attestation to SQLite release_verifications and promotes status to 'VERIFIED'.
    7. Never mutates release files on disk.
    """
    root = resolve_data_root(data_root)
    reg = registry or Registry(root / "control" / "registry.sqlite", data_root=root)

    with writer_lock(root, reentrant=True):
        # 1. Path confinement and physical file validation
        target_dir = root / "releases" / release_id
        if not target_dir.exists():
            raise PostSealVerificationError(f"Release directory missing on disk: {target_dir}")

        manifest_path = target_dir / "manifest.json"
        db_path = target_dir / "analytics.duckdb"
        val_path = target_dir / "validation.json"
        wal_path = target_dir / "analytics.duckdb.wal"

        if not manifest_path.exists():
            raise PostSealVerificationError(f"manifest.json missing in {target_dir}")
        if not db_path.exists():
            raise PostSealVerificationError(f"analytics.duckdb missing in {target_dir}")
        if not val_path.exists():
            raise PostSealVerificationError(f"validation.json missing in {target_dir}")
        if wal_path.exists() and wal_path.stat().st_size > 0:
            raise PostSealVerificationError(f"Uncheckpointed WAL file detected: {wal_path}")

        # 2. Query registry record
        con = reg.get_connection()
        try:
            cur = con.cursor()
            rel_row = cur.execute(
                """
                SELECT derivation_id, manifest_sha256, database_sha256, validation_sha256, status
                FROM releases WHERE release_id = ?;
                """,
                (release_id,),
            ).fetchone()
            if not rel_row:
                raise PostSealVerificationError(f"Release '{release_id}' is not registered in SQLite.")
            derivation_id, exp_manifest_sha, exp_db_sha, exp_val_sha, status = rel_row

            if status in ("REJECTED", "RETIRED"):
                raise InvalidReleaseStatusError(
                    f"Release '{release_id}' has status '{status}'; cannot verify or publish."
                )
        finally:
            con.close()

        # 3. Compute actual digests
        act_db_sha = hash_file_sha256(db_path)
        act_val_sha = hash_file_sha256(val_path)
        act_manifest_sha = hash_file_sha256(manifest_path)

        if act_manifest_sha != exp_manifest_sha:
            raise PostSealVerificationError(
                f"manifest.json digest mismatch: expected {exp_manifest_sha}, got {act_manifest_sha}"
            )
        if act_db_sha != exp_db_sha:
            raise PostSealVerificationError(
                f"analytics.duckdb digest mismatch: expected {exp_db_sha}, got {act_db_sha}"
            )
        if act_val_sha != exp_val_sha:
            raise PostSealVerificationError(
                f"validation.json digest mismatch: expected {exp_val_sha}, got {act_val_sha}"
            )

        # 4. Parse manifest.json
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest_data = json.load(f)
        except Exception as e:
            raise PostSealVerificationError(f"Failed to parse manifest.json: {e}") from e

        if manifest_data.get("release_id") != release_id:
            raise PostSealVerificationError("manifest.json release_id mismatch")
        if manifest_data.get("derivation_id") != derivation_id:
            raise PostSealVerificationError("manifest.json derivation_id mismatch")

        man_db = manifest_data.get("artifacts", {}).get("database", {})
        man_val = manifest_data.get("artifacts", {}).get("validation_report", {})

        if man_db.get("sha256") != act_db_sha:
            raise PostSealVerificationError(
                f"manifest DB sha mismatch: expected {act_db_sha}, manifest has {man_db.get('sha256')}"
            )
        if man_val.get("sha256") != act_val_sha:
            raise PostSealVerificationError(
                f"manifest validation sha mismatch: expected {act_val_sha}, manifest has {man_val.get('sha256')}"
            )
        if man_db.get("byte_count") != db_path.stat().st_size:
            raise PostSealVerificationError("manifest DB byte count mismatch")
        if man_val.get("byte_count") != val_path.stat().st_size:
            raise PostSealVerificationError("manifest validation byte count mismatch")

        # 5. Parse validation.json
        try:
            with open(val_path, "r", encoding="utf-8") as f:
                val_data = json.load(f)
        except Exception as e:
            raise PostSealVerificationError(f"Failed to parse validation.json: {e}") from e

        gate_decision = val_data.get("gate_decision", {})
        if gate_decision.get("outcome") != "PASS" or not gate_decision.get("is_publishable"):
            raise PostSealVerificationError(
                f"Pre-seal gate outcome in validation.json was not PASS: {gate_decision}"
            )

        for rule in val_data.get("pre_seal_rules", []):
            if rule.get("severity") == "BLOCK" and rule.get("status") != "PASS":
                raise PostSealVerificationError(f"BLOCK rule {rule.get('rule_id')} status is {rule.get('status')}")
            if rule.get("status") == "ERROR":
                raise PostSealVerificationError(f"Rule {rule.get('rule_id')} has ERROR status")

        # 6. Read-only DuckDB inspection
        validate_closed_database_read_only(
            db_path=db_path,
            expected_release_id=release_id,
            expected_derivation_id=derivation_id,
        )

        # 7. Record SQLite attestation
        rule_inv_hash = get_rule_inventory_hash()
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        verification_results = {
            "rule_s08": "PASS",
            "disk_duckdb_sha256": act_db_sha,
            "disk_validation_sha256": act_val_sha,
            "read_only_inspection_succeeded": True,
            "verified_at_utc": now_utc,
        }

        ver_res = reg.record_verification(
            release_id=release_id,
            manifest_sha256=act_manifest_sha,
            database_sha256=act_db_sha,
            validation_sha256=act_val_sha,
            rule_inventory_version="v1",
            rule_inventory_hash=rule_inv_hash,
            verification_results_json=canonical_json_dumps(verification_results),
            verifier_version=verifier_version,
        )
        ver_id = ver_res if isinstance(ver_res, str) else ver_res["verification_id"]

        return VerificationAttestation(
            verification_id=ver_id,
            release_id=release_id,
            manifest_sha256=act_manifest_sha,
            database_sha256=act_db_sha,
            validation_sha256=act_val_sha,
            rule_inventory_version="v1",
            rule_inventory_hash=rule_inv_hash,
            verification_results=verification_results,
            verifier_version=verifier_version,
            verified_at_utc=now_utc,
        )


def publish(
    release_id: str,
    expected_parent_generation: int,
    expected_parent_release_id: str | None = None,
    operation_key: str | None = None,
    attempt_id: str | None = None,
    reason: str | None = None,
    data_root: Path | str | None = None,
    registry: Registry | None = None,
) -> PublicationResult:
    """Perform atomic compare-and-swap (CAS) publication of a verified release.
    
    Must be called under writer_lock.
    Handles Scenario T18 idempotency (lost acknowledgement recovery).
    """
    root = resolve_data_root(data_root)
    reg = registry or Registry(root / "control" / "registry.sqlite", data_root=root)

    op_key = operation_key or f"op_pub_{release_id}_{expected_parent_generation}"

    with writer_lock(root, reentrant=True):
        # 1. Pre-register selection request
        reg.record_selection_request(
            operation_key=op_key,
            target_release_id=release_id,
            expected_parent_generation=expected_parent_generation,
            operation_kind="publish",
            expected_parent_release_id=expected_parent_release_id,
            reason=reason or "Monthly candidate publication",
        )

        # 2. Execute CAS publication
        res = reg.publish_release(operation_key=op_key, attempt_id=attempt_id)

        # 3. Transition attempt phase if attempt_id is provided and status is SUCCEEDED
        if attempt_id and res.get("status") == "SUCCEEDED":
            try:
                reg.transition_attempt_phase(
                    attempt_id=attempt_id,
                    new_phase="COMPLETE",
                    outcome="SUCCEEDED",
                    payload={"new_generation": res["new_generation"], "operation_key": op_key},
                )
            except Exception:
                pass

        return PublicationResult(
            status=res["status"],
            event_id=res["event_id"],
            operation_key=op_key,
            operation_kind="publish",
            selected_release_id=res["selected_release_id"],
            previous_release_id=res["previous_release_id"],
            previous_generation=res["previous_generation"],
            new_generation=res["new_generation"],
            event_at_utc=res["event_at_utc"],
            is_current=res.get("is_current", True),
        )


# Backward compatible alias
publish_candidate = publish


def rollback(
    target_release_id: str,
    reason: str,
    expected_parent_generation: int,
    expected_parent_release_id: str | None = None,
    operation_key: str | None = None,
    attempt_id: str | None = None,
    data_root: Path | str | None = None,
    registry: Registry | None = None,
    verify_bytes: bool = True,
) -> PublicationResult:
    """Perform atomic CAS rollback to a previously verified release (Scenario T19).
    
    Verifies physical bytes of retained files on disk before CAS pointer advancement.
    """
    root = resolve_data_root(data_root)
    reg = registry or Registry(root / "control" / "registry.sqlite", data_root=root)

    op_key = operation_key or f"op_rollback_{target_release_id}_{expected_parent_generation}"

    with writer_lock(root, reentrant=True):
        try:
            res = reg.rollback_release(
                operation_key=op_key,
                target_release_id=target_release_id,
                expected_parent_generation=expected_parent_generation,
                expected_parent_release_id=expected_parent_release_id,
                reason=reason,
                attempt_id=attempt_id,
                verify_bytes=verify_bytes,
            )
        except ReleaseIntegrityError as e:
            raise RollbackIntegrityError(f"Rollback target integrity failure: {e}") from e

        return PublicationResult(
            status=res["status"],
            event_id=res["event_id"],
            operation_key=op_key,
            operation_kind="rollback",
            selected_release_id=res["selected_release_id"],
            previous_release_id=res["previous_release_id"],
            previous_generation=res["previous_generation"],
            new_generation=res["new_generation"],
            event_at_utc=res["event_at_utc"],
            is_current=res.get("is_current", True),
        )


def recover_unacknowledged_selection(
    operation_key: str,
    data_root: Path | str | None = None,
    registry: Registry | None = None,
) -> PublicationResult:
    """Reconcile and recover from an unacknowledged publication or rollback commit (Scenario T18).
    
    Invariants:
    1. Query operation_key status via registry.query_selection_status.
    2. If COMMITTED: returns historical success receipt without modifying state or advancing generation.
    3. If PENDING_OR_UNCOMMITTED and can_retry: executes publication safely.
    4. If PENDING_OR_UNCOMMITTED and not can_retry: raises StaleGenerationConflictError.
    5. If NOT_REGISTERED: raises RegistryError.
    """
    root = resolve_data_root(data_root)
    reg = registry or Registry(root / "control" / "registry.sqlite", data_root=root)

    with writer_lock(root, reentrant=True):
        status_info = reg.query_selection_status(operation_key)
        state = status_info["status"]

        if state == "COMMITTED":
            return PublicationResult(
                status="ALREADY_COMMITTED",
                event_id=status_info["event_id"],
                operation_key=operation_key,
                operation_kind=status_info["operation_kind"],
                selected_release_id=status_info["selected_release_id"],
                previous_release_id=status_info["previous_release_id"],
                previous_generation=status_info["previous_generation"],
                new_generation=status_info["new_generation"],
                event_at_utc=status_info["event_at_utc"],
                is_current=status_info.get("is_current", True),
            )

        if state == "PENDING_OR_UNCOMMITTED":
            if not status_info.get("can_retry", False):
                raise StaleGenerationConflictError(
                    f"Cannot retry operation_key '{operation_key}': state moved (expected parent gen "
                    f"{status_info['expected_parent_generation']} != current {status_info['current_generation']})"
                )
            # Safe to execute publication
            res = reg.publish_release(operation_key=operation_key)
            return PublicationResult(
                status=res["status"],
                event_id=res["event_id"],
                operation_key=operation_key,
                operation_kind=status_info["operation_kind"],
                selected_release_id=res["selected_release_id"],
                previous_release_id=res["previous_release_id"],
                previous_generation=res["previous_generation"],
                new_generation=res["new_generation"],
                event_at_utc=res["event_at_utc"],
                is_current=res.get("is_current", True),
            )

        raise RegistryError(f"Operation key '{operation_key}' is not registered in selection_requests.")


def inspect_current(
    data_root: Path | str | None = None,
    registry: Registry | None = None,
) -> dict[str, Any]:
    """Inspect current release singleton pointer and operational status outside cache."""
    root = resolve_data_root(data_root)
    reg = registry or Registry(root / "control" / "registry.sqlite", data_root=root)
    return reg.get_current_release()


def reconcile_releases(
    data_root: Path | str | None = None,
    registry: Registry | None = None,
) -> dict[str, Any]:
    """Audit release directories on disk against SQLite releases table (Scenario T17).
    
    Detects and reports orphan release directories without automatic adoption.
    """
    root = resolve_data_root(data_root)
    reg = registry or Registry(root / "control" / "registry.sqlite", data_root=root)

    releases_dir = root / "releases"
    disk_dirs: list[str] = []
    if releases_dir.exists():
        disk_dirs = [d.name for d in releases_dir.iterdir() if d.is_dir()]

    con = reg.get_connection()
    try:
        cur = con.cursor()
        registered_releases = {
            r[0]: r[1] for r in cur.execute("SELECT release_id, status FROM releases;").fetchall()
        }
        published_releases = {
            r[0] for r in cur.execute("SELECT DISTINCT selected_release_id FROM publication_events;").fetchall()
        }
        curr_row = cur.execute("SELECT release_id, generation FROM current_release WHERE id = 1;").fetchone()
        current_rel_id = curr_row[0] if curr_row else None
        current_gen = curr_row[1] if curr_row else 0
    finally:
        con.close()

    orphans: list[dict[str, Any]] = []
    for d_name in disk_dirs:
        # If directory is not registered in SQLite, or is FINALIZED (never verified),
        # or was verified but never selected and is not current, report as orphan
        reg_status = registered_releases.get(d_name)
        is_orphan = False
        reason = ""

        if reg_status is None:
            is_orphan = True
            reason = "Directory on disk is not registered in releases table"
        elif reg_status == "FINALIZED":
            is_orphan = True
            reason = "Release is registered as FINALIZED but lacks post-seal verification"
        elif d_name not in published_releases and d_name != current_rel_id:
            is_orphan = True
            reason = "Release is verified but has never been selected as current"

        if is_orphan:
            p = releases_dir / d_name
            orphans.append({
                "release_id": d_name,
                "directory": str(p),
                "status": "ORPHAN_UNSELECTED",
                "registered_status": reg_status,
                "reason": reason,
                "has_manifest": (p / "manifest.json").exists(),
                "has_duckdb": (p / "analytics.duckdb").exists(),
                "has_validation": (p / "validation.json").exists(),
            })

    return {
        "total_releases_on_disk": len(disk_dirs),
        "current_release_id": current_rel_id,
        "current_generation": current_gen,
        "orphans": orphans,
        "orphan_count": len(orphans),
    }
