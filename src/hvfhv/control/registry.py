"""SQLite Control Plane and Migration Registry for HVFHV.

Implements the 15 control relations, durability pragmas, versioned migrations
with SQLite Online Backup API, atomic compare-and-swap (CAS) publication,
idempotent operation keys (Scenario T18), and backup/restore recovery (Scenario T24).

Adheres to:
- docs/architecture/low-level-design.md §3, §6
- docs/reference/interfaces-and-operations.md §1, §2, §3
- docs/requirements.md (R13, R14, R15, R16, R21)
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, NamedTuple
import uuid

from hvfhv.config import resolve_data_root
from hvfhv.control.locks import writer_lock, is_lock_held_by_current_thread


# Mandatory SQLite Durability PRAGMA settings on macOS APFS
SQLITE_BUSY_TIMEOUT_MS = 5000


def configure_sqlite_connection(con: sqlite3.Connection) -> dict[str, int | str]:
    """Configure connection-local durability pragmas and verify active settings.
    
    Invariants:
    - foreign_keys = ON (1)
    - journal_mode = DELETE ('delete')
    - synchronous = EXTRA (3)
    - fullfsync = ON (1) on macOS APFS
    - busy_timeout = 5000 ms
    """
    cur = con.cursor()
    try:
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("PRAGMA journal_mode = DELETE;")
        cur.execute("PRAGMA synchronous = EXTRA;")
        cur.execute("PRAGMA fullfsync = ON;")
        cur.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS};")

        fk = cur.execute("PRAGMA foreign_keys;").fetchone()[0]
        jm = cur.execute("PRAGMA journal_mode;").fetchone()[0]
        sync = cur.execute("PRAGMA synchronous;").fetchone()[0]
        ffsync = cur.execute("PRAGMA fullfsync;").fetchone()[0]
    finally:
        cur.close()

    assert fk == 1, f"Expected foreign_keys=1, got {fk}"
    assert jm.lower() == "delete", f"Expected journal_mode='delete', got {jm}"
    assert sync == 3, f"Expected synchronous=3 (EXTRA), got {sync}"
    assert ffsync == 1, f"Expected fullfsync=1 (ON), got {ffsync}"

    return {
        "foreign_keys": fk,
        "journal_mode": jm.lower(),
        "synchronous": sync,
        "fullfsync": ffsync,
    }


def open_registry_connection(db_path: Path | str) -> sqlite3.Connection:
    """Open and configure a SQLite connection to the control registry."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000.0)
    con.row_factory = sqlite3.Row
    configure_sqlite_connection(con)
    return con


# ---------------------------------------------------------------------------
# Migration DDL Definitions
# ---------------------------------------------------------------------------

MIGRATION_V1_DDL = """
-- Migration 001: Initial control registry schema (15 relations)
-- Tracked by schema_migrations

-- 1. Schema Migrations Tracker
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    migration_name TEXT NOT NULL,
    migration_hash TEXT NOT NULL,
    applied_at_utc TEXT NOT NULL
);

-- 2. Logical Source Objects
CREATE TABLE IF NOT EXISTS source_objects (
    logical_source_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('hvfhv', 'taxi_zones')),
    source_month TEXT,
    official_url TEXT NOT NULL,
    CONSTRAINT check_hvfhv_source_month CHECK (
        (kind = 'hvfhv' AND source_month IS NOT NULL AND source_month GLOB '[12][0-9][0-9][0-9]-[01][0-9]') OR
        (kind != 'hvfhv' AND source_month IS NULL)
    )
);

-- 3. Source Observations (Append-only)
CREATE TABLE IF NOT EXISTS source_observations (
    observation_id TEXT PRIMARY KEY,
    logical_source_id TEXT NOT NULL REFERENCES source_objects(logical_source_id),
    observed_at_utc TEXT NOT NULL,
    http_method TEXT NOT NULL CHECK (http_method IN ('GET', 'HEAD')),
    http_status INTEGER,
    etag TEXT,
    last_modified TEXT,
    advertised_bytes INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN ('AVAILABLE', 'UNAVAILABLE', 'FAILED', 'ACCEPTED', 'REUSED')),
    revision_id TEXT REFERENCES source_revisions(revision_id)
);
CREATE INDEX IF NOT EXISTS idx_source_observations_lookup 
ON source_observations(logical_source_id, observed_at_utc);

-- 4. Source Revisions (Immutable, unique content digest per logical source)
CREATE TABLE IF NOT EXISTS source_revisions (
    revision_id TEXT PRIMARY KEY,
    logical_source_id TEXT NOT NULL REFERENCES source_objects(logical_source_id),
    content_sha256 TEXT NOT NULL,
    byte_count INTEGER NOT NULL CHECK (byte_count > 0),
    relative_path TEXT NOT NULL,
    accepted_at_utc TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    footer_row_count INTEGER NOT NULL CHECK (footer_row_count >= 0),
    decoded_row_count INTEGER NOT NULL CHECK (decoded_row_count >= 0),
    provenance_relative_path TEXT NOT NULL,
    UNIQUE (logical_source_id, content_sha256)
);
CREATE INDEX IF NOT EXISTS idx_source_revisions_source 
ON source_revisions(logical_source_id);

-- 5. Build/Pipeline Attempts
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    requested_window_start TEXT,
    requested_window_end TEXT,
    requested_configuration_json TEXT NOT NULL,
    derivation_id TEXT,
    frozen_plan_hash TEXT,
    frozen_plan_json TEXT,
    expected_parent_release_id TEXT REFERENCES releases(release_id),
    expected_parent_generation INTEGER,
    current_phase TEXT NOT NULL CHECK (current_phase IN ('PLANNED', 'ACQUIRING', 'INPUTS_READY', 'BUILDING', 'VALIDATING', 'FINALIZING', 'SELECTING', 'COMPLETE')),
    outcome TEXT NOT NULL CHECK (outcome IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'INTERRUPTED', 'CONFLICT')),
    started_at_utc TEXT NOT NULL,
    ended_at_utc TEXT,
    structured_error_json TEXT,
    dagster_run_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_attempts_phase_outcome 
ON attempts(current_phase, outcome);

-- 6. Attempt Phase Events (Append-only)
CREATE TABLE IF NOT EXISTS attempt_events (
    attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
    sequence INTEGER NOT NULL,
    previous_phase TEXT,
    new_phase TEXT NOT NULL,
    event_at_utc TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT,
    PRIMARY KEY (attempt_id, sequence)
);

-- 7. Sealed Releases (Immutable artifacts)
CREATE TABLE IF NOT EXISTS releases (
    release_id TEXT PRIMARY KEY,
    derivation_id TEXT NOT NULL,
    producing_attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
    release_directory TEXT NOT NULL UNIQUE,
    manifest_sha256 TEXT NOT NULL,
    database_sha256 TEXT NOT NULL,
    validation_sha256 TEXT NOT NULL,
    finalized_at_utc TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('FINALIZED', 'VERIFIED', 'REJECTED', 'RETIRED'))
);
CREATE INDEX IF NOT EXISTS idx_releases_status ON releases(status);
CREATE INDEX IF NOT EXISTS idx_releases_derivation ON releases(derivation_id);

-- 8. Release Sources (1 revision per logical source per release)
CREATE TABLE IF NOT EXISTS release_sources (
    release_id TEXT NOT NULL REFERENCES releases(release_id),
    logical_source_id TEXT NOT NULL REFERENCES source_objects(logical_source_id),
    revision_id TEXT NOT NULL REFERENCES source_revisions(revision_id),
    PRIMARY KEY (release_id, logical_source_id)
);
CREATE INDEX IF NOT EXISTS idx_release_sources_revision ON release_sources(revision_id);

-- 9. Post-Seal Release Verifications (Append-only external attestations)
CREATE TABLE IF NOT EXISTS release_verifications (
    verification_id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES releases(release_id),
    manifest_sha256 TEXT NOT NULL,
    database_sha256 TEXT NOT NULL,
    validation_sha256 TEXT NOT NULL,
    rule_inventory_version TEXT NOT NULL,
    rule_inventory_hash TEXT NOT NULL,
    verification_results_json TEXT NOT NULL,
    verifier_version TEXT NOT NULL,
    verified_at_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_release_verifications_release ON release_verifications(release_id);

-- 10. Current Release Singleton Pointer (Atomic CAS target)
CREATE TABLE IF NOT EXISTS current_release (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    release_id TEXT REFERENCES releases(release_id),
    generation INTEGER NOT NULL CHECK (generation >= 0),
    updated_at_utc TEXT NOT NULL
);

-- 11. Publication Events (Atomic audit log of release pointer movements)
CREATE TABLE IF NOT EXISTS publication_events (
    event_id TEXT PRIMARY KEY,
    operation_key TEXT NOT NULL UNIQUE,
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('publish', 'rollback')),
    previous_release_id TEXT REFERENCES releases(release_id),
    selected_release_id TEXT NOT NULL REFERENCES releases(release_id),
    previous_generation INTEGER NOT NULL,
    new_generation INTEGER NOT NULL CHECK (new_generation > previous_generation),
    attempt_id TEXT REFERENCES attempts(attempt_id),
    reason TEXT,
    event_at_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_publication_events_selected ON publication_events(selected_release_id);

-- 12. Selection Requests (Idempotency and conflict pre-registration)
CREATE TABLE IF NOT EXISTS selection_requests (
    operation_key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    request_json TEXT NOT NULL,
    target_release_id TEXT NOT NULL REFERENCES releases(release_id),
    expected_parent_release_id TEXT REFERENCES releases(release_id),
    expected_parent_generation INTEGER NOT NULL,
    operation_kind TEXT NOT NULL CHECK (operation_kind IN ('publish', 'rollback')),
    reason TEXT,
    created_at_utc TEXT NOT NULL
);

-- 13. Release Status Events (Audit trail for status changes: VERIFIED, REJECTED, RETIRED)
CREATE TABLE IF NOT EXISTS release_status_events (
    event_id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES releases(release_id),
    previous_status TEXT NOT NULL,
    new_status TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence_reference TEXT,
    event_at_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_release_status_events_release ON release_status_events(release_id);

-- 14. Cleanup Requests (Reviewed operator cleanup intentions)
CREATE TABLE IF NOT EXISTS cleanup_requests (
    cleanup_id TEXT PRIMARY KEY,
    preview_digest TEXT NOT NULL,
    preview_json TEXT NOT NULL,
    expected_generation INTEGER NOT NULL,
    protected_set_digest TEXT NOT NULL,
    protected_set_json TEXT NOT NULL,
    review_reference TEXT NOT NULL,
    created_at_utc TEXT NOT NULL
);

-- 15. Cleanup Items (Per-item tracking and audit receipts)
CREATE TABLE IF NOT EXISTS cleanup_items (
    cleanup_id TEXT NOT NULL REFERENCES cleanup_requests(cleanup_id),
    item_id TEXT NOT NULL,
    exact_relative_path TEXT NOT NULL,
    artifact_hash TEXT,
    artifact_identity TEXT,
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    outcome TEXT NOT NULL CHECK (outcome IN ('PREPARED', 'DELETED', 'FAILED', 'SKIPPED')),
    diagnostic TEXT,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (cleanup_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_cleanup_items_outcome ON cleanup_items(cleanup_id, outcome);

-- Seed singleton current_release pointer if not already present
INSERT OR IGNORE INTO current_release (id, release_id, generation, updated_at_utc)
VALUES (1, NULL, 0, '1970-01-01T00:00:00Z');
"""


class Migration(NamedTuple):
    version: int
    name: str
    ddl_sql: str

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.ddl_sql.strip().encode("utf-8")).hexdigest()


# Ordered versioned migrations list
MIGRATIONS: list[Migration] = [
    Migration(
        version=1,
        name="001_initial_15_control_relations",
        ddl_sql=MIGRATION_V1_DDL,
    ),
]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class RegistryError(Exception):
    """Base exception for all registry control plane errors."""
    pass


class MigrationError(RegistryError):
    """Raised on schema migration failure."""
    pass


class MigrationDowngradeError(MigrationError):
    """Raised when an unsupported schema downgrade is attempted."""
    pass


class BackupIntegrityError(RegistryError):
    """Raised when pre-migration backup or restore verification fails."""
    pass


class SelectionConflictError(RegistryError):
    """Raised when an operation key is reused with different request parameters."""
    pass


class StaleGenerationConflictError(RegistryError):
    """Raised when expected parent release or generation does not match current."""
    pass


class InvalidReleaseStatusError(RegistryError):
    """Raised when release status prevents requested operation (e.g. publishing unverified/rejected)."""
    pass


class ReleaseIntegrityError(RegistryError):
    """Raised when release artifact files on disk fail SHA-256 or structural checks."""
    pass


class RollbackError(RegistryError):
    """Raised when a rollback operation cannot be completed."""
    pass


# ---------------------------------------------------------------------------
# Registry Class
# ---------------------------------------------------------------------------

class Registry:
    """Embedded SQLite Control Plane Registry.
    
    Manages all 15 control relations, durability pragmas, versioned migrations
    with SQLite Online Backup API, and atomic CAS publication.
    """

    def __init__(
        self,
        db_path: Path | str,
        data_root: Path | str | None = None,
        auto_migrate: bool = True,
    ) -> None:
        self.db_path = Path(db_path).resolve()
        if data_root is not None:
            self.data_root = Path(data_root).resolve()
        else:
            # Fall back to parent's parent (.data) or resolved data root
            self.data_root = self.db_path.parent.parent
        self.backup_dir = self.db_path.parent / "backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        if auto_migrate:
            self.migrate()

    def get_connection(self) -> sqlite3.Connection:
        """Get a configured SQLite connection with durability pragmas."""
        return open_registry_connection(self.db_path)

    # -----------------------------------------------------------------------
    # Migration & Backup / Restore Engine
    # -----------------------------------------------------------------------

    def get_current_schema_version(self, con: sqlite3.Connection | None = None) -> int:
        """Return the current schema migration version recorded in schema_migrations."""
        close_con = False
        if con is None:
            if not self.db_path.exists():
                return 0
            con = self.get_connection()
            close_con = True

        try:
            cur = con.cursor()
            try:
                table_check = cur.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations';"
                ).fetchone()[0]
                if table_check == 0:
                    return 0
                row = cur.execute("SELECT max(version) FROM schema_migrations;").fetchone()
                if row is None or row[0] is None:
                    return 0
                return int(row[0])
            finally:
                cur.close()
        finally:
            if close_con:
                con.close()

    def create_online_backup(
        self,
        target_path: Path | str | None = None,
        source_con: sqlite3.Connection | None = None,
    ) -> Path:
        """Create a consistent point-in-time backup via SQLite Online Backup API.
        
        Verifies backup integrity and foreign key constraints before returning.
        """
        if target_path is None:
            now_str = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            v = self.get_current_schema_version(source_con)
            target_path = self.backup_dir / f"registry.sqlite.bak.{now_str}_v{v}.sqlite"
        else:
            target_path = Path(target_path).resolve()

        target_path.parent.mkdir(parents=True, exist_ok=True)

        close_src = False
        if source_con is None:
            source_con = sqlite3.connect(self.db_path)
            close_src = True

        try:
            # Open target backup connection and execute online backup
            dest_con = sqlite3.connect(target_path)
            try:
                source_con.backup(dest_con, pages=-1)
            finally:
                dest_con.close()

            # Verify backup integrity independently
            self._verify_connection_integrity(target_path)
            return target_path
        finally:
            if close_src:
                source_con.close()

    @staticmethod
    def _verify_connection_integrity(db_file: Path) -> None:
        """Run PRAGMA integrity_check and PRAGMA foreign_key_check on a database file."""
        con = sqlite3.connect(db_file)
        try:
            cur = con.cursor()
            try:
                cur.execute("PRAGMA foreign_keys = ON;")
                integrity_rows = cur.execute("PRAGMA integrity_check;").fetchall()
                if not integrity_rows or integrity_rows[0][0] != "ok":
                    raise BackupIntegrityError(f"Integrity check failed on {db_file}: {integrity_rows}")

                fk_rows = cur.execute("PRAGMA foreign_key_check;").fetchall()
                if fk_rows:
                    raise BackupIntegrityError(f"Foreign key check failed on {db_file}: {fk_rows}")
            finally:
                cur.close()
        finally:
            con.close()

    def migrate(self, target_version: int | None = None) -> list[int]:
        """Apply pending migrations up to target_version under writer lock.
        
        Performs pre-migration backup via SQLite Online Backup API, validates backup,
        executes DDL transactionally, verifies integrity and foreign keys before commit.
        Downgrades are strictly forbidden.
        """
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                current_v = self.get_current_schema_version(con)
                max_available = max(m.version for m in MIGRATIONS) if MIGRATIONS else 0
                target_v = target_version if target_version is not None else max_available

                if target_v < current_v:
                    raise MigrationDowngradeError(
                        f"Cannot downgrade registry from version {current_v} to {target_v}. "
                        "No in-place downgrade supported; restore from pre-migration backup."
                    )

                if target_v == current_v:
                    return []

                # Find pending migrations
                pending = [m for m in MIGRATIONS if current_v < m.version <= target_v]
                if not pending:
                    return []

                applied: list[int] = []
                now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

                initial_backup_path: Path | None = None
                if current_v > 0:
                    initial_backup_path = self.create_online_backup(source_con=con)

                for mig in pending:
                    cur = con.cursor()
                    try:
                        cleaned_sql = mig.ddl_sql.strip()
                        cleaned_sql = re.sub(
                            r"^\s*BEGIN(\s+(?:IMMEDIATE|EXCLUSIVE|DEFERRED))?\s*;",
                            "",
                            cleaned_sql,
                            flags=re.IGNORECASE,
                        ).strip()
                        cleaned_sql = re.sub(
                            r";\s*COMMIT(\s+TRANSACTION)?\s*;?\s*$",
                            ";",
                            cleaned_sql,
                            flags=re.IGNORECASE,
                        ).strip()

                        cur.executescript(f"BEGIN IMMEDIATE;\n{cleaned_sql}")
                        cur.execute(
                            """
                            INSERT INTO schema_migrations (version, migration_name, migration_hash, applied_at_utc)
                            VALUES (?, ?, ?, ?);
                            """,
                            (mig.version, mig.name, mig.content_hash, now_utc),
                        )

                        # Integrity check prior to commit
                        integrity = cur.execute("PRAGMA integrity_check;").fetchall()
                        if not integrity or integrity[0][0] != "ok":
                            raise MigrationError(f"Integrity check failed during migration {mig.version}: {integrity}")

                        fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()
                        if fk_violations:
                            raise MigrationError(f"Foreign key violations during migration {mig.version}: {fk_violations}")

                        con.commit()
                        applied.append(mig.version)
                    except Exception as e:
                        try:
                            con.rollback()
                        except Exception:
                            pass
                        try:
                            cur.close()
                        except Exception:
                            pass
                        try:
                            con.close()
                        except Exception:
                            pass
                        if initial_backup_path is not None and initial_backup_path.exists():
                            self.restore_from_backup(initial_backup_path, acquire_lock=False)
                        raise MigrationError(f"Migration version {mig.version} ({mig.name}) failed: {e}") from e
                    else:
                        cur.close()

                return applied
            finally:
                try:
                    con.close()
                except Exception:
                    pass

    def restore_from_backup(self, backup_path: Path | str, acquire_lock: bool = True) -> None:
        """Restore registry from a verified pre-migration backup (Scenario T24).
        
        Invariants:
        1. Acquires OS advisory writer lock (if acquire_lock=True and not already held).
        2. Validates backup integrity and foreign key constraints before attempting restore.
        3. Restores using SQLite Online Backup API.
        4. Validates restored database integrity.
        5. Released artifacts on the filesystem are untouched.
        """
        bak_file = Path(backup_path).resolve()
        if not bak_file.exists():
            raise FileNotFoundError(f"Backup file not found: {bak_file}")

        # Validate backup file integrity first
        self._verify_connection_integrity(bak_file)

        def _do_restore() -> None:
            # Open backup as source, active database as target
            src_con = sqlite3.connect(bak_file)
            target_con = sqlite3.connect(self.db_path)
            try:
                try:
                    configure_sqlite_connection(target_con)
                except sqlite3.DatabaseError:
                    # Target database disk image may be malformed/corrupted prior to restore
                    pass
                # Backup src into target
                src_con.backup(target_con, pages=-1)
                configure_sqlite_connection(target_con)
            finally:
                src_con.close()
                target_con.close()

            # Verify restored active database
            self._verify_connection_integrity(self.db_path)

        if acquire_lock:
            lock_path = self.data_root / "control" / "writer.lock"
            if is_lock_held_by_current_thread(lock_path):
                _do_restore()
            else:
                with writer_lock(self.data_root, reentrant=True):
                    _do_restore()
        else:
            _do_restore()

    # -----------------------------------------------------------------------
    # Source Objects, Observations, and Revisions
    # -----------------------------------------------------------------------

    def register_source_object(
        self,
        logical_source_id: str,
        kind: str,
        official_url: str,
        source_month: str | None = None,
    ) -> None:
        """Register or verify an official source object."""
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(
                    """
                    INSERT INTO source_objects (logical_source_id, kind, source_month, official_url)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(logical_source_id) DO UPDATE SET
                        official_url = excluded.official_url;
                    """,
                    (logical_source_id, kind, source_month, official_url),
                )
                con.commit()
            finally:
                con.close()

    def record_observation(
        self,
        logical_source_id: str,
        http_method: str,
        outcome: str,
        http_status: int | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
        advertised_bytes: int | None = None,
        revision_id: str | None = None,
        observed_at_utc: str | None = None,
    ) -> str:
        """Append an official HTTP source observation record."""
        obs_id = str(uuid.uuid4())
        obs_time = observed_at_utc or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(
                    """
                    INSERT INTO source_observations (
                        observation_id, logical_source_id, observed_at_utc,
                        http_method, http_status, etag, last_modified,
                        advertised_bytes, outcome, revision_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        obs_id, logical_source_id, obs_time, http_method,
                        http_status, etag, last_modified, advertised_bytes,
                        outcome, revision_id,
                    ),
                )
                con.commit()
                return obs_id
            finally:
                con.close()

    def register_revision(
        self,
        revision_id: str,
        logical_source_id: str,
        content_sha256: str,
        byte_count: int,
        relative_path: str,
        schema_fingerprint: str,
        footer_row_count: int,
        decoded_row_count: int,
        provenance_relative_path: str,
        accepted_at_utc: str | None = None,
    ) -> str:
        """Register an accepted source revision. Idempotent on (logical_source_id, content_sha256)."""
        accepted_time = accepted_at_utc or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                # Check for existing
                existing = cur.execute(
                    """
                    SELECT revision_id FROM source_revisions
                    WHERE logical_source_id = ? AND content_sha256 = ?;
                    """,
                    (logical_source_id, content_sha256),
                ).fetchone()
                if existing:
                    con.commit()
                    return str(existing[0])

                cur.execute(
                    """
                    INSERT INTO source_revisions (
                        revision_id, logical_source_id, content_sha256, byte_count,
                        relative_path, accepted_at_utc, schema_fingerprint,
                        footer_row_count, decoded_row_count, provenance_relative_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        revision_id, logical_source_id, content_sha256, byte_count,
                        relative_path, accepted_time, schema_fingerprint,
                        footer_row_count, decoded_row_count, provenance_relative_path,
                    ),
                )
                con.commit()
                return revision_id
            finally:
                con.close()

    # -----------------------------------------------------------------------
    # Build Attempts and Events
    # -----------------------------------------------------------------------

    def create_attempt(
        self,
        attempt_id: str,
        requested_configuration_json: str,
        requested_window_start: str | None = None,
        requested_window_end: str | None = None,
        expected_parent_release_id: str | None = None,
        expected_parent_generation: int | None = None,
        dagster_run_id: str | None = None,
    ) -> None:
        """Create a new build attempt in PLANNED phase and RUNNING outcome."""
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(
                    """
                    INSERT INTO attempts (
                        attempt_id, requested_window_start, requested_window_end,
                        requested_configuration_json, expected_parent_release_id,
                        expected_parent_generation, current_phase, outcome,
                        started_at_utc, dagster_run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, 'PLANNED', 'RUNNING', ?, ?);
                    """,
                    (
                        attempt_id, requested_window_start, requested_window_end,
                        requested_configuration_json, expected_parent_release_id,
                        expected_parent_generation, now_utc, dagster_run_id,
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO attempt_events (
                        attempt_id, sequence, previous_phase, new_phase,
                        event_at_utc, event_type, payload_json
                    ) VALUES (?, 1, NULL, 'PLANNED', ?, 'ATTEMPT_CREATED', NULL);
                    """,
                    (attempt_id, now_utc),
                )
                con.commit()
            finally:
                con.close()

    def transition_attempt_phase(
        self,
        attempt_id: str,
        new_phase: str,
        event_type: str = "PHASE_TRANSITION",
        payload_json: str | None = None,
        derivation_id: str | None = None,
        frozen_plan_hash: str | None = None,
        frozen_plan_json: str | None = None,
    ) -> None:
        """Advance attempt phase and record append-only attempt event."""
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                row = cur.execute(
                    "SELECT current_phase FROM attempts WHERE attempt_id = ?;", (attempt_id,)
                ).fetchone()
                if not row:
                    raise RegistryError(f"Attempt not found: {attempt_id}")
                prev_phase = row[0]

                # Max sequence
                seq_row = cur.execute(
                    "SELECT coalesce(max(sequence), 0) + 1 FROM attempt_events WHERE attempt_id = ?;",
                    (attempt_id,),
                ).fetchone()
                next_seq = seq_row[0]

                # Update attempt
                update_fields = ["current_phase = ?"]
                params: list[Any] = [new_phase]

                if derivation_id is not None:
                    update_fields.append("derivation_id = ?")
                    params.append(derivation_id)
                if frozen_plan_hash is not None:
                    update_fields.append("frozen_plan_hash = ?")
                    params.append(frozen_plan_hash)
                if frozen_plan_json is not None:
                    update_fields.append("frozen_plan_json = ?")
                    params.append(frozen_plan_json)

                params.append(attempt_id)
                cur.execute(
                    f"UPDATE attempts SET {', '.join(update_fields)} WHERE attempt_id = ?;",
                    params,
                )

                cur.execute(
                    """
                    INSERT INTO attempt_events (
                        attempt_id, sequence, previous_phase, new_phase,
                        event_at_utc, event_type, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?);
                    """,
                    (attempt_id, next_seq, prev_phase, new_phase, now_utc, event_type, payload_json),
                )
                con.commit()
            finally:
                con.close()

    def complete_attempt(
        self,
        attempt_id: str,
        outcome: str,
        structured_error_json: str | None = None,
    ) -> None:
        """Mark attempt as completed (SUCCEEDED, FAILED, CANCELLED, INTERRUPTED, CONFLICT)."""
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(
                    """
                    UPDATE attempts
                    SET outcome = ?, ended_at_utc = ?, structured_error_json = ?
                    WHERE attempt_id = ?;
                    """,
                    (outcome, now_utc, structured_error_json, attempt_id),
                )
                con.commit()
            finally:
                con.close()

    # -----------------------------------------------------------------------
    # Releases and Verification Attestations
    # -----------------------------------------------------------------------

    def register_finalized_release(
        self,
        release_id: str,
        derivation_id: str,
        producing_attempt_id: str,
        release_directory: str,
        manifest_sha256: str,
        database_sha256: str,
        validation_sha256: str,
        source_revision_map: dict[str, str],  # logical_source_id -> revision_id
        finalized_at_utc: str | None = None,
    ) -> None:
        """Register a finalized release directory and its constituent source revisions in FINALIZED status."""
        fin_time = finalized_at_utc or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(
                    """
                    INSERT INTO releases (
                        release_id, derivation_id, producing_attempt_id, release_directory,
                        manifest_sha256, database_sha256, validation_sha256,
                        finalized_at_utc, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'FINALIZED');
                    """,
                    (
                        release_id, derivation_id, producing_attempt_id, release_directory,
                        manifest_sha256, database_sha256, validation_sha256,
                        fin_time,
                    ),
                )
                for src_id, rev_id in source_revision_map.items():
                    cur.execute(
                        """
                        INSERT INTO release_sources (release_id, logical_source_id, revision_id)
                        VALUES (?, ?, ?);
                        """,
                        (release_id, src_id, rev_id),
                    )
                cur.execute(
                    """
                    INSERT INTO release_status_events (
                        event_id, release_id, previous_status, new_status, reason, evidence_reference, event_at_utc
                    ) VALUES (?, ?, 'NONE', 'FINALIZED', 'Candidate finalized to immutable directory', NULL, ?);
                    """,
                    (str(uuid.uuid4()), release_id, fin_time),
                )
                con.commit()
            finally:
                con.close()

    def record_verification(
        self,
        release_id: str,
        manifest_sha256: str,
        database_sha256: str,
        validation_sha256: str,
        rule_inventory_version: str,
        rule_inventory_hash: str,
        verification_results_json: str,
        verifier_version: str,
        verified_at_utc: str | None = None,
    ) -> str:
        """Record post-seal S08 verification attestation and promote release status from FINALIZED to VERIFIED."""
        ver_id = str(uuid.uuid4())
        ver_time = verified_at_utc or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")

                # Verify release digests match exactly
                rel = cur.execute(
                    """
                    SELECT manifest_sha256, database_sha256, validation_sha256, status
                    FROM releases WHERE release_id = ?;
                    """,
                    (release_id,),
                ).fetchone()
                if not rel:
                    raise RegistryError(f"Release not found: {release_id}")

                if (rel[0] != manifest_sha256 or rel[1] != database_sha256 or rel[2] != validation_sha256):
                    raise RegistryError("Verification digests do not match finalized release digests.")

                # Insert attestation
                cur.execute(
                    """
                    INSERT INTO release_verifications (
                        verification_id, release_id, manifest_sha256, database_sha256,
                        validation_sha256, rule_inventory_version, rule_inventory_hash,
                        verification_results_json, verifier_version, verified_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        ver_id, release_id, manifest_sha256, database_sha256,
                        validation_sha256, rule_inventory_version, rule_inventory_hash,
                        verification_results_json, verifier_version, ver_time,
                    ),
                )

                # Transition status to VERIFIED
                cur.execute(
                    "UPDATE releases SET status = 'VERIFIED' WHERE release_id = ?;",
                    (release_id,),
                )
                cur.execute(
                    """
                    INSERT INTO release_status_events (
                        event_id, release_id, previous_status, new_status, reason, evidence_reference, event_at_utc
                    ) VALUES (?, ?, ?, 'VERIFIED', 'Passed post-seal S08 verification', ?, ?);
                    """,
                    (str(uuid.uuid4()), release_id, rel[3], ver_id, ver_time),
                )
                con.commit()
                return ver_id
            finally:
                con.close()

    def reject_release(
        self,
        release_id: str,
        reason: str,
        evidence_reference: str | None = None,
    ) -> None:
        """Mark a release as REJECTED. Immediately revokes serving eligibility without mutating files."""
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                rel = cur.execute(
                    "SELECT status FROM releases WHERE release_id = ?;", (release_id,)
                ).fetchone()
                if not rel:
                    raise RegistryError(f"Release not found: {release_id}")
                prev_status = rel[0]

                cur.execute(
                    "UPDATE releases SET status = 'REJECTED' WHERE release_id = ?;",
                    (release_id,),
                )
                cur.execute(
                    """
                    INSERT INTO release_status_events (
                        event_id, release_id, previous_status, new_status, reason, evidence_reference, event_at_utc
                    ) VALUES (?, ?, ?, 'REJECTED', ?, ?, ?);
                    """,
                    (str(uuid.uuid4()), release_id, prev_status, reason, evidence_reference, now_utc),
                )
                con.commit()
            finally:
                con.close()

    def retire_release(
        self,
        release_id: str,
        reason: str,
        evidence_reference: str | None = None,
    ) -> None:
        """Mark a release as RETIRED prior to payload cleanup. RETIRED is terminal."""
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                rel = cur.execute(
                    "SELECT status FROM releases WHERE release_id = ?;", (release_id,)
                ).fetchone()
                if not rel:
                    raise RegistryError(f"Release not found: {release_id}")
                prev_status = rel[0]

                cur.execute(
                    "UPDATE releases SET status = 'RETIRED' WHERE release_id = ?;",
                    (release_id,),
                )
                cur.execute(
                    """
                    INSERT INTO release_status_events (
                        event_id, release_id, previous_status, new_status, reason, evidence_reference, event_at_utc
                    ) VALUES (?, ?, ?, 'RETIRED', ?, ?, ?);
                    """,
                    (str(uuid.uuid4()), release_id, prev_status, reason, evidence_reference, now_utc),
                )
                con.commit()
            finally:
                con.close()

    # -----------------------------------------------------------------------
    # Current Release, Selection Requests, and CAS Publication
    # -----------------------------------------------------------------------

    def get_current_release(self) -> dict[str, Any]:
        """Query current release singleton pointer outside cache."""
        con = self.get_connection()
        try:
            cur = con.cursor()
            row = cur.execute(
                """
                SELECT cr.release_id, cr.generation, cr.updated_at_utc, r.status
                FROM current_release cr
                LEFT JOIN releases r ON cr.release_id = r.release_id
                WHERE cr.id = 1;
                """
            ).fetchone()
            if not row:
                return {"release_id": None, "generation": 0, "status": None, "updated_at_utc": None}
            return {
                "release_id": row[0],
                "generation": row[1],
                "updated_at_utc": row[2],
                "status": row[3],
            }
        finally:
            con.close()

    def record_selection_request(
        self,
        operation_key: str,
        target_release_id: str,
        expected_parent_generation: int,
        operation_kind: str = "publish",
        expected_parent_release_id: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Persist immutable selection request before attempting selection.
        
        Enforces conflict detection: reusing the same operation_key with different
        request parameters raises SelectionConflictError.
        """
        req_dict = {
            "operation_key": operation_key,
            "target_release_id": target_release_id,
            "expected_parent_release_id": expected_parent_release_id,
            "expected_parent_generation": expected_parent_generation,
            "operation_kind": operation_kind,
            "reason": reason,
        }
        canonical_json = json.dumps(req_dict, sort_keys=True, separators=(",", ":"))
        req_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                existing = cur.execute(
                    "SELECT request_hash FROM selection_requests WHERE operation_key = ?;",
                    (operation_key,),
                ).fetchone()

                if existing:
                    if existing[0] != req_hash:
                        con.rollback()
                        raise SelectionConflictError(
                            f"Operation key '{operation_key}' was previously registered with different parameters."
                        )
                    con.commit()
                    return

                cur.execute(
                    """
                    INSERT INTO selection_requests (
                        operation_key, request_hash, request_json, target_release_id,
                        expected_parent_release_id, expected_parent_generation,
                        operation_kind, reason, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        operation_key, req_hash, canonical_json, target_release_id,
                        expected_parent_release_id, expected_parent_generation,
                        operation_kind, reason, now_utc,
                    ),
                )
                con.commit()
            finally:
                con.close()

    def publish_release(
        self,
        operation_key: str,
        attempt_id: str | None = None,
    ) -> dict[str, Any]:
        """Perform atomic compare-and-swap (CAS) publication or rollback.
        
        Guarantees idempotency (Scenario T18):
        If process crashed after commit before acknowledgement, repeating the call
        with the same operation_key discovers the committed event and returns
        the historical outcome without re-incrementing the generation!
        """
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()

                # 1. Fetch persisted selection request
                req_row = cur.execute(
                    """
                    SELECT target_release_id, expected_parent_release_id,
                           expected_parent_generation, operation_kind, reason
                    FROM selection_requests WHERE operation_key = ?;
                    """,
                    (operation_key,),
                ).fetchone()
                if not req_row:
                    raise RegistryError(f"No selection request found for operation_key '{operation_key}'")

                target_rel_id = req_row[0]
                expected_parent_id = req_row[1]
                expected_parent_gen = req_row[2]
                op_kind = req_row[3]
                reason = req_row[4]

                # 2. Check for previously committed event (Scenario T18 idempotency check)
                existing_event = cur.execute(
                    """
                    SELECT event_id, previous_generation, new_generation, event_at_utc,
                           selected_release_id, previous_release_id
                    FROM publication_events WHERE operation_key = ?;
                    """,
                    (operation_key,),
                ).fetchone()

                if existing_event:
                    curr_row = cur.execute(
                        "SELECT release_id, generation FROM current_release WHERE id = 1;"
                    ).fetchone()
                    current_rel_id = curr_row[0] if curr_row else None
                    current_gen = curr_row[1] if curr_row else 0
                    is_current = (existing_event[4] == current_rel_id and existing_event[2] == current_gen)
                    return {
                        "status": "ALREADY_COMMITTED",
                        "event_id": existing_event[0],
                        "previous_generation": existing_event[1],
                        "new_generation": existing_event[2],
                        "event_at_utc": existing_event[3],
                        "selected_release_id": existing_event[4],
                        "previous_release_id": existing_event[5],
                        "is_current": is_current,
                        "current_generation": current_gen,
                        "current_release_id": current_rel_id,
                    }

                # 3. Short CAS transaction
                cur.execute("BEGIN IMMEDIATE;")

                # Check target release eligibility
                target_rel = cur.execute(
                    "SELECT status FROM releases WHERE release_id = ?;", (target_rel_id,)
                ).fetchone()
                if not target_rel:
                    con.rollback()
                    raise RegistryError(f"Target release not found: {target_rel_id}")

                if target_rel[0] != "VERIFIED":
                    con.rollback()
                    raise InvalidReleaseStatusError(
                        f"Target release status is '{target_rel[0]}'. Only 'VERIFIED' releases can be selected."
                    )

                # Assert matching post-seal verification attestation exists
                attestation = cur.execute(
                    "SELECT verification_id FROM release_verifications WHERE release_id = ?;",
                    (target_rel_id,),
                ).fetchone()
                if not attestation:
                    con.rollback()
                    raise RegistryError(f"No post-seal verification attestation found for release {target_rel_id}")

                # Read current release pointer
                curr_row = cur.execute(
                    "SELECT release_id, generation FROM current_release WHERE id = 1;"
                ).fetchone()
                if not curr_row:
                    con.rollback()
                    raise RegistryError("Current release singleton row missing.")

                current_rel_id = curr_row[0]
                current_gen = curr_row[1]

                # Verify expected parent generation and release match
                if current_gen != expected_parent_gen:
                    con.rollback()
                    raise StaleGenerationConflictError(
                        f"CAS conflict: current generation {current_gen} != expected {expected_parent_gen}"
                    )
                if current_rel_id != expected_parent_id:
                    con.rollback()
                    raise StaleGenerationConflictError(
                        f"CAS conflict: current release {current_rel_id} != expected parent {expected_parent_id}"
                    )

                # Increment generation and update pointer
                new_gen = current_gen + 1
                now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                event_id = str(uuid.uuid4())

                cur.execute(
                    """
                    UPDATE current_release
                    SET release_id = ?, generation = ?, updated_at_utc = ?
                    WHERE id = 1 AND generation = ?;
                    """,
                    (target_rel_id, new_gen, now_utc, current_gen),
                )
                if cur.rowcount != 1:
                    con.rollback()
                    raise StaleGenerationConflictError("Concurrent update detected on current_release singleton.")

                # Append atomic publication event
                cur.execute(
                    """
                    INSERT INTO publication_events (
                        event_id, operation_key, operation_kind, previous_release_id,
                        selected_release_id, previous_generation, new_generation,
                        attempt_id, reason, event_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        event_id, operation_key, op_kind, current_rel_id,
                        target_rel_id, current_gen, new_gen, attempt_id,
                        reason, now_utc,
                    ),
                )

                con.commit()
                return {
                    "status": "SUCCEEDED",
                    "event_id": event_id,
                    "previous_generation": current_gen,
                    "new_generation": new_gen,
                    "event_at_utc": now_utc,
                    "selected_release_id": target_rel_id,
                    "previous_release_id": current_rel_id,
                }
            finally:
                con.close()

    def query_selection_status(self, operation_key: str) -> dict[str, Any]:
        """Query the status of an operation key across requests, events, and active pointer.
        
        Adheres to Scenario T18: enables unacknowledged commit recovery and reconciliation
        without triggering state mutations.
        """
        con = self.get_connection()
        try:
            cur = con.cursor()

            # 1. Fetch current release pointer
            curr_row = cur.execute(
                "SELECT release_id, generation FROM current_release WHERE id = 1;"
            ).fetchone()
            current_rel_id = curr_row[0] if curr_row else None
            current_gen = curr_row[1] if curr_row else 0

            # 2. Check publication_events (committed state)
            ev = cur.execute(
                """
                SELECT event_id, operation_kind, previous_release_id, selected_release_id,
                       previous_generation, new_generation, attempt_id, reason, event_at_utc
                FROM publication_events
                WHERE operation_key = ?;
                """,
                (operation_key,),
            ).fetchone()

            if ev:
                selected_rel_id = ev[3]
                new_gen = ev[5]
                is_current = (selected_rel_id == current_rel_id and new_gen == current_gen)
                return {
                    "status": "COMMITTED",
                    "operation_key": operation_key,
                    "event_id": ev[0],
                    "operation_kind": ev[1],
                    "previous_release_id": ev[2],
                    "selected_release_id": selected_rel_id,
                    "previous_generation": ev[4],
                    "new_generation": new_gen,
                    "attempt_id": ev[6],
                    "reason": ev[7],
                    "event_at_utc": ev[8],
                    "is_current": is_current,
                    "current_generation": current_gen,
                    "current_release_id": current_rel_id,
                }

            # 3. Check selection_requests (registered intent, uncommitted)
            req = cur.execute(
                """
                SELECT target_release_id, expected_parent_release_id,
                       expected_parent_generation, operation_kind, reason, created_at_utc
                FROM selection_requests
                WHERE operation_key = ?;
                """,
                (operation_key,),
            ).fetchone()

            if req:
                target_rel_id = req[0]
                expected_parent_id = req[1]
                expected_parent_gen = req[2]
                can_retry = (expected_parent_gen == current_gen and expected_parent_id == current_rel_id)
                return {
                    "status": "PENDING_OR_UNCOMMITTED",
                    "operation_key": operation_key,
                    "target_release_id": target_rel_id,
                    "expected_parent_release_id": expected_parent_id,
                    "expected_parent_generation": expected_parent_gen,
                    "operation_kind": req[3],
                    "reason": req[4],
                    "created_at_utc": req[5],
                    "can_retry": can_retry,
                    "current_generation": current_gen,
                    "current_release_id": current_rel_id,
                }

            return {
                "status": "NOT_REGISTERED",
                "operation_key": operation_key,
                "current_generation": current_gen,
                "current_release_id": current_rel_id,
            }
        finally:
            con.close()

    def verify_release_integrity(
        self,
        release_id: str,
        releases_dir: Path | str | None = None,
    ) -> dict[str, Any]:
        """Verify the cryptographic and physical integrity of a retained release artifact on disk.
        
        Invariants:
        1. Checks database record: status must be 'VERIFIED'.
        2. Checks post-seal attestation in release_verifications.
        3. Verifies physical existence and SHA-256 of manifest.json, validation.json, and analytics.duckdb.
        4. Verifies absence of uncheckpointed WAL file (analytics.duckdb.wal).
        """
        con = self.get_connection()
        try:
            cur = con.cursor()
            rel = cur.execute(
                """
                SELECT release_directory, manifest_sha256, database_sha256, validation_sha256, status
                FROM releases WHERE release_id = ?;
                """,
                (release_id,),
            ).fetchone()

            if not rel:
                raise RegistryError(f"Release '{release_id}' not found in registry.")

            rel_dir_str, exp_manifest_sha, exp_db_sha, exp_val_sha, status = rel

            if status != "VERIFIED":
                raise InvalidReleaseStatusError(
                    f"Release '{release_id}' has status '{status}'. Only 'VERIFIED' releases can be selected or rolled back."
                )

            # Check post-seal attestation exists
            att = cur.execute(
                "SELECT verification_id FROM release_verifications WHERE release_id = ?;",
                (release_id,),
            ).fetchone()
            if not att:
                raise RegistryError(f"Release '{release_id}' has no post-seal attestation in release_verifications.")
        finally:
            con.close()

        # Resolve physical directory
        if releases_dir is not None:
            base_dir = Path(releases_dir)
            target_dir = base_dir / release_id if (base_dir / release_id).exists() else base_dir
        else:
            base_dir = self.data_root / "releases"
            target_dir = base_dir / release_id

        if not target_dir.exists():
            target_dir = self.data_root / rel_dir_str
        if not target_dir.exists():
            raise ReleaseIntegrityError(f"Release directory missing on disk: {target_dir}")

        manifest_path = target_dir / "manifest.json"
        db_path = target_dir / "analytics.duckdb"
        val_path = target_dir / "validation.json"
        wal_path = target_dir / "analytics.duckdb.wal"

        if not manifest_path.exists():
            raise ReleaseIntegrityError(f"manifest.json missing in {target_dir}")
        if not db_path.exists():
            raise ReleaseIntegrityError(f"analytics.duckdb missing in {target_dir}")
        if not val_path.exists():
            raise ReleaseIntegrityError(f"validation.json missing in {target_dir}")
        if wal_path.exists() and wal_path.stat().st_size > 0:
            raise ReleaseIntegrityError(f"Uncheckpointed WAL file detected: {wal_path} ({wal_path.stat().st_size} bytes)")

        # Compute file digests in 64 KiB chunks
        def _calc_sha256(p: Path) -> str:
            h = hashlib.sha256()
            with open(p, "rb") as f:
                while chunk := f.read(65536):
                    h.update(chunk)
            return h.hexdigest()

        actual_manifest_sha = _calc_sha256(manifest_path)
        actual_db_sha = _calc_sha256(db_path)
        actual_val_sha = _calc_sha256(val_path)

        if actual_manifest_sha != exp_manifest_sha:
            raise ReleaseIntegrityError(
                f"manifest.json digest mismatch for {release_id}: expected {exp_manifest_sha}, got {actual_manifest_sha}"
            )
        if actual_db_sha != exp_db_sha:
            raise ReleaseIntegrityError(
                f"analytics.duckdb digest mismatch for {release_id}: expected {exp_db_sha}, got {actual_db_sha}"
            )
        if actual_val_sha != exp_val_sha:
            raise ReleaseIntegrityError(
                f"validation.json digest mismatch for {release_id}: expected {exp_val_sha}, got {actual_val_sha}"
            )

        return {
            "release_id": release_id,
            "release_directory": str(target_dir),
            "manifest_sha256": actual_manifest_sha,
            "database_sha256": actual_db_sha,
            "validation_sha256": actual_val_sha,
            "status": "VERIFIED",
        }

    def rollback_release(
        self,
        operation_key: str,
        target_release_id: str,
        expected_parent_generation: int,
        expected_parent_release_id: str | None = None,
        reason: str | None = None,
        attempt_id: str | None = None,
        verify_bytes: bool = False,
        releases_dir: Path | str | None = None,
    ) -> dict[str, Any]:
        """Perform atomic compare-and-swap (CAS) rollback to a previously verified release.
        
        Adheres to Scenario T19:
        1. If verify_bytes is True, re-verifies physical SHA-256 digests of target release files.
        2. Records selection request with operation_kind='rollback'.
        3. Enforces CAS validation (rejects stale parent/generation).
        4. Updates current_release singleton to target_release_id and increments generation.
        5. Appends publication_events record with operation_kind='rollback' and reason.
        """
        if verify_bytes:
            self.verify_release_integrity(target_release_id, releases_dir=releases_dir)

        self.record_selection_request(
            operation_key=operation_key,
            target_release_id=target_release_id,
            expected_parent_generation=expected_parent_generation,
            operation_kind="rollback",
            expected_parent_release_id=expected_parent_release_id,
            reason=reason,
        )
        return self.publish_release(operation_key=operation_key, attempt_id=attempt_id)

    # -----------------------------------------------------------------------
    # Cleanup Tracking
    # -----------------------------------------------------------------------

    def record_cleanup_request(
        self,
        cleanup_id: str,
        preview_digest: str,
        preview_json: str,
        expected_generation: int,
        protected_set_digest: str,
        protected_set_json: str,
        review_reference: str,
    ) -> None:
        """Persist reviewed operator cleanup intent."""
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(
                    """
                    INSERT INTO cleanup_requests (
                        cleanup_id, preview_digest, preview_json, expected_generation,
                        protected_set_digest, protected_set_json, review_reference, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        cleanup_id, preview_digest, preview_json, expected_generation,
                        protected_set_digest, protected_set_json, review_reference, now_utc,
                    ),
                )
                con.commit()
            finally:
                con.close()

    def record_cleanup_items(
        self,
        cleanup_id: str,
        items: list[dict[str, Any]],
    ) -> None:
        """Record prepared cleanup items prior to deletion."""
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                for item in items:
                    cur.execute(
                        """
                        INSERT INTO cleanup_items (
                            cleanup_id, item_id, exact_relative_path, artifact_hash,
                            artifact_identity, byte_count, outcome, diagnostic, updated_at_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, 'PREPARED', ?, ?);
                        """,
                        (
                            cleanup_id, item["item_id"], item["exact_relative_path"],
                            item.get("artifact_hash"), item.get("artifact_identity"),
                            item["byte_count"], item.get("diagnostic"), now_utc,
                        ),
                    )
                con.commit()
            finally:
                con.close()

    def update_cleanup_item_outcome(
        self,
        cleanup_id: str,
        item_id: str,
        outcome: str,
        diagnostic: str | None = None,
    ) -> None:
        """Update cleanup item outcome after file deletion (DELETED, FAILED, SKIPPED)."""
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with writer_lock(self.data_root, reentrant=True):
            con = self.get_connection()
            try:
                cur = con.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute(
                    """
                    UPDATE cleanup_items
                    SET outcome = ?, diagnostic = ?, updated_at_utc = ?
                    WHERE cleanup_id = ? AND item_id = ?;
                    """,
                    (outcome, diagnostic, now_utc, cleanup_id, item_id),
                )
                con.commit()
            finally:
                con.close()
