"""Control subpackage for HVFHV platform.

Exposes POSIX advisory locks and database control plane utilities.
"""

from hvfhv.control.locks import (
    AdvisoryLock,
    LockContentionError,
    LockError,
    query_lock,
    writer_lock,
)
from hvfhv.control.registry import (
    BackupIntegrityError,
    InvalidReleaseStatusError,
    Migration,
    MigrationDowngradeError,
    MigrationError,
    Registry,
    RegistryError,
    SelectionConflictError,
    StaleGenerationConflictError,
    configure_sqlite_connection,
    open_registry_connection,
)

__all__ = [
    "AdvisoryLock",
    "BackupIntegrityError",
    "InvalidReleaseStatusError",
    "LockContentionError",
    "LockError",
    "Migration",
    "MigrationDowngradeError",
    "MigrationError",
    "Registry",
    "RegistryError",
    "SelectionConflictError",
    "StaleGenerationConflictError",
    "configure_sqlite_connection",
    "open_registry_connection",
    "query_lock",
    "writer_lock",
]

