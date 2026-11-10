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

__all__ = [
    "AdvisoryLock",
    "LockContentionError",
    "LockError",
    "query_lock",
    "writer_lock",
]
