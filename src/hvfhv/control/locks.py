"""Advisory POSIX locking primitives for the HVFHV platform.

Enforces cross-process mutual exclusion via BSD flock(2) on macOS APFS.
Lock files are permanent markers in $DATA_ROOT/control/ and must NEVER be unlinked.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
from pathlib import Path
from typing import Any


class LockError(Exception):
    """Base exception for locking errors."""
    pass


class LockContentionError(LockError):
    """Raised when an advisory lock cannot be acquired or timed out."""

    def __init__(self, message: str, holder_info: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.holder_info = holder_info or {}


class AdvisoryLock:
    """Advisory POSIX flock context manager.

    Invariants:
    1. Lock files are opened in 'a+' mode to prevent pre-lock truncation.
    2. Lock files are NEVER unlinked.
    3. Contention raises LockContentionError with diagnostic details.
    """

    def __init__(
        self,
        lock_path: Path | str,
        exclusive: bool = True,
        timeout: float = 0.0,
        poll_interval: float = 0.05,
        diagnostic_info: dict[str, Any] | None = None,
    ) -> None:
        self.lock_path = Path(lock_path)
        self.exclusive = exclusive
        self.timeout = max(0.0, float(timeout))
        self.poll_interval = max(0.005, float(poll_interval))
        self.diagnostic_info = diagnostic_info or {}
        self._file: Any | None = None

    def acquire(self) -> AdvisoryLock:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        # Open in 'a+' mode to prevent truncating prior to lock acquisition
        self._file = open(self.lock_path, "a+", encoding="utf-8")
        op = fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH

        start_time = time.monotonic()
        deadline = start_time + self.timeout

        while True:
            try:
                fcntl.flock(self._file.fileno(), op | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    holder_info = self._read_diagnostic()
                    self._file.close()
                    self._file = None
                    lock_type = "exclusive" if self.exclusive else "shared"
                    raise LockContentionError(
                        f"Could not acquire {lock_type} lock on '{self.lock_path}' "
                        f"within {self.timeout:.2f}s; currently held by: {holder_info}",
                        holder_info=holder_info,
                    )
                time.sleep(self.poll_interval)

        # Successfully acquired: write diagnostic metadata if exclusive
        if self.exclusive:
            self._write_diagnostic(state="HELD")

        return self

    def release(self) -> None:
        if self._file is not None and not self._file.closed:
            try:
                if self.exclusive:
                    self._write_diagnostic(state="RELEASED")
            except Exception:
                pass
            finally:
                try:
                    fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
                finally:
                    self._file.close()
                    self._file = None

    def _read_diagnostic(self) -> dict[str, Any]:
        if self._file is None or self._file.closed:
            return {}
        try:
            self._file.seek(0)
            raw = self._file.read().strip()
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return {}

    def _write_diagnostic(self, state: str) -> None:
        if self._file is None or self._file.closed:
            return
        try:
            self._file.seek(0)
            self._file.truncate(0)
            payload = {
                "pid": os.getpid(),
                "state": state,
                "lock_type": "exclusive" if self.exclusive else "shared",
                "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                **self.diagnostic_info,
            }
            json.dump(payload, self._file)
            self._file.flush()
        except Exception:
            pass

    def __enter__(self) -> AdvisoryLock:
        return self.acquire()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


def writer_lock(
    data_root: Path | str,
    timeout: float = 0.0,
    diagnostic_info: dict[str, Any] | None = None,
) -> AdvisoryLock:
    """Acquire the exclusive OS advisory writer lock at $DATA_ROOT/control/writer.lock."""
    lock_path = Path(data_root) / "control" / "writer.lock"
    meta = {"lock_name": "writer.lock", **(diagnostic_info or {})}
    return AdvisoryLock(
        lock_path=lock_path,
        exclusive=True,
        timeout=timeout,
        diagnostic_info=meta,
    )


def query_lock(
    data_root: Path | str,
    timeout: float = 0.0,
    shared: bool = False,
    diagnostic_info: dict[str, Any] | None = None,
) -> AdvisoryLock:
    """Acquire the OS advisory query lock at $DATA_ROOT/control/query.lock.

    By default (shared=False), enforces max 1 active analytical reader query
    across local Streamlit processes. Set shared=True if shared reader access
    is explicitly requested.
    """
    lock_path = Path(data_root) / "control" / "query.lock"
    meta = {"lock_name": "query.lock", **(diagnostic_info or {})}
    return AdvisoryLock(
        lock_path=lock_path,
        exclusive=not shared,
        timeout=timeout,
        diagnostic_info=meta,
    )
