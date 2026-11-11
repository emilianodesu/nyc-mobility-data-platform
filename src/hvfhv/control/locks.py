"""Advisory POSIX locking primitives for the HVFHV platform.

Enforces cross-process mutual exclusion via BSD flock(2) on macOS APFS.
Lock files are permanent markers in $DATA_ROOT/control/ and must NEVER be unlinked.
"""

from __future__ import annotations

import fcntl
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

_thread_local = threading.local()


def is_lock_held_by_current_thread(lock_path: Path | str, exclusive_only: bool = True) -> bool:
    """Check if the calling thread currently holds the advisory lock on lock_path."""
    held = getattr(_thread_local, "held_locks", None)
    if not held:
        return False
    canonical_path = Path(lock_path).resolve()
    state = held.get(canonical_path)
    if state is None or state.get("depth", 0) <= 0:
        return False
    if exclusive_only:
        return state.get("mode") == "exclusive"
    return True


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
    4. Supports thread-local re-entrancy tracking when reentrant=True.
    """

    def __init__(
        self,
        lock_path: Path | str,
        exclusive: bool = True,
        timeout: float = 0.0,
        poll_interval: float = 0.05,
        diagnostic_info: dict[str, Any] | None = None,
        reentrant: bool = False,
    ) -> None:
        self.lock_path = Path(lock_path)
        self.exclusive = exclusive
        self.timeout = max(0.0, float(timeout))
        self.poll_interval = max(0.005, float(poll_interval))
        self.diagnostic_info = diagnostic_info or {}
        self.reentrant = reentrant
        self._file: Any | None = None
        self._is_reentrant: bool = False

    def acquire(self) -> AdvisoryLock:
        canonical_path = self.lock_path.resolve()
        held = getattr(_thread_local, "held_locks", None)
        if held is None:
            held = {}
            _thread_local.held_locks = held

        # Thread-local re-entrancy tracking:
        # If reentrant is enabled and current thread already holds the lock
        if self.reentrant and canonical_path in held:
            state = held[canonical_path]
            # Current thread holds exclusive, or holds shared and requests shared
            if state.get("mode") == "exclusive" or (state.get("mode") == "shared" and not self.exclusive):
                state["depth"] = state.get("depth", 0) + 1
                self._is_reentrant = True
                self._file = state.get("file")
                return self

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

        # Successfully acquired: record in thread-local storage
        held[canonical_path] = {
            "mode": "exclusive" if self.exclusive else "shared",
            "depth": 1,
            "file": self._file,
            "owner": self,
        }
        self._is_reentrant = False

        # Successfully acquired: write diagnostic metadata if exclusive
        if self.exclusive:
            self._write_diagnostic(state="HELD")

        return self

    def release(self) -> None:
        canonical_path = self.lock_path.resolve()
        held = getattr(_thread_local, "held_locks", None)

        if held is not None and canonical_path in held:
            state = held[canonical_path]
            state["depth"] = state.get("depth", 1) - 1
            if state["depth"] > 0:
                # Still held by an outer acquisition context; do not release OS flock or close file
                self._file = None
                return
            # Depth reached 0; clean up thread local and proceed to release
            file_obj = state.get("file") or self._file
            del held[canonical_path]
        else:
            file_obj = self._file

        if file_obj is not None and not file_obj.closed:
            try:
                if self.exclusive:
                    self._write_diagnostic_file(file_obj, state="RELEASED")
            except Exception:
                pass
            finally:
                try:
                    fcntl.flock(file_obj.fileno(), fcntl.LOCK_UN)
                finally:
                    file_obj.close()
                    if self._file is file_obj:
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
        self._write_diagnostic_file(self._file, state)

    def _write_diagnostic_file(self, f: Any, state: str) -> None:
        if f is None or f.closed:
            return
        try:
            f.seek(0)
            f.truncate(0)
            payload = {
                "pid": os.getpid(),
                "state": state,
                "lock_type": "exclusive" if self.exclusive else "shared",
                "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                **self.diagnostic_info,
            }
            json.dump(payload, f)
            f.flush()
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
    reentrant: bool = False,
) -> AdvisoryLock:
    """Acquire the exclusive OS advisory writer lock at $DATA_ROOT/control/writer.lock."""
    lock_path = Path(data_root) / "control" / "writer.lock"
    meta = {"lock_name": "writer.lock", **(diagnostic_info or {})}
    return AdvisoryLock(
        lock_path=lock_path,
        exclusive=True,
        timeout=timeout,
        diagnostic_info=meta,
        reentrant=reentrant,
    )


def query_lock(
    data_root: Path | str,
    timeout: float = 0.0,
    shared: bool = False,
    diagnostic_info: dict[str, Any] | None = None,
    reentrant: bool = False,
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
        reentrant=reentrant,
    )
