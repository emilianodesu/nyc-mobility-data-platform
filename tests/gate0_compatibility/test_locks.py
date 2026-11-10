"""Tests for Gate G0: Advisory POSIX Locking via fcntl.flock on macOS APFS.

Validates:
- writer_lock lifecycle and metadata recording
- Non-blocking contention detection (LockContentionError)
- Inode stability invariant (lock files never unlinked)
- Cross-process mutual exclusion via subprocess
- Crash recovery (SIGKILL releases kernel flock immediately)
- query_lock exclusive by default and shared mode
- Bounded wait timeout
- Context manager exception safety
"""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

try:
    import pytest
except ImportError:
    class _RaisesContext:
        def __init__(self, expected_exception, match=None):
            self.expected_exception = expected_exception
            self.match = match
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is None:
                raise AssertionError(f"Expected {self.expected_exception} but no exception was raised")
            if not issubclass(exc_type, self.expected_exception):
                return False
            if self.match and self.match not in str(exc_val):
                raise AssertionError(f"Expected match '{self.match}' in '{exc_val}'")
            return True

    class _PytestShim:
        @staticmethod
        def raises(expected_exception, match=None):
            return _RaisesContext(expected_exception, match)

    pytest = _PytestShim()

from hvfhv.control.locks import (
    AdvisoryLock,
    LockContentionError,
    LockError,
    query_lock,
    writer_lock,
)


def test_writer_lock_lifecycle(tmp_path: Path):
    """Verify context manager acquisition, metadata recording, and no unlinking."""
    data_root = tmp_path / "data"
    lock_file = data_root / "control" / "writer.lock"

    assert not lock_file.exists()

    with writer_lock(data_root, diagnostic_info={"attempt_id": "att-1"}):
        assert lock_file.exists()
        raw = lock_file.read_text(encoding="utf-8")
        assert "att-1" in raw
        assert "HELD" in raw

    # Lock file must NEVER be unlinked on release
    assert lock_file.exists()
    raw_after = lock_file.read_text(encoding="utf-8")
    assert "RELEASED" in raw_after


def test_writer_lock_contention_non_blocking(tmp_path: Path):
    """Verify non-blocking acquisition attempt when locked raises LockContentionError immediately."""
    data_root = tmp_path / "data"

    with writer_lock(data_root, diagnostic_info={"owner": "primary"}):
        with pytest.raises(LockContentionError) as exc_info:
            with writer_lock(data_root, timeout=0.0):
                pass
        # If exc_info is a context manager with an exception caught:
        # In custom shim or pytest:
        pass


def test_lock_file_never_unlinked_inode_stability(tmp_path: Path):
    """Verify inode number remains identical across acquire/release/acquire cycles."""
    data_root = tmp_path / "data"
    lock_file = data_root / "control" / "writer.lock"

    with writer_lock(data_root):
        ino1 = lock_file.stat().st_ino

    ino2 = lock_file.stat().st_ino
    assert ino1 == ino2, "Lock file was unlinked and recreated during release!"

    with writer_lock(data_root):
        ino3 = lock_file.stat().st_ino
        assert ino1 == ino3, "Lock file was unlinked and recreated during re-acquisition!"


def test_cross_process_contention(tmp_path: Path):
    """Verify true cross-process mutual exclusion via subprocess."""
    data_root = tmp_path / "data"

    child_script = f"""
import sys
import time
from pathlib import Path
sys.path.insert(0, {repr(str(Path(__file__).resolve().parents[2] / 'src'))})
from hvfhv.control.locks import writer_lock

with writer_lock({repr(str(data_root))}, diagnostic_info={{"proc": "child"}}):
    print("CHILD_LOCKED", flush=True)
    time.sleep(1.0)
print("CHILD_RELEASED", flush=True)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", child_script],
        stdout=subprocess.PIPE,
        text=True,
    )
    line = proc.stdout.readline()
    assert "CHILD_LOCKED" in line

    # Parent attempt while child holds it must fail
    with pytest.raises(LockContentionError):
        with writer_lock(data_root, timeout=0.0):
            pass

    proc.wait()
    # Now parent can acquire cleanly
    with writer_lock(data_root, timeout=0.0):
        pass


def test_crash_recovery_sigkill(tmp_path: Path):
    """Verify that macOS kernel immediately releases flock upon child SIGKILL termination."""
    data_root = tmp_path / "data"

    child_script = f"""
import sys
import time
from pathlib import Path
sys.path.insert(0, {repr(str(Path(__file__).resolve().parents[2] / 'src'))})
from hvfhv.control.locks import writer_lock

with writer_lock({repr(str(data_root))}, diagnostic_info={{"proc": "doomed"}}):
    print("LOCKED", flush=True)
    time.sleep(100.0)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", child_script],
        stdout=subprocess.PIPE,
        text=True,
    )
    proc.stdout.readline()

    # Verify child is holding it
    with pytest.raises(LockContentionError):
        with writer_lock(data_root, timeout=0.0):
            pass

    # Terminate child with SIGKILL
    proc.kill()
    proc.wait()

    # Kernel immediately releases POSIX flock
    with writer_lock(data_root, timeout=0.0):
        pass


def test_query_lock_exclusive_by_default(tmp_path: Path):
    """Verify query_lock is exclusive by default (max 1 active reader)."""
    data_root = tmp_path / "data"

    with query_lock(data_root, diagnostic_info={"reader": "reader-1"}):
        with pytest.raises(LockContentionError):
            with query_lock(data_root, timeout=0.0):
                pass


def test_query_lock_shared_mode(tmp_path: Path):
    """Verify query_lock in shared mode allows multiple readers while blocking exclusive writer."""
    data_root = tmp_path / "data"

    # Two shared readers can coexist
    with query_lock(data_root, shared=True):
        with query_lock(data_root, shared=True, timeout=0.0):
            pass

        # Exclusive query lock blocks while shared is held
        with pytest.raises(LockContentionError):
            with query_lock(data_root, shared=False, timeout=0.0):
                pass


def test_bounded_wait_timeout(tmp_path: Path):
    """Verify lock acquisition waits up to timeout and succeeds if released."""
    data_root = tmp_path / "data"

    child_script = f"""
import sys
import time
from pathlib import Path
sys.path.insert(0, {repr(str(Path(__file__).resolve().parents[2] / 'src'))})
from hvfhv.control.locks import writer_lock

with writer_lock({repr(str(data_root))}):
    print("CHILD_LOCKED", flush=True)
    time.sleep(0.4)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", child_script],
        stdout=subprocess.PIPE,
        text=True,
    )
    proc.stdout.readline()

    # Waiter with timeout=2.0 should succeed after child finishes (~0.4s)
    start = time.monotonic()
    with writer_lock(data_root, timeout=2.0):
        elapsed = time.monotonic() - start
        assert elapsed >= 0.3

    proc.wait()


def test_exception_safety(tmp_path: Path):
    """Verify lock is released cleanly if exception occurs inside context."""
    data_root = tmp_path / "data"

    try:
        with writer_lock(data_root):
            raise RuntimeError("simulated pipeline error")
    except RuntimeError:
        pass

    # Next caller must acquire immediately without deadlock
    with writer_lock(data_root, timeout=0.0):
        pass


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        test_writer_lock_lifecycle(tpath)
        test_writer_lock_contention_non_blocking(tpath)
        test_lock_file_never_unlinked_inode_stability(tpath)
        test_cross_process_contention(tpath)
        test_crash_recovery_sigkill(tpath)
        test_query_lock_exclusive_by_default(tpath)
        test_query_lock_shared_mode(tpath)
        test_bounded_wait_timeout(tpath)
        test_exception_safety(tpath)
    print("ALL 9 POSIX FLOCK TESTS PASSED.")
