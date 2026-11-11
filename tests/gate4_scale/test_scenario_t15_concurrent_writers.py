"""Scenario T15: Concurrent Writers Under POSIX Advisory Lock.

Acceptance Criteria:
1. One mutating application process holds $DATA_ROOT/control/writer.lock.
2. A second writer (CLI or orchestrator) attempting mutation concurrently exits boundedly
   without modifying application artifacts, corrupting SQLite, or overwriting releases.
"""

from __future__ import annotations

import multiprocessing
from pathlib import Path
import tempfile
import time

import pytest

from hvfhv.control.locks import writer_lock, LockContentionError


def _holding_writer_process(data_root_str: str, hold_duration: float, ready_event, release_event):
    """Holds writer.lock for a specified duration to simulate a running build."""
    data_root = Path(data_root_str)
    with writer_lock(data_root, timeout=2.0):
        ready_event.set()
        release_event.wait(timeout=hold_duration)


def test_t15_concurrent_writers_exit_boundedly():
    """Verify that a second writer attempting to acquire writer.lock raises LockContentionError."""
    with tempfile.TemporaryDirectory() as td:
        data_root = Path(td)
        (data_root / "control").mkdir(parents=True, exist_ok=True)

        ready_event = multiprocessing.Event()
        release_event = multiprocessing.Event()

        # Start first process holding the writer lock
        p1 = multiprocessing.Process(
            target=_holding_writer_process,
            args=(str(data_root), 5.0, ready_event, release_event),
        )
        p1.start()

        try:
            # Wait until p1 successfully holds writer.lock
            assert ready_event.wait(timeout=3.0), "Worker process failed to acquire writer.lock in time"

            # Second writer tries to acquire with a short timeout
            start_t = time.time()
            with pytest.raises(LockContentionError) as exc_info:
                with writer_lock(data_root, timeout=0.2):
                    pytest.fail("Second writer should not have acquired writer.lock")

            elapsed = time.time() - start_t
            assert elapsed >= 0.2, f"Writer lock should have waited for timeout, elapsed: {elapsed}"
            assert "Could not acquire exclusive" in str(exc_info.value)
        finally:
            release_event.set()
            p1.join(timeout=2.0)
            if p1.is_alive():
                p1.terminate()
