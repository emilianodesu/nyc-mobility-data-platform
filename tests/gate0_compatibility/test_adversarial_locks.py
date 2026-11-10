"""Adversarial stress harness for Advisory POSIX Locking (hvfhv.control.locks).

Empirically tests:
1. Multi-process race contention (10 processes racing for writer_lock with shared state accumulator)
2. High-concurrency non-blocking contention (10 processes racing simultaneously, exactly 1 wins)
3. SIGKILL crash recovery under rapid succession (5 successive process terminations)
4. Shared vs Exclusive query_lock matrix with concurrent reader barrier
5. Lock file inode stability and zero unlinking across hundreds of operations
6. Corrupted/partial diagnostic JSON recovery
7. Timeout accuracy and boundary conditions
"""

from __future__ import annotations

import multiprocessing as mp
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any

from hvfhv.control.locks import (
    AdvisoryLock,
    LockContentionError,
    LockError,
    query_lock,
    writer_lock,
)


# ==============================================================================
# Helper worker functions for multiprocessing
# ==============================================================================

def _writer_race_worker(data_root_str: str, counter_file_str: str, iterations: int, worker_id: int):
    """Worker process that repeatedly acquires writer_lock and increments a shared counter."""
    data_root = Path(data_root_str)
    counter_file = Path(counter_file_str)
    
    for i in range(iterations):
        # Acquire lock with a reasonable timeout to allow queueing under contention
        with writer_lock(data_root, timeout=10.0, diagnostic_info={"worker_id": worker_id, "iter": i}):
            # Read current counter
            val = 0
            if counter_file.exists():
                text = counter_file.read_text(encoding="utf-8").strip()
                if text:
                    val = int(text)
            
            # Artificial micro-jitter to expose any race conditions if mutual exclusion fails
            time.sleep(0.001)
            
            # Write back incremented counter
            counter_file.write_text(str(val + 1), encoding="utf-8")


def _nonblocking_race_worker(data_root_str: str, barrier: Any, result_queue: Any):
    """Worker process that waits on barrier then immediately attempts non-blocking acquire."""
    data_root = Path(data_root_str)
    barrier.wait()
    try:
        with writer_lock(data_root, timeout=0.0):
            result_queue.put(("ACQUIRED", os.getpid()))
            time.sleep(0.2)  # Hold briefly so others definitely collide
    except LockContentionError as exc:
        result_queue.put(("CONTENTION", os.getpid(), exc.holder_info))
    except Exception as exc:
        result_queue.put(("ERROR", os.getpid(), str(exc)))


def _shared_reader_worker(data_root_str: str, ready_barrier: Any, exit_event: Any, counter_file_str: str):
    """Worker that acquires shared query_lock and records its active presence."""
    data_root = Path(data_root_str)
    counter_file = Path(counter_file_str)
    
    with query_lock(data_root, shared=True, timeout=5.0):
        # Atomically report presence
        with open(counter_file, "a", encoding="utf-8") as f:
            f.write(f"+{os.getpid()}\n")
        ready_barrier.wait()
        exit_event.wait()
        with open(counter_file, "a", encoding="utf-8") as f:
            f.write(f"-{os.getpid()}\n")


# ==============================================================================
# Empirical Adversarial Tests
# ==============================================================================

def test_adversarial_multiprocess_writer_race(tmp_path: Path):
    """10 processes racing for writer_lock incrementing shared counter 20 times each = 200 increments."""
    print("  -> Testing adversarial multi-process writer_lock contention (10 procs x 20 iters)...")
    data_root = tmp_path / "data_race"
    counter_file = data_root / "counter.txt"
    data_root.mkdir(parents=True, exist_ok=True)
    counter_file.write_text("0", encoding="utf-8")
    
    num_procs = 10
    iters_per_proc = 20
    procs = []
    
    start_time = time.monotonic()
    for worker_id in range(num_procs):
        p = mp.Process(
            target=_writer_race_worker,
            args=(str(data_root), str(counter_file), iters_per_proc, worker_id),
        )
        p.start()
        procs.append(p)
    
    for p in procs:
        p.join(timeout=30.0)
        assert not p.is_alive(), f"Process {p.pid} timed out"
        assert p.exitcode == 0, f"Process {p.pid} exited with code {p.exitcode}"
        
    duration = time.monotonic() - start_time
    final_val = int(counter_file.read_text(encoding="utf-8").strip())
    expected_val = num_procs * iters_per_proc
    
    assert final_val == expected_val, (
        f"Race condition detected! Final counter is {final_val}, expected {expected_val}. "
        "Mutual exclusion was violated!"
    )
    print(f"     [PASS] Mutual exclusion verified: exact {final_val}/{expected_val} increments in {duration:.2f}s")


def test_adversarial_nonblocking_contention_storm(tmp_path: Path):
    """12 processes simultaneously release from a barrier and race for non-blocking writer_lock."""
    print("  -> Testing non-blocking contention storm (12 processes synchronized barrier)...")
    data_root = tmp_path / "data_storm"
    num_procs = 12
    barrier = mp.Barrier(num_procs)
    result_queue = mp.Queue()
    
    procs = []
    for _ in range(num_procs):
        p = mp.Process(
            target=_nonblocking_race_worker,
            args=(str(data_root), barrier, result_queue),
        )
        p.start()
        procs.append(p)
        
    for p in procs:
        p.join(timeout=10.0)
        assert not p.is_alive(), f"Process {p.pid} hung during contention storm"
        assert p.exitcode == 0, f"Process {p.pid} exited abnormally"
        
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
        
    assert len(results) == num_procs, f"Expected {num_procs} results, got {len(results)}"
    
    acquired = [r for r in results if r[0] == "ACQUIRED"]
    contended = [r for r in results if r[0] == "CONTENTION"]
    errors = [r for r in results if r[0] == "ERROR"]
    
    assert len(errors) == 0, f"Unexpected errors during race: {errors}"
    assert len(acquired) == 1, (
        f"Expected exactly 1 process to acquire non-blocking lock, but {len(acquired)} acquired it! "
        f"Acquired: {acquired}"
    )
    assert len(contended) == num_procs - 1, (
        f"Expected {num_procs - 1} processes to receive LockContentionError, got {len(contended)}"
    )
    print(f"     [PASS] Non-blocking contention verified: 1 winner, {len(contended)} rejections, 0 anomalies")


def test_adversarial_sigkill_crash_recovery_cascade(tmp_path: Path):
    """Cascade test: 5 consecutive processes holding lock are terminated abruptly with SIGKILL."""
    print("  -> Testing SIGKILL crash recovery cascade (5 consecutive kills)...")
    data_root = tmp_path / "data_sigkill"
    lock_file = data_root / "control" / "writer.lock"
    
    for cycle in range(5):
        child_code = f"""
import sys
import time
from pathlib import Path
sys.path.insert(0, {repr(str(Path(__file__).resolve().parents[2] / 'src'))})
from hvfhv.control.locks import writer_lock

with writer_lock({repr(str(data_root))}, diagnostic_info={{"cycle": {cycle}}}):
    print("ACQUIRED", flush=True)
    time.sleep(30.0)
"""
        proc = subprocess.Popen(
            [sys.executable, "-c", child_code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Wait until child reports acquisition
        line = proc.stdout.readline().strip()
        assert line == "ACQUIRED", f"Child failed to acquire in cycle {cycle}: {proc.stderr.read()}"
        
        # Verify parent cannot acquire
        try:
            with writer_lock(data_root, timeout=0.0):
                raise AssertionError("Acquired lock while child held it!")
        except LockContentionError:
            pass
            
        # Abruptly SIGKILL the child
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        
        # Verify immediate kernel flock release (< 0.05s)
        t0 = time.monotonic()
        with writer_lock(data_root, timeout=0.1, diagnostic_info={"parent_recovered": cycle}):
            elapsed = time.monotonic() - t0
            assert elapsed < 0.08, f"Acquisition after SIGKILL was delayed: {elapsed:.4f}s"
            # Ensure the lock file was overwritten with parent's metadata
            content = lock_file.read_text(encoding="utf-8")
            assert "parent_recovered" in content
            assert f'"cycle": {cycle}' not in content
            
    print("     [PASS] SIGKILL cascade verified: 5/5 instant kernel releases (<80ms)")


def test_adversarial_shared_vs_exclusive_query_lock_matrix(tmp_path: Path):
    """Verify concurrent shared readers block exclusive writer, and exclusive writer blocks readers."""
    print("  -> Testing shared vs exclusive query_lock concurrency matrix...")
    data_root = tmp_path / "data_sh_ex"
    counter_file = data_root / "readers.log"
    data_root.mkdir(parents=True, exist_ok=True)
    
    num_readers = 6
    ready_barrier = mp.Barrier(num_readers + 1)
    exit_event = mp.Event()
    
    readers = []
    for _ in range(num_readers):
        p = mp.Process(
            target=_shared_reader_worker,
            args=(str(data_root), ready_barrier, exit_event, str(counter_file)),
        )
        p.start()
        readers.append(p)
        
    # Wait for all readers to acquire shared lock
    ready_barrier.wait()
    
    # Verify all 6 readers are concurrently inside the shared lock
    lines = counter_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == num_readers, f"Expected {num_readers} active readers, found {len(lines)}"
    
    # 1. While 6 readers hold shared lock, an exclusive acquire must FAIL immediately
    try:
        with query_lock(data_root, shared=False, timeout=0.0):
            raise AssertionError("Exclusive lock acquired while shared readers held it!")
    except LockContentionError:
        pass
        
    # 2. Another shared reader CAN acquire
    with query_lock(data_root, shared=True, timeout=0.0):
        pass
        
    # Release shared readers
    exit_event.set()
    for p in readers:
        p.join(timeout=5.0)
        assert p.exitcode == 0
        
    # 3. Now acquire exclusive query lock
    with query_lock(data_root, shared=False, timeout=0.0):
        # While exclusive query lock is held, shared reader must FAIL immediately
        try:
            with query_lock(data_root, shared=True, timeout=0.0):
                raise AssertionError("Shared reader acquired while exclusive lock held!")
        except LockContentionError:
            pass
            
    print(f"     [PASS] Query lock matrix verified: {num_readers} concurrent readers, mutual exclusion vs exclusive")


def test_adversarial_lock_file_inode_permanence(tmp_path: Path):
    """Verify that lock file inode NEVER changes across 200 rapid acquire/release cycles."""
    print("  -> Testing inode permanence (200 acquire/release cycles)...")
    data_root = tmp_path / "data_inode"
    lock_file = data_root / "control" / "writer.lock"
    
    with writer_lock(data_root):
        initial_stat = lock_file.stat()
        initial_ino = initial_stat.st_ino
        initial_dev = initial_stat.st_dev
        
    for i in range(200):
        with writer_lock(data_root, diagnostic_info={"cycle": i}):
            current_stat = lock_file.stat()
            assert current_stat.st_ino == initial_ino, f"Inode changed at cycle {i}!"
            assert current_stat.st_dev == initial_dev, f"Device changed at cycle {i}!"
            
    # Verify post-release stat
    final_stat = lock_file.stat()
    assert final_stat.st_ino == initial_ino
    assert final_stat.st_dev == initial_dev
    print(f"     [PASS] Inode stability verified: inode {initial_ino} preserved through 200 cycles")


def test_adversarial_corrupted_diagnostic_resilience(tmp_path: Path):
    """Verify that corrupted, partial, or malformed JSON in lock file does not crash the locker."""
    print("  -> Testing resilience against corrupted diagnostic metadata in lock file...")
    data_root = tmp_path / "data_corrupt"
    lock_file = data_root / "control" / "writer.lock"
    data_root.mkdir(parents=True, exist_ok=True)
    (data_root / "control").mkdir(parents=True, exist_ok=True)
    
    # Inject corrupted bytes into lock file
    corrupted_payloads = [
        "",                           # empty file
        "{malformed json",            # syntax error
        "\x00\x00\x00\x00\xff\xfe",   # binary garbage
        '{"pid": "not an int"}',      # unexpected types
        '{"pid": 1234, "state": ',    # truncated write
    ]
    
    for payload in corrupted_payloads:
        lock_file.write_text(payload, encoding="utf-8")
        
        # Test 1: acquire exclusive overwrites cleanly
        with writer_lock(data_root, timeout=0.0):
            content = lock_file.read_text(encoding="utf-8")
            assert "HELD" in content
            
        # Inject corrupted bytes again while simulating contention
        lock_file.write_text(payload, encoding="utf-8")
        
        # Hold lock with low-level flock to trigger LockContentionError
        fd = open(lock_file, "a+", encoding="utf-8")
        import fcntl
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            try:
                with writer_lock(data_root, timeout=0.0):
                    raise AssertionError("Should have failed with LockContentionError")
            except LockContentionError as exc:
                # Must handle corrupted json cleanly and return empty dict or parsed
                assert isinstance(exc.holder_info, dict)
        finally:
            fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
            fd.close()
            
    print("     [PASS] Resilience against corrupted metadata verified across 5 malformed payload classes")


def test_adversarial_timeout_precision(tmp_path: Path):
    """Verify that timeout deadline is honored within reasonable bounds and doesn't busy-spin."""
    print("  -> Testing timeout precision and deadline adherence...")
    data_root = tmp_path / "data_timeout"
    lock_file = data_root / "control" / "writer.lock"
    data_root.mkdir(parents=True, exist_ok=True)
    (data_root / "control").mkdir(parents=True, exist_ok=True)
    
    import fcntl
    fd = open(lock_file, "a+", encoding="utf-8")
    fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    
    try:
        # Test zero timeout
        t0 = time.monotonic()
        try:
            with writer_lock(data_root, timeout=0.0):
                raise AssertionError("Acquired locked file")
        except LockContentionError:
            elapsed_0 = time.monotonic() - t0
            assert elapsed_0 < 0.05, f"Timeout 0.0 took too long: {elapsed_0:.4f}s"
            
        # Test 0.3s timeout
        t0 = time.monotonic()
        try:
            with writer_lock(data_root, timeout=0.3):
                raise AssertionError("Acquired locked file")
        except LockContentionError:
            elapsed_300 = time.monotonic() - t0
            assert 0.28 <= elapsed_300 <= 0.45, f"Timeout 0.3s outside tolerance: {elapsed_300:.4f}s"
            
    finally:
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
        fd.close()
        
    print("     [PASS] Timeout precision verified: 0.0s instant fail, 0.3s bounded window")


def test_adversarial_multithread_writer_race(tmp_path: Path):
    """Verify mutual exclusion across 10 threads racing in the same process."""
    print("  -> Testing multi-threaded writer_lock serialization (10 threads x 20 iters)...")
    import threading
    data_root = tmp_path / "data_threads"
    counter_file = data_root / "counter.txt"
    data_root.mkdir(parents=True, exist_ok=True)
    counter_file.write_text("0", encoding="utf-8")
    
    num_threads = 10
    iters_per_thread = 20
    
    def worker(tid: int):
        for i in range(iters_per_thread):
            with writer_lock(data_root, timeout=10.0, diagnostic_info={"tid": tid, "iter": i}):
                val = int(counter_file.read_text(encoding="utf-8").strip())
                time.sleep(0.001)
                counter_file.write_text(str(val + 1), encoding="utf-8")
                
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
        
    final_val = int(counter_file.read_text(encoding="utf-8").strip())
    expected_val = num_threads * iters_per_thread
    assert final_val == expected_val, f"Thread race failed! {final_val} != {expected_val}"
    print(f"     [PASS] Thread serialization verified: exact {final_val}/{expected_val} increments")


def test_adversarial_instance_isolation_same_process(tmp_path: Path):
    """Verify two distinct AdvisoryLock instances in same process mutually exclude each other."""
    print("  -> Testing distinct instance mutual exclusion within same process...")
    data_root = tmp_path / "data_isolation"
    
    lk1 = writer_lock(data_root)
    lk2 = writer_lock(data_root)
    
    lk1.acquire()
    try:
        try:
            lk2.acquire()
            raise AssertionError("lk2 acquired lock while lk1 held it in same process!")
        except LockContentionError:
            pass
    finally:
        lk1.release()
        
    # After lk1 released, lk2 can acquire
    lk2.acquire()
    lk2.release()
    print("     [PASS] Distinct instance mutual exclusion within same process verified")


# ==============================================================================
# Main Runner
# ==============================================================================

def run_all_adversarial_tests():
    print("=" * 70)
    print("GATE G0: ADVERSARIAL STRESS TEST SUITE (EMPIRICAL CHALLENGER)")
    print("=" * 70)
    
    start_total = time.monotonic()
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        test_adversarial_multiprocess_writer_race(tpath)
        test_adversarial_nonblocking_contention_storm(tpath)
        test_adversarial_sigkill_crash_recovery_cascade(tpath)
        test_adversarial_shared_vs_exclusive_query_lock_matrix(tpath)
        test_adversarial_lock_file_inode_permanence(tpath)
        test_adversarial_corrupted_diagnostic_resilience(tpath)
        test_adversarial_timeout_precision(tpath)
        test_adversarial_multithread_writer_race(tpath)
        test_adversarial_instance_isolation_same_process(tpath)
        
    total_duration = time.monotonic() - start_total
    print("=" * 70)
    print(f"ADVERSARIAL STRESS TESTS: ALL 9 SUITES PASSED in {total_duration:.2f}s")
    print("=" * 70)


if __name__ == "__main__":
    run_all_adversarial_tests()

