"""Resource Measurement Harness for Gate G2 Vertical Slice.

Samples wall clock time, CPU user/system times, process RSS/USS,
DuckDB database size, WAL growth, and spill files at 50ms intervals.
Supports psutil with standard-library resource/time fallback.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
import resource
import threading
import time
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None


@dataclass
class ResourceMetrics:
    wall_time_seconds: float = 0.0
    cpu_user_seconds: float = 0.0
    cpu_system_seconds: float = 0.0
    cpu_percent_avg: float = 0.0
    baseline_rss_mb: float = 0.0
    peak_rss_mb: float = 0.0
    peak_uss_mb: float = 0.0
    duckdb_file_bytes: int = 0
    duckdb_peak_wal_bytes: int = 0
    duckdb_peak_spill_bytes: int = 0
    spill_file_count: int = 0
    phase_timings: dict[str, float] = field(default_factory=dict)


class ResourceTracker:
    """Threaded high-resolution resource tracker sampling process memory and disk files."""

    def __init__(self, staging_dir: Path, sample_interval_s: float = 0.05):
        self.staging_dir = Path(staging_dir)
        self.sample_interval_s = sample_interval_s
        self.proc = psutil.Process(os.getpid()) if psutil is not None else None
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)

        self.peak_rss_bytes = 0
        self.peak_uss_bytes = 0
        self.peak_wal_bytes = 0
        self.peak_spill_bytes = 0
        self.spill_file_count = 0
        self.cpu_samples: list[float] = []

        self.start_wall = 0.0
        self.start_cpu_user = 0.0
        self.start_cpu_sys = 0.0
        self.baseline_rss_bytes = 0

    def _sample_rss(self) -> int:
        if self.proc is not None:
            try:
                return self.proc.memory_info().rss
            except Exception:
                pass
        # macOS resource.getrusage returns bytes
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    def _sample_uss(self) -> int:
        if self.proc is not None:
            try:
                mem = self.proc.memory_full_info()
                if hasattr(mem, "uss"):
                    return mem.uss
            except Exception:
                pass
        return self._sample_rss()

    def _monitor_loop(self):
        spill_dir = self.staging_dir / "spill_writer"
        db_path = self.staging_dir / "candidate.duckdb"
        wal_path = self.staging_dir / "candidate.duckdb.wal"

        while not self._stop_event.is_set():
            try:
                # 1. Process memory
                rss = self._sample_rss()
                if rss > self.peak_rss_bytes:
                    self.peak_rss_bytes = rss

                uss = self._sample_uss()
                if uss > self.peak_uss_bytes:
                    self.peak_uss_bytes = uss

                # 2. CPU
                if self.proc is not None:
                    try:
                        self.cpu_samples.append(self.proc.cpu_percent(interval=None))
                    except Exception:
                        pass

                # 3. WAL tracking
                if wal_path.exists():
                    wal_size = wal_path.stat().st_size
                    if wal_size > self.peak_wal_bytes:
                        self.peak_wal_bytes = wal_size

                # 4. Spill tracking
                if spill_dir.exists():
                    spill_bytes = 0
                    count = 0
                    for f in spill_dir.iterdir():
                        if f.is_file():
                            spill_bytes += f.stat().st_size
                            count += 1
                    if spill_bytes > self.peak_spill_bytes:
                        self.peak_spill_bytes = spill_bytes
                    self.spill_file_count = max(self.spill_file_count, count)
            except Exception:
                pass
            time.sleep(self.sample_interval_s)

    def start(self):
        self.baseline_rss_bytes = self._sample_rss()
        self.peak_rss_bytes = self.baseline_rss_bytes
        self.peak_uss_bytes = self._sample_uss()

        self.start_wall = time.perf_counter()
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        self.start_cpu_user = rusage.ru_utime
        self.start_cpu_sys = rusage.ru_stime

        if self.proc is not None:
            try:
                self.proc.cpu_percent(interval=None)
            except Exception:
                pass

        self._thread.start()

    def stop(self) -> ResourceMetrics:
        self._stop_event.set()
        self._thread.join(timeout=1.0)
        end_wall = time.perf_counter()

        rusage = resource.getrusage(resource.RUSAGE_SELF)
        end_cpu_user = rusage.ru_utime
        end_cpu_sys = rusage.ru_stime

        db_path = self.staging_dir / "candidate.duckdb"
        final_db_bytes = db_path.stat().st_size if db_path.exists() else 0

        avg_cpu = sum(self.cpu_samples) / max(len(self.cpu_samples), 1) if self.cpu_samples else 0.0

        return ResourceMetrics(
            wall_time_seconds=end_wall - self.start_wall,
            cpu_user_seconds=end_cpu_user - self.start_cpu_user,
            cpu_system_seconds=end_cpu_sys - self.start_cpu_sys,
            cpu_percent_avg=avg_cpu,
            baseline_rss_mb=self.baseline_rss_bytes / (1024 * 1024),
            peak_rss_mb=self.peak_rss_bytes / (1024 * 1024),
            peak_uss_mb=self.peak_uss_bytes / (1024 * 1024),
            duckdb_file_bytes=final_db_bytes,
            duckdb_peak_wal_bytes=self.peak_wal_bytes,
            duckdb_peak_spill_bytes=self.peak_spill_bytes,
            spill_file_count=self.spill_file_count,
        )
