"""Platform configuration and resource definitions for HVFHV.

Resolves storage roots, resource limits, and runtime parameters.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ROOT = DEFAULT_PROJECT_ROOT / ".data"


def resolve_data_root(custom_path: Path | str | None = None) -> Path:
    """Resolve and return the canonical DATA_ROOT path.

    Precedence:
    1. Explicit custom_path argument
    2. HVFHV_DATA_ROOT environment variable
    3. Default workspace .data path: <project_root>/.data
    """
    if custom_path is not None:
        p = Path(custom_path).resolve()
    elif "HVFHV_DATA_ROOT" in os.environ:
        p = Path(os.environ["HVFHV_DATA_ROOT"]).resolve()
    else:
        p = DEFAULT_DATA_ROOT.resolve()
    return p


@dataclass(frozen=True)
class ResourceCaps:
    """Resource bounds for analytical query and transformation engines."""
    threads: int
    memory_limit: str
    max_temp_directory_size: str
    spill_directory: Path


@dataclass(frozen=True)
class SystemConfig:
    """System-wide configuration settings."""
    data_root: Path
    data_budget_bytes: int = 80 * (1024 ** 3)  # 80 GiB
    sqlite_busy_timeout_ms: int = 5000
    http_connect_timeout_seconds: float = 10.0
    http_read_timeout_seconds: float = 60.0
    http_max_retries: int = 3

    @property
    def control_dir(self) -> Path:
        return self.data_root / "control"

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def staging_dir(self) -> Path:
        return self.data_root / "staging"

    @property
    def releases_dir(self) -> Path:
        return self.data_root / "releases"

    @property
    def spill_dir(self) -> Path:
        return self.data_root / "spill"

    def get_writer_caps(self, attempt_id: str | None = None) -> ResourceCaps:
        """Writer caps: 4 threads, 8GB memory, 14GB spill."""
        if attempt_id:
            spill = self.staging_dir / attempt_id / "spill_writer"
        else:
            spill = self.staging_dir / "spill_writer"
        return ResourceCaps(
            threads=4,
            memory_limit="8GB",
            max_temp_directory_size="14GB",
            spill_directory=spill,
        )

    def get_reader_caps(self) -> ResourceCaps:
        """Reader caps: 1 thread, 1GB memory, 2GB spill."""
        return ResourceCaps(
            threads=1,
            memory_limit="1GB",
            max_temp_directory_size="2GB",
            spill_directory=self.spill_dir,
        )

    def ensure_directories(self) -> None:
        """Ensure all required root directories exist."""
        for d in (self.control_dir, self.raw_dir, self.staging_dir, self.releases_dir, self.spill_dir):
            d.mkdir(parents=True, exist_ok=True)


def get_default_config() -> SystemConfig:
    """Obtain default SystemConfig using resolved DATA_ROOT."""
    root = resolve_data_root()
    return SystemConfig(data_root=root)
