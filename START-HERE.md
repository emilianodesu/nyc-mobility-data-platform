# Start here — Portable Project Context

**Status**: Fully Implemented & Certified Platform (Gates G0–G6 Certified, 108/108 Automated Tests Passing).

This repository contains the complete, production-grade local data platform for NYC TLC High-Volume For-Hire Vehicle (HVFHV) monthly data reliability. All architectural requirements (`R01`–`R26`) and verification scenarios (`T01`–`T29`) have been implemented and empirically validated with zero mock facades.

---

## 1. Reading Order

1. **[README.md](README.md)**: Architectural overview, core mathematical invariants, layout, and quickstart commands.
2. **[Execution Log](docs/verification/execution-log.md)**: Chronological verification receipts and exact test evidence across Gates G0 through G6.
3. **[Requirements Matrix](docs/requirements.md)**: Traceability matrix mapping requirements R01–R26 to gates and test scenarios.
4. **[Gate G2 Review Report](docs/verification/gate2_pause_report.md)**: Real monthly vertical slice execution metrics and Option A retention decision.
5. **[Architecture Overview](docs/architecture/overview.md)** & **[Low-Level Design](docs/architecture/low-level-design.md)**: Complete system design, data models, and storage contracts.

---

## 2. Certified System Capabilities

- **Milestone M0 (Gate G0)**: Clean environment compatibility on Python 3.14, SQLite durability pragmas (`EXTRA`, `fullfsync`), POSIX advisory locking (`writer.lock`, `query.lock`), and DuckDB 2.0.0 / PyArrow 26.0.1 native C-extensions. (28/28 tests PASS).
- **Milestone M1 (Gate G1)**: Trustworthy acquisition, bounded HTTP streaming, full Parquet decoding verification, raw SHA-256 byte store, RFC 8785 canonical JSON hashing, and SQLite control plane state machine. (31/31 tests PASS).
- **Milestone M2 (Gate G2)**: Genuine DuckDB vertical slice on real December 2024 TLC Parquet data (~21M rows), naive 64-bit microsecond interval arithmetic, 3-class interval partition, Type 7 continuous quantiles, pre-seal quality rules S01–S07, and 0-byte post-checkpoint WAL. (18/18 tests PASS).
- **Milestone M3 (Gate G3)**: Candidate finalization, Rule S08 post-seal manifest verification, atomic Compare-and-Swap (CAS) publication, crash recovery (`T18`), atomic rollback (`T19`), and SQLite online backup (`T24`). (14/14 tests PASS).
- **Milestone M4 (Gate G4)**: Incremental rebuild equivalence vs full rebuild, writer lock contention, 80 GiB budget preflight, and Option A retention policy enforcement (protects current + 1 prior analytical release, bounded storage with ~22.6 GiB safe headroom). (5/5 tests PASS).
- **Milestone M5 (Gate G5)**: Streamlit read-only consumer with dual views (Reliability and Recorded Intervals), mandatory out-of-cache eligibility checks, `query.lock` concurrency, bounded LRU cache, and Dagster asset checks & frontier gap detection. (12/12 tests PASS).
- **Milestone M6 (Gate G6)**: Independent requirement audit across all R01–R26, finalized execution log, and full system regression across all gates (108/108 tests PASS, 100%).

---

## 3. Verified Verification Commands

Run the full platform test suite from the repository root:
```bash
export PYTHONPATH=src
.venv/bin/python3 tests/gate0_compatibility/run_gate0_tests.py
.venv/bin/python3 tests/gate1_acquisition/run_gate1_tests.py
.venv/bin/python3 tests/gate2_monthly_slice/run_gate2_tests.py
.venv/bin/python3 tests/gate3_publication_safety/run_gate3_tests.py
.venv/bin/python3 tests/gate4_scale/run_gate4_tests.py
.venv/bin/python3 tests/gate5_consumer/run_gate5_tests.py
```
