# Start here — Portable Project Context

**Status**: Fully Implemented & Certified Platform (Gates G0–G6 Certified, `229/229` Automated `pytest` Tests Passing, `41,474,517` Real Official NYC TLC Trips Ingested & Published).

This repository contains the complete, production-grade local data platform for NYC TLC High-Volume For-Hire Vehicle (HVFHV) monthly data reliability. All architectural requirements (`R01`–`R26`) and verification scenarios (`T01`–`T29`) have been implemented and empirically validated with zero mock facades, including full execution on official NYC TLC `2024-12` (`21,068,851` rows) and `2025-01` (`20,405,666` rows) Parquet releases.

---

## 1. Reading Order

1. **[README.md](README.md)**: Architectural overview, core mathematical invariants, real NYC TLC dataset telemetry, layout, and quickstart commands.
2. **[Execution Log](docs/verification/execution-log.md)**: Chronological verification receipts, exact test evidence across Gates G0 through G6, and Phase 2 real 2-month NYC TLC execution receipt.
3. **[Requirements Matrix](docs/requirements.md)**: Traceability matrix mapping requirements R01–R26 to gates and test scenarios.
4. **[Gate G2 Review Report](docs/verification/gate2_pause_report.md)** & **[ADR 0006 (Option A Retention)](docs/architecture/decisions/0006-amend-release-retention-to-option-a.md)**: Real monthly vertical slice execution metrics and formalized Option A retention amendment.
5. **[Architecture Overview](docs/architecture/overview.md)** & **[Low-Level Design](docs/architecture/low-level-design.md)**: Complete system design, data models, and storage contracts.

---

## 2. Certified System Capabilities

- **Milestone M0 (Gate G0)**: Clean environment compatibility on Python 3.14, SQLite durability pragmas (`EXTRA`, `fullfsync`), POSIX advisory locking (`writer.lock`, `query.lock`), and DuckDB 2.0.0 / PyArrow 26.0.1 native C-extensions. (28/28 tests PASS).
- **Milestone M1 (Gate G1)**: Trustworthy acquisition, bounded HTTP streaming, full Parquet decoding verification, raw SHA-256 byte store, RFC 8785 canonical JSON hashing, and SQLite control plane state machine. Verified on real `taxi_zone_lookup.csv` (`265` rows, SHA-256 `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`), `fhvhv_tripdata_2024-12.parquet` (`21,068,851` rows, SHA-256 `dcf0885722bc76ab7b5f22a23b784513caf7e7c2b23381ed854610c436c48887`), and `fhvhv_tripdata_2025-01.parquet` (`20,405,666` rows, SHA-256 `fcdef276f4e9887f2beab30590165cdea74caef50a1313d71fa01ff6afacca3a`). (31/31 tests PASS).
- **Milestone M2 (Gate G2)**: Genuine DuckDB vertical slice on real December 2024 (`21,068,851` rows, `rel_2024_12_3f8bf865`, `p50=268.0s`, `p95=716.0s`) and combined December 2024 + January 2025 (`41,474,517` rows, `rel_2024_12_2025_01_f2d153b3`, `p50=249.0s`, `p95=666.0s`) TLC Parquet data, naive 64-bit microsecond interval arithmetic, 3-class interval partition, Type 7 continuous quantiles, pre-seal quality rules S01–S07, profiles Q01–Q09, and 0-byte post-checkpoint WAL. (18/18 tests PASS).
- **Milestone M3 (Gate G3)**: Candidate finalization, Rule S08 post-seal manifest verification, atomic Compare-and-Swap (CAS) publication, crash recovery (`T18`), atomic rollback (`T19`), and SQLite online backup (`T24`). (14/14 tests PASS).
- **Milestone M4 (Gate G4)**: Incremental rebuild equivalence vs full rebuild, writer lock contention, 80 GiB budget preflight, and Option A retention policy enforcement formalized in **[ADR 0006](docs/architecture/decisions/0006-amend-release-retention-to-option-a.md)** (protects current + 1 prior analytical release; `20.75 GiB` active 2-month footprint with `59.25 GiB` headroom under the `80.0 GiB` budget). (5/5 tests PASS).
- **Milestone M5 (Gate G5)**: Streamlit read-only consumer with dual views (Reliability and Recorded Intervals), mandatory out-of-cache eligibility checks, `query.lock` concurrency, bounded LRU cache, and Dagster asset checks & frontier gap detection. (12/12 tests PASS).
- **Milestone M6 (Gate G6)**: Independent requirement audit across all R01–R26, finalized execution log, real 2-month NYC TLC receipt (`.data/real_tlc_execution_receipt.json`), and full system regression across all gates (`229/229` `pytest` tests PASS, `108/108` gate runner assertions PASS, 100%).

---

## 3. Verified Verification Commands

Run the full platform test suite from the repository root:
```bash
# Unified pytest execution across all acceptance and adversarial challenger suites (229 tests)
PYTHONPATH=src:. .venv/bin/pytest -q

# Individual gate suite runners (108 tests)
export PYTHONPATH=src
.venv/bin/python3 tests/gate0_compatibility/run_gate0_tests.py
.venv/bin/python3 tests/gate1_acquisition/run_gate1_tests.py
.venv/bin/python3 tests/gate2_monthly_slice/run_gate2_tests.py
.venv/bin/python3 tests/gate3_publication_safety/run_gate3_tests.py
.venv/bin/python3 tests/gate4_scale/run_gate4_tests.py
.venv/bin/python3 tests/gate5_consumer/run_gate5_tests.py
```
