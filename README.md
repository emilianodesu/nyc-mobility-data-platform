# NYC TLC High-Volume For-Hire Vehicle (HVFHV) Monthly Data Reliability Platform

A production-grade, local analytical data engineering platform for making New York City Taxi & Limousine Commission (TLC) High Volume For-Hire Vehicle trip data trustworthy, traceable, inspectable, and mathematically reproducible.

Built for local execution on macOS (Apple Silicon / APFS) and POSIX systems with zero cloud dependencies, zero container requirements, and zero remote network leaks.

---

## 1. System Architecture & Engineering Principles

```
                  [ Official NYC TLC Sources (Parquet / CSV) ]
                                      │
                   Bounded Download & Full Decode (G1)
                                      ▼
             [ $DATA_ROOT/raw/sha256/<hash>/source.parquet ]
                   (Immutable Accepted Raw Byte Store)
                                      │
               Streaming PyArrow Batches (65,536 rows)
                                      ▼
               [ Embedded DuckDB Engine: Staging Candidate ]
                   ├── Zero-based physical locators
                   ├── Schema adaptation (Pre-2025 vs 2025+)
                   ├── Naive 64-bit microsecond interval arithmetic
                   ├── 3-class mutually exclusive interval partition
                   └── Type 7 continuous quantiles (quantile_cont)
                                      │
                    Pre-Seal Quality Engine (G2, S01–S07)
                    Semantic Column Profiles (G2, Q01–Q09)
                    Post-Seal Manifest Digest Check (G3, S08)
                                      ▼
                 [ SQLite 3 Control Plane (registry.sqlite) ]
                   ├── POSIX advisory locking (writer.lock)
                   ├── Atomic Compare-and-Swap (CAS) publication
                   ├── Unacknowledged commit crash recovery (T18)
                   └── Option A Retention Policy (<= 80 GiB Cap, G4)
                                      │
                     Verified Analytical Releases (G3)
                                      ▼
                 [ Read-Only Consumer Interface (G5) ]
                   ├── Streamlit Interactive Dashboard (app.py)
                   │    ├── Data Reliability View (coverage, S01–S08, Q01–Q09)
                   │    └── Recorded Intervals View (exact p50/p95, denominators)
                   ├── Out-of-cache SQLite eligibility checks (T21, T27)
                   ├── POSIX query.lock concurrency barrier
                   └── Bounded in-memory LRU cache (T28)
```

### Core Invariants & Mathematical Guarantees
1. **Raw Traceability & Anti-Inflation**: Every trip observation is traceable to an immutable accepted raw Parquet file via physical tuple locators `(revision_id, source_row_number)`. Re-ingestion of identical or updated revisions purges historical facts without row count inflation.
2. **Naive Microsecond Arithmetic**: Request-to-pickup intervals are computed via exact 64-bit microsecond integer subtraction divided by `1,000,000.0` on naive source timestamps without invented timezone conversions or assumptions.
3. **3-Class Mutually Exclusive Partition**: Every observation belongs to exactly one interval class:
   $$\text{total} = \text{eligible\_nonnegative} + \text{negative\_recorded\_interval} + \text{missing\_timestamp}$$
4. **Type 7 Continuous Quantiles & Non-Averaging**: Median ($p50$) and 95th percentile ($p95$) are evaluated directly over eligible trips using continuous linear interpolation (`quantile_cont`). Daily quantiles are never averaged across dates or zones.
5. **Atomic Publication & Recovery**: Releases are published via atomic compare-and-swap (CAS) on generation counters. Process kills during staging, rename, or post-commit recover idempotently without duplicate generations or partial database exposure.
6. **Option A Retention Policy**: The platform maintains the current analytical release plus exactly 1 prior analytical release (2 analytical releases total). Older databases are safely unlinked to reclaim storage while audit manifests (`manifest.json`) and validation receipts (`validation.json`) are preserved permanently. Total storage is strictly bounded within $\le 80.0\text{ GiB}$ with $\sim 22.6\text{ GiB}$ safe headroom.

---

## 2. Platform Verification & Certification Status

All 7 staged gates (`G0` through `G6`), 26 architectural requirements (`R01`–`R26`), and 29 verification scenarios (`T01`–`T29`) have been fully implemented, tested, and certified with genuine automated execution evidence.

| Gate | Milestone | Acceptance Criteria | Tests Executed | Status |
| :--- | :--- | :--- | :--- | :--- |
| **G0** | Compatibility & Dependency Lock | Python 3.14, SQLite pragmas, POSIX locking, DuckDB 2.0.0 & PyArrow 26.0.1 | 28 / 28 PASS | **CERTIFIED PASS** |
| **G1** | Trustworthy Acquisition & Control | Bounded HTTP streaming, raw provenance, RFC 8785 canonical hashes, 15-table SQLite control plane | 31 / 31 PASS | **CERTIFIED PASS** |
| **G2** | Monthly Vertical Slice & Quality | Real December 2024 TLC Parquet (~21M rows), 7 pre-seal rules S01–S07, profiles Q01–Q09, 0-byte post-checkpoint WAL | 18 / 18 PASS | **CERTIFIED PASS** |
| **G3** | Publication Safety & Durability | Candidate finalization, Rule S08 digest check, atomic CAS publication, crash recovery, SQLite online backup | 14 / 14 PASS | **CERTIFIED PASS** |
| **G4** | Scale & Option A Retention | Incremental vs full rebuild equivalence, writer lock contention, 80 GiB budget preflight, Option A retention enforcement | 5 / 5 PASS | **CERTIFIED PASS** |
| **G5** | Consumer & Operations | Streamlit Reliability & Intervals views, out-of-cache eligibility, query.lock, bounded LRU cache, Dagster asset checks & gaps scan | 12 / 12 PASS | **CERTIFIED PASS** |
| **G6** | Portfolio Evidence & Walkthrough | Independent requirement audit (R01–R26), execution log finalization, end-to-end regression across all gates | 108 / 108 PASS | **CERTIFIED PASS** |

---

## 3. Directory Layout

```
.
├── contracts/                        # Frozen schemas, pre-seal rules, and metric inventories
│   ├── rules.json                    # S01–S08 rules, Q01–Q09 semantic profiles
│   └── schemas.json                  # Source schema adapters (pre-2025, 2025+)
├── docs/                             # Architecture records, decisions, requirements, evidence
│   ├── architecture/                 # arc42 architectural documentation & ADRs
│   ├── requirements.md               # R01–R26 requirements matrix
│   └── verification/
│       ├── execution-log.md          # Chronological execution receipts across all gates
│       └── gate2_pause_report.md     # Gate G2 pause review report
├── src/hvfhv/                        # Core application implementation
│   ├── acquisition/                  # Bounded streaming download & raw acceptance
│   │   ├── acquire.py
│   │   └── discover.py
│   ├── config.py                     # System configuration & filesystem layout
│   ├── consumer/                     # Read-only consumer interfaces
│   │   ├── app.py                    # Streamlit analytical dashboard & headless runner
│   │   └── reader.py                 # Pinned release reader, LRU cache, out-of-cache checks
│   ├── contracts/                    # RFC 8785 canonical JSON & hash identity
│   │   └── identity.py
│   ├── control/                      # SQLite control plane, advisory locks, publication CAS
│   │   ├── locks.py                  # POSIX advisory flock (writer.lock, query.lock)
│   │   ├── publication.py            # Finalization, post-seal verification, CAS publication
│   │   └── registry.py               # 15 relational tables, state machines, migrations
│   ├── orchestration/                # Dagster orchestration adapters & frontier gap scanner
│   │   └── defs.py
│   └── transforms/                   # Ingestion, SQL analytical models, quality engine
│       ├── builder.py                # Full rebuild, incremental build, Option A retention
│       ├── ingest.py                 # PyArrow streaming batch reader & DuckDB writer
│       └── quality.py                # Quality engine evaluating S01–S07 and Q01–Q09
└── tests/                            # Comprehensive automated acceptance suites
    ├── gate0_compatibility/          # G0: Environment, pragmas, advisory locks, DuckDB/Arrow
    ├── gate1_acquisition/            # G1: Discovery, HTTP errors, provenance, control plane
    ├── gate2_monthly_slice/          # G2: Real monthly slice, interval arithmetic, quantiles
    ├── gate3_publication_safety/     # G3: Crash recovery, atomic rollback, CAS concurrency
    ├── gate4_scale/                  # G4: Rebuild equivalence, Option A retention, 80GB budget
    └── gate5_consumer/               # G5: Streamlit queries, cache bounds, frontier gaps
```

---

## 4. Quickstart & Verification Commands

### Environment Setup
The platform requires Python 3.11+ (verified on CPython 3.14.3 macOS Apple Silicon).
```bash
# Set Python path to include source tree
export PYTHONPATH=src

# Run complete system regression across all gates (108 tests)
.venv/bin/python3 tests/gate0_compatibility/run_gate0_tests.py
.venv/bin/python3 tests/gate1_acquisition/run_gate1_tests.py
.venv/bin/python3 tests/gate2_monthly_slice/run_gate2_tests.py
.venv/bin/python3 tests/gate3_publication_safety/run_gate3_tests.py
.venv/bin/python3 tests/gate4_scale/run_gate4_tests.py
.venv/bin/python3 tests/gate5_consumer/run_gate5_tests.py
```

### Running Individual Gate Suites
```bash
# Gate G0: Environment, SQLite durability pragmas, and POSIX advisory locking
PYTHONPATH=src .venv/bin/python3 tests/gate0_compatibility/run_gate0_tests.py

# Gate G1: Acquisition, HTTP network fault injection, and SQLite control plane
PYTHONPATH=src .venv/bin/python3 tests/gate1_acquisition/run_gate1_tests.py

# Gate G2: Real monthly vertical slice, continuous quantiles, and pre-seal quality engine
PYTHONPATH=src .venv/bin/python3 tests/gate2_monthly_slice/run_gate2_tests.py

# Gate G3: Publication safety, CAS concurrency, crash injection, and SQLite online backup
PYTHONPATH=src .venv/bin/python3 tests/gate3_publication_safety/run_gate3_tests.py

# Gate G4: Incremental rebuild equivalence, lock contention, and Option A retention cleanup
PYTHONPATH=src .venv/bin/python3 tests/gate4_scale/run_gate4_tests.py

# Gate G5: End-to-end consumer reader, out-of-cache eligibility, and frontier gap scanner
PYTHONPATH=src .venv/bin/python3 tests/gate5_consumer/run_gate5_tests.py
```

### Running the Read-Only Consumer Dashboard
```bash
# Launch interactive Streamlit dashboard on local loopback
PYTHONPATH=src .venv/bin/streamlit run src/hvfhv/consumer/app.py

# Or execute headless consumer summary for automated inspection
PYTHONPATH=src .venv/bin/python3 -c "
from hvfhv.consumer.reader import ReleaseReader
from hvfhv.consumer.app import render_headless_summary
from hvfhv.config import resolve_data_root

reader = ReleaseReader(data_root=resolve_data_root())
summary = render_headless_summary(reader)
print('Current Release:', summary['reliability']['release_id'])
print('Total Trips:', summary['intervals']['denominators']['total_population_count'])
print('Median Interval:', summary['intervals']['denominators']['median_seconds'], 's')
"
```

---

## 5. Explicit Disclosures & Domain Invariants
- **Naive Interval Arithmetic**: Recorded request-to-pickup seconds are computed from timestamps as recorded by high-volume dispatchers without timezone manipulation.
- **Non-SLA Disclaimer**: The platform measures recorded vehicle request-to-pickup interval; it does not claim to represent customer waiting time, fleet availability, or dispatcher SLA adherence.
- **Data Boundary**: Local file data is stored strictly in `$DATA_ROOT` (`.data/` within project root, gitignored). Under Option A retention, analytical storage is capped under 80 GiB.
- **Local Git Repository Boundary**: All commits are made strictly to the local repository (`main` branch); no remote publishing or unauthorized external data transmission is performed.
