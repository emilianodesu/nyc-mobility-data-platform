# Contributing to the HVFHV Data Reliability Platform

All contributions to this platform must adhere strictly to the engineering invariants, architectural contracts, and verification gates established in the repository.

---

## 1. Principles & Invariants

1. **Preserve Mathematical & Semantic Invariants**:
   - Timestamp arithmetic must remain 64-bit microsecond integer subtraction divided by `1,000,000.0` on naive source timestamps without timezone manipulation.
   - Percentiles must be computed as Type 7 continuous quantiles (`quantile_cont`) directly over eligible trips; precomputed daily percentiles must never be averaged across dates or zones.
   - The 3-class interval count identity must strictly hold:
     $$\text{total} = \text{eligible\_nonnegative} + \text{negative\_recorded\_interval} + \text{missing\_timestamp}$$
2. **Never Bypass Access Controls or Integrity Gates**:
   - Mutating operations must strictly hold the advisory POSIX `writer.lock`.
   - Analytical queries must respect `query.lock` and perform out-of-cache eligibility verification against SQLite `registry.sqlite` both before query execution and after query completion.
   - Never mark a test PASS without genuine execution evidence.
3. **Storage & Retention Compliance**:
   - Total project data under `$DATA_ROOT` must strictly not exceed the 80.0 GiB budget.
   - Analytical release retention must follow **Option A**: retain the current verified release plus exactly 1 prior release (2 analytical releases total). Older analytical databases are unlinked while manifests and validation receipts remain permanently preserved for provenance and audit.
4. **Git & Repository Boundary**:
   - This project maintains a strict local-only repository. Never configure remote publishing, push to public networks, or transmit raw data outside the local workspace.

---

## 2. Verification Protocol

Any proposed changes must pass the full regression test suite across all 6 gates:
```bash
export PYTHONPATH=src
.venv/bin/python3 tests/gate0_compatibility/run_gate0_tests.py
.venv/bin/python3 tests/gate1_acquisition/run_gate1_tests.py
.venv/bin/python3 tests/gate2_monthly_slice/run_gate2_tests.py
.venv/bin/python3 tests/gate3_publication_safety/run_gate3_tests.py
.venv/bin/python3 tests/gate4_scale/run_gate4_tests.py
.venv/bin/python3 tests/gate5_consumer/run_gate5_tests.py
```

All test receipts and execution measurements must be documented in `docs/verification/execution-log.md` with timestamps, exact command strings, and verified output metrics.
