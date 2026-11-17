# Contributing to the HVFHV Data Reliability Platform

Contributions must preserve the platform's correctness invariants, storage constraints, and automated gate checks (`G0`–`G6`).

## 1. Engineering Invariants

1. **Mathematical & Semantic Fidelity**:
   - Compute request-to-pickup intervals with 64-bit microsecond integer subtraction divided by `1,000,000.0` on naive source timestamps without timezone fabrication.
   - Compute percentiles via Type 7 continuous quantiles (`quantile_cont`) over the exact eligible population; never average precomputed daily or zone quantiles.
   - Preserve the 3-class partition identity at every aggregation boundary:
     $$\text{total} = \text{eligible\_nonnegative} + \text{negative\_recorded\_interval} + \text{missing\_timestamp}$$
2. **Lock & Publication Safety**:
   - All mutating operations must acquire the advisory POSIX `writer.lock`.
   - Consumer queries must respect `query.lock` and run out-of-cache eligibility validation against SQLite (`registry.sqlite`) before and after query execution.
3. **Storage & Data Hygiene**:
   - Keep `$DATA_ROOT` (`HVFHV_DATA_ROOT` or `.data/`) outside Git history; never commit `.parquet`, `.duckdb`, or `.sqlite` runtime artifacts.
   - Maintain Option A release retention (`current` + `1` prior verified analytical DuckDB file) within the `80.0 GiB` budget.

## 2. Verification Protocol

Run the full automated suite before submitting changes:

```bash
pytest -q
```

You can also invoke individual gate harnesses directly (`PYTHONPATH=src python3 tests/gate<0..5>_*/run_gate<0..5>_tests.py`). Any change affecting schema contracts, quality rules, or storage bounds should include an accompanying test and ADR/documentation update.
