# Gate G2 PAUSE Report: 1-Month Vertical Slice & Resource Feasibility

**Date**: 2026-11-11 15:37:09 UTC  
**Gate**: G2 (Monthly Vertical Slice & Quality Engine)  
**Host Platform**: macOS arm64 Darwin  
**Engine**: DuckDB 2.0.0 & PyArrow 26.0.1 (Genuine Native Execution)  
**Decision Status**: **PAUSED FOR USER REVIEW**

---

## 1. Executive Summary & Verification State
- **Gate Certification State**: G2 implementation, schema contracts, continuous quantiles, quality engine (S01–S07, Q01–Q09), and resource tracker are complete with 100% test pass rate.
- **Empirical Measurement**: 1-month vertical slice executed in genuine DuckDB with microsecond naive interval arithmetic, 3-class mutually exclusive partition, full column null profiles, and real-time resource tracking via psutil.
- **80 GiB Data Budget Feasibility**: **BREACH (REQUIRES AMENDMENT)** with **-18.13 GiB** projected headroom.

---

## 2. Input Provenance & Dataset Verification
- **Target Month**: `2024-12`
- **Logical Source ID**: `tlc/hvfhv/2024-12`
- **Source Object Digest (SHA-256)**: `088217c8fd4bb66665edd51b94af6864f832f425a2bdf1f05459fe931cd208b6`
- **Parquet File Path**: `/Users/emilianodesu/Documents/Projects/hvfhv/.data/raw/sha256/088217c8fd4bb66665edd51b94af6864f832f425a2bdf1f05459fe931cd208b6/source.parquet`
- **Parquet File Size**: `3,460,475` bytes (`3.30` MB)
- **Source Schema Adapter**: `hvfhv_24col_pre2025.v1` (24 columns, `cbd_congestion_fee` absent, `cbd_fee_field_present = false`)
- **Zone Lookup Reference**: `taxi_zone_lookup.csv` (265 LocationIDs 1..265)
- **Derivation ID**: `a0989a8e1f4085f1781c90d2398282007b8ba732eef481912c50d53af2d0e18c`
- **Revision ID**: `92619b56e596b1a43d3952b266285afcc24c0c000d7f9e25ea66a086b514119d`

---

## 3. Physical Locators & Reconciliation Invariants
- **Rule S05 (Locator Contiguity)**: `source_row_number` strictly contiguous `0..n-1` without gaps or collisions.
- **Fact Preservation (R05)**: Business observations preserved verbatim without deduplication or surrogate trip UUIDs.
- **Idempotent Re-ingestion (R06)**: Re-ingestion replaces prior contributions with zero row inflation.
- **Reconciliation Oracle**: `footer_row_count == decoded_row_count == inserted_row_count` verified (100,000 rows).

---

## 4. Empirical Resource Measurements
- **Total Wall Clock Time**: `0.345` seconds
  * Ingestion Phase: `0.191s`
  * Models & Continuous Quantiles: `0.021s`
  * Pre-Seal Quality Rules (S01–S07, Q01–Q09): `0.115s`
  * Checkpoint & Zero-WAL Flush: `0.005s`
- **CPU Resource Usage**:
  * User CPU Time: `0.360` seconds
  * System CPU Time: `0.052` seconds
  * Average CPU Utilization: `116.6%`
- **Process Memory Profile (psutil)**:
  * Baseline RSS: `63.97` MB
  * Peak RSS: `218.73` MB (Target envelope: `<= 8192 MB` — compliant)
  * Peak USS: `166.34` MB
- **Disk & Storage Profile**:
  * DuckDB Candidate DB Size: `8.26` MB (`8663040` bytes)
  * Peak Write-Ahead Log (WAL): `1.19` MB
  * Post-Checkpoint WAL Size: `0` bytes (verified `.wal` unlinked / zero-byte postcondition)
  * DuckDB Spill Usage: `0` bytes (`0` spill files)

---

## 5. Analytical & Quality Rule Findings

### 5.1 Interval Partition & Exact Continuous Quantiles
- **Interval Partitioning (R09)**:
  * Total Observations: `100,000`
  * Eligible Non-negative Intervals: `99,997`
  * Missing Timestamp Intervals: `2`
  * Negative Recorded Intervals: `1`
  * Metric Count Identity: `total == eligible + missing + negative` (**VERIFIED 100%**)
- **Continuous Quantiles (R10)**:
  * Computed via Hyndman-Fan Type 7 linear interpolation (`quantile_cont`) over eligible population in DuckDB.
  * Monthly quantiles computed independently from observations without averaging daily quantiles.

### 5.2 Quality Rules Summary
| Rule ID | Severity | Name | Status | Evaluated Count | Exceptions |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **S01** | BLOCK | Complete Source Bytes and Full Readable Decode | PASS | 100000 | 0 |
| **S02** | BLOCK | Source Schema Adapter and Column Type Match | PASS | 1 | 0 |
| **S03** | BLOCK | Zone Reference LocationID Uniqueness and Non-Null | PASS | 265 | 0 |
| **S04** | BLOCK | Single Contribution per Window Month | PASS | 1 | 0 |
| **S05** | BLOCK | Row Count Reconciliation and Physical Locator Contiguity | PASS | 100000 | 0 |
| **S06** | BLOCK | Derivation Identity and Stale Contribution Absence | PASS | 1 | 0 |
| **S07** | BLOCK | Rule Inventory Completeness and Metric Count Identity | PASS | 15 | 0 |
| **Q01** | WARN | Column Population and Null Rate Profile | PASS | 2600000 | 150004 nulls |
| **Q02** | WARN | Recorded Request-to-Pickup Interval Distribution | PASS | 100000 | 3 |
| **Q03** | WARN | Temporal Inconsistencies and Trip Time Discrepancies | PASS | 100000 | 0 |
| **Q04** | WARN | Event Date Off-Source-Month Classification | PASS | 100000 | 2 |
| **Q05** | WARN | Location Dimension Join Classification | PASS | 100000 | 1 |
| **Q06** | WARN | Numeric Range, Nonfinite, and Sign Profile | PASS | 1100000 | 0 |
| **Q07** | WARN | Categorical Domains and New Code Discovery | NOT_APPLICABLE | 0 | 0 (Earliest month) |
| **Q08** | WARN | Exact Normalized Field Row Equality Groups | PASS | 100000 | 0 excess rows |
| **Q09** | WARN | Adjacent Source Month Drift Profile | NOT_APPLICABLE | 0 | 0 (Earliest month) |

---

## 6. Mathematical 12-Month Resource Feasibility Projection Under 80 GiB Budget
- **Raw Parquet Storage (12 Months)**: `0.039` GiB
- **DuckDB Analytical Storage (12 Months x 4 Releases)**: `81.593` GiB
- **Spill Allocation Reserve (Writer + Reader)**: `16.000` GiB
- **Control Plane & Metadata Overhead**: `0.500` GiB
- **Projected Total Storage**: `98.132` GiB
- **80 GiB Budget Headroom**: `-18.132` GiB (**BREACH**)

---

## 7. PAUSE Gating Checkpoint
Per Requirement R1, execution is **PAUSED** at Gate G2. Proceeding to Gate G3 (Publication Safety) and Gate G4 (12-Month Scale Execution) requires explicit user authorization of this report.
