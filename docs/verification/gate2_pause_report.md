# Gate G2 PAUSE Report: 1-Month Vertical Slice & Resource Feasibility

**Date**: 2026-11-11 15:37:09 UTC  
**Gate**: G2 (Monthly Vertical Slice & Quality Engine)  
**Host Platform**: macOS arm64 Darwin  
**Engine**: DuckDB 2.0.0 & PyArrow 26.0.1 (Genuine Native Execution)  
**Decision Status**: **PAUSED FOR USER REVIEW**

---

## 1. Executive Summary & Verification State
- **Gate Certification State**: G2 implementation, schema contracts, continuous quantiles, quality engine (S01–S07, Q01–Q09), and resource tracker are complete with 100% test pass rate (`229/229` automated `pytest` tests passing across all gates).
- **Empirical Measurement**: 1-month vertical slice executed in genuine DuckDB with microsecond naive interval arithmetic, 3-class mutually exclusive partition, full column null profiles, and real-time resource tracking via psutil. *(Note: Sections 2–6 record the initial 100,000-row sandboxed offline baseline run; **Section 8** below documents the Phase 2 full real official NYC TLC dataset execution across `21,068,851` rows for `2024-12` and `20,405,666` rows for `2025-01` = `41,474,517` total rows, together with formalization in [ADR 0006](../architecture/decisions/0006-amend-release-retention-to-option-a.md)).*
- **80 GiB Data Budget Feasibility**: **BREACH (REQUIRES AMENDMENT)** with **-18.13 GiB** projected headroom under the unamended 3-prior-release baseline; resolved via **Option A** ([ADR 0006](../architecture/decisions/0006-amend-release-retention-to-option-a.md): retain current + 1 prior verified release).

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

---

## 8. Phase 2 Real Official NYC TLC `2024-12` (21,068,851 rows) & `2025-01` (20,405,666 rows) Execution Evidence

Following the initial 100,000-row sandboxed offline baseline recorded in Sections 2–6, the full official NYC TLC CloudFront files were downloaded, cryptographically verified, decoded in full, ingested into native DuckDB `2.0.0`, evaluated across all pre-seal (`S01`–`S07`), semantic (`Q01`–`Q09`), and post-seal (`S08`) rules, and published into `$DATA_ROOT/releases/` and `$DATA_ROOT/control/registry.sqlite`. The machine-readable receipt is stored at `.data/real_tlc_execution_receipt.json` (`executed_at_utc: 2026-11-16T21:12:09.756905+00:00`), and the retention amendment is formalized in [ADR 0006](../architecture/decisions/0006-amend-release-retention-to-option-a.md).

### 8.1 Real Official NYC TLC Acquired Source Provenance

| Logical Source ID | Source URL | Schema Fingerprint | Exact Bytes | Footer / Decoded Rows | Content SHA-256 Digest | Revision ID |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `tlc/taxi_zones` | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` | `taxi_zone_lookup.v1` | `12,331` | `265` / `265` | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` | `3861f289d4635ee01da25248ccd3748ae81c975271c83e6065b67cb230cc7bfc` |
| `tlc/hvfhv/2024-12` | `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet` | `hvfhv_24col_pre2025.v1` | `507,534,115` (`484.02 MiB`) | `21,068,851` / `21,068,851` | `dcf0885722bc76ab7b5f22a23b784513caf7e7c2b23381ed854610c436c48887` | `9b2cec2bba8da4ddfdee42656beb9ddca33ea4d3d451c1b0f89cd6a35d508150` |
| `tlc/hvfhv/2025-01` | `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet` | `hvfhv_25col_2025onward.v1` | `491,076,642` (`468.33 MiB`) | `20,405,666` / `20,405,666` | `fcdef276f4e9887f2beab30590165cdea74caef50a1313d71fa01ff6afacca3a` | `214059832746f573ec27a4a35490951b797f7d7b7c46dbff43a1c2adb953b400` |

### 8.2 Published Analytical Releases & Empirical Resource Telemetry

| Metric / Artifact | Release 1 (`2024-12` 1-Month Vertical Slice) | Release 2 (`2024-12` + `2025-01` 2-Month Cross-Year Window) |
| :--- | :--- | :--- |
| **Release ID** | `rel_2024_12_3f8bf865` (Generation `1`) | `rel_2024_12_2025_01_f2d153b3` (Generation `2`) |
| **Derivation ID** | `addd81692af4fad0b75efcabd5439cd3b3da4ff608187cb26269cc2a1cfa3605` | `0c66a30e05e3cf3275b7e8a59f6cc87d1adfe114e150d24cca2e85d3c932cc8a` |
| **Manifest SHA-256** | `afd4d70bf45133cebea0f2b5638fd3dc82d970f7431d20a853731c894ef2254a` | `727af865a29324158e3d6276145299879893642d6442bd34f1eec983c8f6fcac` |
| **Database SHA-256** | `c1c56145cf0f9db683d57623ea9ffe13a4984629e1145869541b1196736c5d90` | `76bb7a32f3373b37526f7cece936e78b422c530b17505805d391a814020cbba6` |
| **Validation SHA-256** | `bb8e4b9f8c20ae83ab19d32e2c183a9bfdc7222e2cf8a6a9cd203b4c3450d3e1` | `31f51c2ea243457e7b1c8aa9747f7007352eea38cc874b1c61f3c3f0ac7c3f76` |
| **Verification ID** | `a60dbb68-839f-42c9-8ac5-6c5f096c99a1` | `64341a17-44f6-420b-821e-446a3b996a7d` |
| **Total Trips Ingested** | `21,068,851` | `41,474,517` (`21,068,851` + `20,405,666`) |
| **Eligible Intervals (`>= 0s`)** | `20,817,586` (`337` zero, `20,817,249` positive) | `41,023,464` (`545` zero, `41,022,919` positive) |
| **Negative Intervals (`< 0s`)** | `251,265` | `451,053` |
| **Missing Timestamp Intervals** | `0` | `0` |
| **Count Identity (`total = eligible + neg + missing`)** | `21,068,851 == 20,817,586 + 251,265 + 0` (`True`) | `41,474,517 == 41,023,464 + 451,053 + 0` (`True`) |
| **Continuous Median (`p50`, Type 7)** | `268.0` seconds | `249.0` seconds (`2024-12`: `268.0s`, `2025-01`: `232.0s`) |
| **Continuous 95th Percentile (`p95`, Type 7)** | `716.0` seconds | `666.0` seconds |
| **Wall Clock Build Time** | `20.611` seconds | `32.626` seconds |
| **CPU Time (User / System)** | `55.740s` user / `1.646s` system | `88.199s` user / `3.248s` system |
| **Memory RSS (Baseline / Peak)** | `901.67 MB` / `6,227.20 MB` (`<= 8192 MB` compliant) | `5,784.03 MB` / `7,634.23 MB` (`<= 8192 MB` compliant) |
| **DuckDB Database Size** | `1,040,986,112` bytes (`992.76 MiB`) | `2,052,861,952` bytes (`1,957.76 MiB`) |
| **Peak WAL / Post-Checkpoint WAL** | `1,309,369` bytes / `0` bytes | `2,744,629` bytes / `0` bytes |
| **DuckDB Spill Usage** | `0` bytes | `0` bytes |

### 8.3 Pre-Seal (`S01`–`S07`), Post-Seal (`S08`), and Semantic (`Q01`–`Q09`) Findings on Real Data

- **Blocking Rules (`S01`–`S08`)**: **100% PASS** on both Release 1 (`21,068,851` rows) and Release 2 (`41,474,517` rows).
  * `S01` & `S05`: Exact row reconciliation (`decoded_rows == inserted_rows == actual_rows`) and zero-based contiguous locators (`0..21,068,850` for `2024-12` and `0..20,405,665` for `2025-01`).
  * `S02`: Cross-year schema evolution verified in production: `2024-12` bound to `hvfhv_24col_pre2025.v1` (`cbd_fee_field_present = false`) and `2025-01` bound to `hvfhv_25col_2025onward.v1` (`cbd_fee_field_present = true`).
  * `S03`: `265` unique non-null `LocationID` rows in `taxi_zone_lookup.csv` with all 3 sentinels (`1`, `264`, `265`) present.
  * `S04`, `S06`, `S07`, `S08`: Single contribution per window month, exact `derivation_id` match (`0` unselected rows), complete rule inventory with verified count identity, and post-seal SHA-256 digest match.
- **Semantic Quality Rules (`Q01`–`Q09`) on Release 2 (`41,474,517` rows)**:
  * `Q01` (Null Profile): `1,078,337,442` cells evaluated across `26` columns and `4` cohorts; `41,382,406` nulls (`3.8376%` aggregate null rate).
  * `Q02` (Interval Distribution): `41,023,464` eligible (`545` zero, `41,022,919` positive), `451,053` negative, `0` missing; `p50 = 249.0s`, `p95 = 666.0s`.
  * `Q03` (Temporal Inconsistencies): `0` dropoff-before-pickup, `439,702` `on_scene_before_request`, `11,654` `on_scene_after_pickup`, `19,667` `trip_time` discrepancies.
  * `Q04` (Off-Source-Month Events): `0` off-source-month pickup timestamps (`0.0%`), `0` null pickup datetimes.
  * `Q05` (Zone Join Classification): Pickup locations: `41,472,985` ordinary, `1,532` sentinel, `0` null, `0` unmatched. Dropoff locations: `39,427,372` ordinary, `2,047,145` sentinel, `0` null, `0` unmatched.
  * `Q06` (Numeric & Sign Profile): `cbd_congestion_fee` is `NULL` on all `21,068,851` rows of `2024-12`, and populated on all `20,405,666` rows of `2025-01` (`14,105,454` zero `$0.00` and `6,300,212` positive up to `$3.00`, `p50 = $0.00`, `p95 = $1.50`). Negative values recorded in raw data preserved verbatim and surfaced (`base_passenger_fare`: `4,437` negative rows down to `-$27.98`; `driver_pay`: `1,308` negative rows down to `-$105.38`).
  * `Q07` (Categorical Domains): Evaluated across `["2024-12", "2025-01"]` (`6` columns); `0` unexpected new categorical codes discovered.
  * `Q08` (Normalized Field Equality Groups): `0` excess duplicate groups across both revisions; all rows strictly retained without deletion.
  * `Q09` (Adjacent Month Drift Profile): Evaluated across `2024-12` (`21,068,851` rows, `p50 = 268.0s`) and `2025-01` (`20,405,666` rows, `p50 = 232.0s`).
- **Storage Footprint & Consumer Verification**:
  * Active `$DATA_ROOT` footprint with both real releases retained + `16.0 GiB` spill reserve: `20.7473 GiB` (`22,277,220,262` bytes), leaving **`59.2527 GiB` headroom** under the `80.0 GiB` budget (`is_compliant = true`).
  * Consumer `ReleaseReader` pinned to `rel_2024_12_2025_01_f2d153b3` executed headless summary in `1.923s`, confirming `41,474,517` total trips, `17/17` Dagster asset checks passing, and `SYNCHRONIZED` frontier status.
