# 0006 — Amend retained analytical release policy to Option A (current plus one prior verified release)

Status: accepted amendment (user-approved at Gate G2 pause checkpoint). Date: 2026-11-11; formalized: 2026-11-16. Amends: [0002](0002-local-native-analytical-storage.md), [0003](0003-immutable-releases-and-selection.md). Requirements: R17, R20, R22. Scenarios: T19, T22, T27.

## Context

The initial baseline specification (`R17`, [low-level design §7](../low-level-design.md)) proposed retaining the current verified analytical release plus two distinct prior verified releases (`3` retained DuckDB releases) alongside `1` unpublished staging candidate during build (`4` full-window DuckDB databases simultaneously on disk), plus `16.0 GiB` reserved spill (`14.0 GiB` writer + `2.0 GiB` reader) and `~6.0 GiB` of 12-month raw Parquet originals, all within an `80.0 GiB` total project-data budget (`raw + 3 * released_database_size + candidate_size + spill + control/logs <= 80 GiB`).

At the Gate G2 empirical checkpoint ([Gate G2 pause report](../../verification/gate2_pause_report.md)), extrapolated 12-month native DuckDB analytical storage (`~20.4 GiB` per full 12-month release) showed that holding `4` simultaneous full-window DuckDB copies plus `16.0 GiB` spill and raw data requires `~98.13 GiB` (or `~104 GiB` with 6 GiB raw Parquet), exceeding the `80.0 GiB` cap by `18.13–24.0 GiB`.

Per project rules (`AGENTS.md` and `R20`), resource limits must never be passed by silently downsampling rows, truncating the 12-month analytical window, or weakening quality/quantile semantics. Instead, `0002` and `0003` explicitly established a revisit trigger if native snapshot storage exceeded the `80.0 GiB` budget.

## Decision

Adopt **Option A (Retain Current + 1 Prior Verified Analytical Release)**:

1. **Protected Analytical Release Set**: The control plane and retention engine protect the `current` selected `VERIFIED` release plus `1` immediate prior distinct `VERIFIED` release (`2` retained analytical DuckDB databases total, plus at most `1` staging candidate during active builds = `3` DuckDB copies peak).
2. **Audited Retirement of Older Releases**: Analytical releases older than the `current + 1 prior` protected set become eligible for audited cleanup (`preview_retention_cleanup_option_a` followed by `enforce_retention_policy_option_a`). Before unlinking an older `analytics.duckdb` file, the release transitions to `RETIRED` status with a transactional `release_status_events` entry and `cleanup_requests` / `cleanup_items` receipts (`PREPARED` → `DELETED`).
3. **Permanent Provenance & Raw Preservation**: Immutable `manifest.json` and `validation.json` receipts in `$DATA_ROOT/releases/<release_id>/`, all SQLite control/audit rows, and all accepted raw Parquet/CSV originals in `$DATA_ROOT/raw/sha256/` remain permanently preserved. Any retired historical release can be deterministically reconstructed from the retained raw originals and frozen input plan in `manifest.json`.
4. **Consumer Enforcement**: `RETIRED` is terminal. Any consumer or rollback attempt targeting a `RETIRED` release fails closed outside cache (`RetiredReleaseError`, `T27`).

## Alternatives considered

- **Option B (Increase total storage budget from 80 GiB to 110 GiB)**: Preserves 3 retained releases + 1 candidate, but violates the workstation `80.0 GiB` storage constraint.
- **Option C (Retain only 1 current release, 0 prior releases)**: Maximizes disk headroom (`~37 GiB` peak), but eliminates immediate zero-rebuild CAS rollback (`T19`) to the previous verified release.
- **Silent window reduction or sampling**: Explicitly forbidden by `R02`, `R08`, `R10`, and `R20`.

## Consequences

- Peak 12-month storage footprint drops from `~98.1–104.0 GiB` (4 DuckDB copies + spill + raw) to `~57.4–63.2 GiB` (2 retained DuckDB copies + 1 candidate + 16 GiB spill + raw), maintaining `~16.8–22.6 GiB` of safe headroom under the `80.0 GiB` budget.
- Immediate atomic CAS rollback (`T19`) remains fully supported for the most recent prior verified release without rebuilding.
- Rollback beyond 1 prior generation requires rebuilding a new candidate from the preserved immutable raw originals using the historical `manifest.json` frozen plan.

## Revisit trigger

Revisit if workstation storage allocation increases above `110 GiB` or if multi-generation instant rollback without rebuild becomes an operational requirement.

Evidence and detail: [Gate G2 pause report](../../verification/gate2_pause_report.md), [execution log](../../verification/execution-log.md), [low-level design](../low-level-design.md).
