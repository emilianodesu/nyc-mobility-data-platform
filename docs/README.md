# Project Documentation

Documentation for the **HVFHV Monthly Data Reliability Platform** is organized by architectural overview, contracts, verification evidence, and source research.

## Architecture & Decisions

- [Architecture overview](architecture/overview.md): System scope, technology stack, responsibility boundaries, and recovery guarantees.
- [Low-level design](architecture/low-level-design.md): Module boundaries, identity model, SQLite control schema, and storage layout.
- [Architecture diagrams](architecture/diagrams.md): Context, publication sequence, and model lineage diagrams.
- [Architecture decision records](architecture/decisions/README.md): Formal ADRs (`0001`–`0006`), including [ADR 0006 (Option A Retention)](architecture/decisions/0006-amend-release-retention-to-option-a.md).
- [arc42 index](architecture/arc42-index.md): Cross-reference across architectural viewpoints.
- [Deployment context](architecture/deployment-context.md): Local process and storage boundaries.
- [Risk & uncertainty register](architecture/risks.md): Mitigations and acceptance closure evidence.

## Requirements & Contracts

- [Requirements & traceability](requirements.md): System invariants `R01`–`R26` mapped to ownership and verification.
- [Data & quality contracts](reference/data-contracts.md): Canonical schemas, adapters (`24-col` and `25-col`), interval definitions, and rule inventory (`S01`–`S08`, `Q01`–`Q09`).
- [Operations design](reference/interfaces-and-operations.md): Control-plane operations, lock discipline, and failure recovery contracts.
- [Glossary](reference/glossary.md): Core domain and storage definitions.

## Verification & Source Research

- [Verification & acceptance plan](verification/acceptance-plan.md): Evidence tiers, scenario suite `T01`–`T29`, and milestone gates `G0`–`G6`.
- [Execution log](verification/execution-log.md): Empirical gate certifications and full-scale NYC TLC execution records.
- [Gate 2 resource checkpoint](verification/gate2_pause_report.md): Vertical slice benchmarks and 80 GiB storage capacity analysis.
- [Technical readiness & numerical oracles](verification/technical-readiness.md): Dependency constraints and hand-calculated Type 7 quantile oracles.
- [Source research summary](evidence/source-research.md) & [source register](evidence/source-register.md): Official NYC TLC metadata and structural observations.
- [Research archive](../references/README.md): Historical TLC and zone lookup inspection records.
