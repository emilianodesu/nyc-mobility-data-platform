# 0002 — Use local DuckDB and original Parquet

Status: recorded design; implementation pending. Date: 2026-11-09. Requirements: R04–R07, R20, R22–R23.

## Context

Single-machine free operation and measured engineering behavior matter more than tool count. The observed monthly files warrant columnar processing; full-year footprint remains unproven.

## Decision

Preserve exact originals and use DuckDB with modular SQL for analytical models. SQLite owns application control. Use a dedicated locked environment later.

## Alternatives considered

Spark, Iceberg/Delta and an object-store service introduce additional operational contracts not currently required. Mandatory bronze/silver/gold physical copies are not a requirement.

## Consequences

The project does not demonstrate distributed compute. Native release copies and exact aggregate memory are explicit feasibility risks; table-level data format interoperability is limited.

## Revisit trigger

Revisit for measured budget failure, concurrent writers or a concrete interoperability/distributed learning requirement, recording alternatives against evidence.

Evidence and detail: [source register](../../evidence/source-register.md), [architecture](../overview.md), [low-level design](../low-level-design.md).
