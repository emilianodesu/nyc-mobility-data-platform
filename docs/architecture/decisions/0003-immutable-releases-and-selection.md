# 0003 — Select immutable analytical releases transactionally

Status: recorded design; implementation pending. Date: 2026-11-09. Requirements: R12–R17, R21.

## Context

A successful task or table update does not prove a consistent consumer view. Filesystem finalization and database selection are different operations.

## Decision

Freeze inputs, validate candidate, close/hash/finalize files, verify externally, then compare-and-swap the SQLite selection and append its idempotent event. Rejection and retirement prevent further serving.

## Alternatives considered

Overwriting the currently read DB risks mixed state. Directory timestamps or a second current symlink would create competing authorities. A custom general table format is unnecessary.

## Consequences

Orphans require reconciliation. Rollback retains distinct versions. Cleanup is explicit and audited. Process-failure tests cannot prove power-loss durability.

## Revisit trigger

Revisit if the local filesystem protocol cannot meet demonstrated requirements or native snapshot cost is impractical.

Evidence and detail: [source register](../../evidence/source-register.md), [architecture](../overview.md), [low-level design](../low-level-design.md).
