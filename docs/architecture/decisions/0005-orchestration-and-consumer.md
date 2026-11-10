# 0005 — Use Dagster OSS and a bounded local read-only consumer

Status: recorded design; implementation pending. Date: 2026-11-09. Requirements: R14, R16, R18–R20, R23.

## Context

Scheduling and presentation should expose reliability without becoming the correctness authority or permanent hosted infrastructure.

## Decision

Dagster invokes shared application operations, while their own locks and gates enforce correctness. Streamlit reads pinned verified releases with bounded query/cache resources and eligibility checks outside cache.

## Alternatives considered

Airflow or a custom API could be chosen for different operational needs, but are not required here. An always-on cloud service adds cost and deployment obligations.

## Consequences

No work runs while the host is off; catch-up is explicit. The widest consumer query must be measured under its own memory allocation. Local services are not publicly exposed.

## Revisit trigger

Revisit for a real external consumer/deployment requirement or measured scheduler/UI limitations.

Evidence and detail: [source register](../../evidence/source-register.md), [architecture](../overview.md), [low-level design](../low-level-design.md).
