# 0004 — Keep quality findings separate from metric eligibility

Status: recorded design; implementation pending. Date: 2026-11-09. Requirements: R05, R08–R11, R13.

## Context

Observed negative intervals, coverage changes and unequal duration fields have unresolved causes. A universal clean/dirty row flag would discard useful evidence.

## Decision

Structural failures block; semantic exceptions remain visible. Define mutually exclusive interval classes and exact continuous quantiles over eligible rows. Report profiles and denominators without invented anomaly thresholds.

## Alternatives considered

Automatic deduplication, clipping negative intervals and zero-filling absent fees manufacture meaning. Averaging daily percentiles is statistically incorrect for monthly populations.

## Consequences

Rules and metric policy require versioning. Equal rows are not proven duplicate trips. Exact statistics have a measured resource gate; no universal quality score is claimed.

## Revisit trigger

Revisit only with source/domain evidence or an explicit accuracy/resource tradeoff, preserving old definitions and evaluation.

Evidence and detail: [source register](../../evidence/source-register.md), [architecture](../overview.md), [low-level design](../low-level-design.md).
