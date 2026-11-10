# 0001 — Use monthly HVFHV with explicit semantic limits

Status: recorded design; implementation pending. Date: 2026-11-09. Requirements: R01–R03, R08–R10.

## Context

The goal is a real recurring source and a complete engineering workflow, not a simulated live stream. TLC provides monthly trip observations; request and on-scene interpretation is not fully established.

## Decision

Use HVFHV monthly originals and the official zone reference. Keep source month separate from event date. Describe the derived output as recorded request-to-pickup interval.

## Alternatives considered

A static commerce dataset does not provide the selected recurring arrival. Combining Yellow with HVFHV or adding artificial streaming would change source semantics without solving this requirement.

## Consequences

Monthly arrival is sufficient. There is no evidence of all requests, cancellations or demand. Scheduled requests and timezone ambiguity remain limitations.

## Revisit trigger

Revisit only if a new source supplies required missing events or the user explicitly changes the objective.

Evidence and detail: [source register](../../evidence/source-register.md), [architecture](../overview.md), [low-level design](../low-level-design.md).
