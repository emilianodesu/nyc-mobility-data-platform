# Implementation sequence after design review

This is a future work breakdown, not an implementation record. The [acceptance plan](../verification/acceptance-plan.md) owns G0–G6 and T01–T29; do not create a second definition of passing.

| Work item | Dependencies and bounded deliverable | Review evidence |
| --- | --- | --- |
| G0 environment | Exact host/interpreter, resolver metadata, dedicated environment and reviewed dependency lock | Clean install and required-library integration checks; no invented compatibility from version labels |
| G1 acquisition/control | Official-source discovery, immutable originals, registry and physical locators | Failure fixtures plus exact source manifests and schema reference cases |
| G2 monthly slice | One complete real monthly input through models, profiles and candidate metrics | Row reconciliation, exception evidence and measured runtime/resources |
| G3 release safety | Publication, orphan reconciliation, rejection and rollback | Failure injection; uncertainty and crash class explicitly labeled |
| G4 target scale | Incremental replacement and complete 12-month plan | Full-rebuild equivalence, peak retained/candidate/spill footprint and exact-aggregate measurements |
| G5 consumer/operations | Reliability and interval views; local schedule/catch-up | Query populations, cache/rejection behavior and visual review |
| G6 handoff/evidence | Maintained decisions, reproducible commands, measured limitations | Independent requirement audit and user explanation separate from agent contribution |

Each implementation task must name allowed write paths, immutable inputs, affected requirement IDs, expected outputs and applicable scenarios. Shared schema/control mutations have one owner; independent verification does not manufacture approval. No source-code task is assigned by this document.

Stop or amend when an acceptance gate fails materially. Resource failure must not shrink the source window, replace exact metrics or remove retained releases silently. Capture the blocker, evidence and proposed decision before adding infrastructure.
