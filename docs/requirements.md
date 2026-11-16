# HVFHV — Requirements and acceptance traceability

Date: 2026-11-09. Status: specified, not implemented. Owner: future project maintainer. User intent: a real monthly end-to-end data engineering portfolio project demonstrating robustness, quality and profiling with a fictional consumer. No claim of business adoption, employment impact or user mastery follows from this specification.

## Scope and terminology

The business source is official NYC TLC HVFHV trip observations plus the official zone lookup. The problem addressed is reliable, reproducible preparation of changing public monthly data for inspection and simple recorded request-to-pickup analysis. A published trip file is not an exhaustive request stream. The operator and reviewer are project roles, not interviewed customers.

Source month = object provenance. Event date = recorded pickup date. Observation time = when this system checked or captured upstream content. Revision = exact source bytes attached to a logical source. Derivation = frozen recipe and selected evidence. Release = immutable analytical artifacts. Publication = local selection of a release for consumption, distinct from publishing code or a website to the internet. Rejection = control-plane denial of further serving. Rollback = explicit selection of a retained verified release.

## Requirements

`Txx` refers to [verification scenarios](verification/acceptance-plan.md); `Gx` to its implementation gates. A mapping establishes intended coverage, not a passed test. All runtime requirements remain UNVERIFIED.

| ID | Requirement / observable acceptance | Design owner | Acceptance evidence |
| --- | --- | --- | --- |
| R01 | Consume official monthly HVFHV and zone sources; record URL, observation, exact bytes and provenance; no synthetic business source | Acquisition / data contracts | T01–T05, T29; G1 real-object manifest |
| R02 | Begin with twelve consecutive published source months; distinguish active, discovery and proposed windows; preserve current when a gap prevents a complete plan | Discovery | T09, T23, T25 |
| R03 | Detect content revisions by completed-byte digest; HTTP validators aid but do not establish identity | Acquisition | T02–T04 |
| R04 | Preserve immutable originals and recover finalized-but-unregistered files only with verified acquisition provenance | Control / acquisition | T01, T20, T29 |
| R05 | Give every source row a stable physical locator within its exact revision; do not invent a trip identity or deduplicate business observations | Transform | T06, T07, Q08 report |
| R06 | Repeated processing cannot inflate contribution counts; replacing a source revision removes its previous contribution | Transform | T02, T06, T13 |
| R07 | Preserve supported historical schema distinctions, particularly absent vs null vs zero CBD fee; unsupported schema fails explicitly | Adapter | T05, T14 |
| R08 | Preserve source values and metric-specific exception reasons; full-scan profiles label population, scope and method | Quality | T08–T11, T26; Q01–Q09 |
| R09 | Calculate recorded request-to-pickup intervals with microsecond precision and explicit naive-time limitations; count excluded rows | Metric contract | T08, T09, hand oracle in technical readiness |
| R10 | Exact continuous median/p95 use the actual eligible population; never average subgroup percentiles | Metric contract | T12, T13, widest-filter resource evidence |
| R11 | Version zone reference, preserve unmatched/sentinel zones, and recompute all dependent outputs after its revision | Reference / transform | T10, T13 |
| R12 | Freeze exact source, rules, metrics, code, lock and semantic configuration identity; do not inherit hidden comparative baselines | Planning | T13, T14 |
| R13 | Publish all analytical outputs consistently; missing/error gate results block, semantic warnings remain visible | Validation / publication | T14, T16, T17, T26 |
| R14 | One mutating application process; CLI and orchestration respect the same lock; stale plans cannot select over changed generations | Control | T15, T19 |
| R15 | Recover uncertain publication outcomes by idempotent event identity; an old successful operation must not reselect itself | Control | T17–T19 |
| R16 | Readers pin verified immutable releases and recheck rejection outside caches; corruption or rejection cannot trigger silent fallback | Consumer / control | T20, T21, T27 |
| R17 | Maintain current plus two distinct prior successful releases (amended to current plus one prior verified release under [ADR 0006](architecture/decisions/0006-amend-release-retention-to-option-a.md) Option A); cleanup explicit and cannot remove protected/open artifacts | Operations | T19, T22; cleanup dry-run review |
| R18 | Daily local discovery while environment runs; explicit catch-up/on-demand operations; no always-on promise | Orchestration | T23, T25 |
| R19 | Reliability and interval views show coverage, denominators, exclusions, versions and separate live operational state | Consumer | T09, T21, T27; G5 visual review |
| R20 | Resource exhaustion preserves current, reports failure and never silently reduces data or quality scope | Operations / compute | T12, T22, T28; G2/G4/G5 measurements |
| R21 | Schema migrations are versioned, backed up, checked and restorable without modifying released artifacts | Control | T24 |
| R22 | Reproducibility includes exact environment, host, artifact identities and full-rebuild comparison; no untested speed or durability claim | Packaging / verification | G0–G6; T13, T16–T20, T24 |
| R23 | All required capabilities run locally with free resources and no Docker/cloud dependency; both DE and AI/ML presentation must describe actual demonstrated work | Architecture / scope | Dependency/runtime inventory, G0/G6 review; AI/ML model is not required |
| R24 | Public code/site publication is a separate user-reviewed action; source data bulk mirroring is not assumed | Handoff | Export content review; no remote or deployment in this stage |
| R25 | Deliver a self-contained English documentation package for a future clean repository, with decisions, requirements, contracts, operations, evidence and acceptance plans | Documentation | Package file inventory, relative-link/hash audit and requirement-level review |
| R26 | Current work creates documentation only; no flagship pipeline, executable schemas, container, application or repository initialization | Stage boundary | Written-file inventory and action record for this goal |

## Nonfunctional policy and limits

- Correctness: source-count reconciliation, exact plan membership, consistent release and explicit exceptions take priority over a successful-looking run.
- Resource envelope: 80 GiB project-data budget including temporary files; one writer; provisional 8 GiB writer and 1 GiB reader engine targets; global spill split 14/2 GiB. These are acceptance targets, not measured guarantees or OS-enforced RSS limits.
- Availability: local operator environment only. A failed new run preserves the last valid release; a rejected/corrupt current release is unavailable until explicit remediation.
- Security: official HTTPS ingestion; path confinement; no execution of imported content; versioned application SQL; local services bound to loopback at implementation. No public authentication system is needed for an unexposed local consumer.
- Maintainability: one owner per rule, traceable amendments, no duplicated publication pointer and no misleading fake quickstart.

## Explicit exclusions and unresolved facts

No live stream, CDC, multi-writer distributed database, real customer commitments, demand/cancellation prediction, payment ledger, driver income estimation, ML model or deployment infrastructure is required. Raw timestamp timezone, on-scene coverage changes and scheduled-request interpretation are not resolved by the architecture. Source research samples cannot establish monthly error rates. Full-month performance, yearly storage and dependency interoperability remain empirical gates.

## Review and change policy

User decisions in the conversation establish source/cadence/objective; assistant-authored technical defaults are labeled design decisions, not fabricated user approvals. Changing a rule or metric requires rationale, affected requirements/scenarios, compatibility impact and a new version. Changes to source meaning or stack require an ADR amendment. Runtime evidence records exact versions; it never retrospectively turns an unexecuted scenario into PASS.
