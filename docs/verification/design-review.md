# HVFHV — Design verification, second pass

Date: 2026-11-09. Scope: adversarial document review, not execution of the proposed pipeline. Reviewed Architecture v1, low-level design, data contracts and verification plan. Coordinator review and independent architecture reviewer identified the same first three cases below; the independent reviewer additionally identified the original-finalization gap.

## Findings and dispositions

| Finding | Counterexample | Design correction | Future executable evidence |
| --- | --- | --- | --- |
| Discovery boundary omitted new periods | Checking only the twelve included files never discovers month thirteen | Separate included window, forward discovery range and contiguous proposed window; scan beyond a 404 | T25 |
| Cached rejected release remained readable | A session keeps a cached R1 after R1 rejection and rollback | Check eligibility outside cache before render/export and after query; distinguish superseded from rejected | T27 |
| Gate status semantics ambiguous | BLOCK/NOT_APPLICABLE could pass an inventory-only gate; WARN/FAIL could incorrectly stop real data | BLOCK requires PASS; all ERROR/missing/duplicate results block; qualified WARN findings remain publishable | T26 |
| Raw orphan recovery unspecified | Process dies after file finalization but before registry insertion | Verify existing bytes and acquisition provenance, register/reuse without overwrite or implicit plan mutation | T29 |
| Reader caching unbounded | Many individually valid filter queries accumulate result copies | Bounded cache entries/payload, no persistent cache, explicit oversized-result handling and combined RSS measurements | T28 |

Corrections are documented in [low-level design](../architecture/low-level-design.md), [data contracts](../reference/data-contracts.md) and [verification plan](acceptance-plan.md). They preserve the selected stack and monthly source. The first-pass fixes—external post-seal attestations, history-independent comparative baselines, expected generation and idempotent historical selection events—remain in place.

## Verification actually performed

- Read and cross-checked operation interfaces, schema descriptions, identity definitions, gate behavior, reader semantics and failure scenarios.
- Independent reviewer supplied concrete counterexamples; the coordinator incorporated corresponding contract changes.
- Checked local Markdown destinations, required rule/scenario identifiers, scenario-to-gate coverage and document hashes using a read-only document consistency script, then refreshed decision-record hashes.
- Consulted official [Streamlit caching documentation](https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_data): cache hits can bypass function execution, so eligibility checks belong outside the cached function. Framework defaults are not project memory guarantees.

Result: the identified documentary gaps are addressed. This is not a proof that all possible defects have been eliminated. No automated pipeline test, source acquisition, dependency installation, benchmark, application launch or deployment was performed during this review.

## Remaining empirical acceptance

All T01–T29 scenarios remain NOT RUN. G0–G6 remain pending. Outstanding evidence includes a compatible exact dependency lock, full-object decoding and source-schema coverage, measured full-month and twelve-month resource usage, process-interruption recovery, actual consumer behavior and supported filesystem synchronization. No power-loss durability guarantee is made. Previously observed real source samples remain partial where originally labeled; no new population statistics were inferred.

Documentary review can close contract ambiguity; it cannot substitute for these empirical checks. Next implementation work, when requested, starts with G0 and G1 rather than claiming the verification plan has already passed.
