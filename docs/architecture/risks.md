# Risk and uncertainty register

Status: open until the named evidence exists. No invented probabilities or measured performance.

| ID | Risk / impact | Mitigation and closure evidence | Owner / gate |
| --- | --- | --- | --- |
| K01 | Twelve-month native DB copies plus candidate/spill may exceed 80 GiB | Measure full monthly slice, conservative preflight then full horizon; explicit architecture amendment if infeasible | Implementer / G2,G4 |
| K02 | Exact percentiles or equality grouping exceed memory, especially consumer | Measure widest filters and independent exact-oracle results; no hidden approximation | Compute reviewer / G2,G4,G5 |
| K03 | Dependency ranges overlap but complete environment fails | Exact resolver/wheels/lock and clean install integration | Implementer / G0 |
| K04 | Filesystem/SQLite failure leaves inconsistent observable state | Ordered finalization, compare-and-swap, reconciliation and fault injection; power-loss guarantee excluded | Control reviewer / G3 |
| K05 | Request/on-scene timestamps do not mean true wait/arrival | Preserve naive values and limitations; no unsupported relabeling | Data-contract owner / ongoing |
| K06 | Upstream revision lacks changed validators or unobserved history | Completed-byte hash on acquisition; explicit revalidation; no all-revision capture promise | Acquisition / G1 |
| K07 | Research sample conclusions overstated | Preserve exact methods, metadata-vs-scan and nonrandom scope in archive | Reviewer / documentation and G2 |
| K08 | Schema changes or unknown codes cause silent coercion | Named adapters, unknown-schema stop, typed semantic flags | Transform / G1,G2 |
| K09 | Retired/rejected release served from cache | Registry eligibility outside cache, post-query check, audited cleanup | Consumer / G3,G5 |
| K10 | Stale documentation or unverified claims diverge from codebase | Authoritative ADRs, empirical execution logs, and automated gate tests | Repository maintainer |

Domain ambiguities (such as raw timestamp timezone conventions and on-scene definitions) remain documented limitations verified by explicit UI/query disclosures.
