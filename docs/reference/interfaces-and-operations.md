# HVFHV — Operations design v1

Operator contracts and failure-recovery specifications. Authority: [Architecture v1](../architecture/overview.md), [low-level design](../architecture/low-level-design.md), [data contracts](data-contracts.md) and [verification plan](../verification/acceptance-plan.md). Compatibility and resource boundaries are detailed in [technical readiness](../verification/technical-readiness.md).

## 1. Meaning of operation and publication

The interfaces below define canonical application operations implemented under `src/hvfhv/` and exposed via Dagster assets/jobs and CLI runners.

Local publication means atomically selecting an already verified analytical release in SQLite. Routine acquisition, build, and local selection run without external network publication; rollback and rejection are explicit operator decisions with recorded audit reasons, while storage cleanup requires explicit preview and confirmation.

## 2. Common request and result envelope

Every mutating operation records the operation name, request identity, relevant attempt/release/source IDs, UTC start/end, effective configuration version and structured outcome. Logs contain bounded diagnostic context, not entire source rows. An attempt ID identifies execution; it is not a source revision, derivation or publication idempotency key.

Results distinguish `SUCCEEDED`, `NO_CHANGE`, `PENDING_INPUT`, `BUSY`, `CONFLICT`, `FAILED` and `OUTCOME_UNKNOWN`. These interface outcomes do not replace the attempt-state enum in the low-level design. A failure includes a taxonomy code, retry guidance and any preserved artifact identities. Unknown outcomes must be reconciled before repeating a mutation. A process that dies cannot be assumed to have recorded its final outcome.

An immutable selection request contains operation key, operation kind, target release, expected parent, expected generation and reason when required. Persist that request before selection. Reuse a key only for the identical request; different parameters with the same key are a conflict. A committed event returns its historical success even if a later operation changed current. A newly intended selection after a conflict receives a new operation key.

All mutation respects the common OS writer lock. Bounded contention returns BUSY without changing application artifacts. Never delete the lock file or use a stale heartbeat to steal a live lock. Long operations do not hold SQLite transactions open. Read-only reconciliation may observe concurrent work: report its captured generation and require fresh checks under the writer lock before applying a repair.

## 3. Logical interfaces

| Proposed operation | Required inputs | Successful or nonfailure output | Boundary |
| --- | --- | --- | --- |
| Discover | Active window, explicit observation cutoff, metadata/full-revalidation policy; bootstrap listing or replay range when applicable | Observation IDs, availability by period, acquisition proposals, consecutive proposed window and explicit gaps | No release selection; unknown availability differs from 404 |
| Acquire | Logical source, approved official URL, acquisition attempt ID, bounded HTTP policy | Accepted revision ID, digest, counts, acquisition evidence; reused revision when bytes match | Complete decode/readability is required; analytical schema support is separate |
| Freeze plan | Exact selected revisions, configured consecutive months, zone revision, code/rule/schema/lock/config hashes, attempt ID | Immutable plan, derivation ID, dependency impact; PENDING_INPUT for gaps | No fabricated revision hashes; no later mutation of the frozen plan |
| Build | Frozen plan, new attempt ID, verified base or full-rebuild mode, expected parent/generation | Unpublished candidate and contribution reconciliation | Failed candidates are not resumed in v1 |
| Validate | Candidate, exact phase inventory and contract versions | Pre-seal rule results and gate decision | Errors and missing results never become passes |
| Finalize | Candidate with passing pre-seal gate, unique release ID | Closed immutable directory, manifest/database/report digests | No current-pointer update; existing destination is never overwritten |
| Verify release | Release identity and expected digests, supported verifier/model versions | Post-seal S08 attestation linked to exact bytes, or failure | Attestation is external to sealed artifacts; rejected releases are not rehabilitated by a new hash check |
| Publish | Immutable selection request, verified release and matching attestation | Selection event and generation, or historical success for the same operation key | Compare-and-swap; no blind reselection on acknowledgement loss |
| Inspect current | Optional previously pinned release handle | Current identity/generation, eligibility, separately dated operational status | Missing/corrupt/rejected current is unavailable, not an automatic fallback |
| Reconcile | Scope of attempts/raw/releases/control, observation generation | Discrepancy report with evidence and permitted next action | Reporting does not publish, delete or guess missing provenance |
| Register verified orphan | Exact orphan identity, persisted provenance/plan, reconciliation evidence | Registered revision or release, without selection | Recheck under writer lock; missing evidence leaves it unassigned |
| Reject | Release ID, reason and supporting observation | Rejection recorded with time; eligibility becomes REJECTED | No edits to sealed bytes and no replacement selection |
| Rollback | Verified retained target, reason, expected parent/generation, new operation key | Selection event using the publication protocol | Reverify exact bytes; rejected target is forbidden |
| Preview cleanup | Inventory, current generation, protected set, explicit requested candidates | Exact paths/identities/sizes, exclusions, preview digest | No deletion; preserve raw evidence and audit/manifests |
| Apply cleanup | Reviewed preview digest and its exact candidates | Per-artifact deleted/skipped/failed receipt | Consumer stopped; recheck generation/protection/identity under lock; stale preview stops |

## 4. Discovery and acquisition runbook

1. Capture the published window and observation cutoff. At bootstrap inspect the official listing and record it. On subsequent discovery inspect active months for revisions, the frontier through the last completed calendar month, and the zone reference. Continue past individual unavailable months.
2. Propose twelve consecutive months ending at the latest observed available month. Record holes and failed checks separately. Do not advance current because the calendar changed or substitute nonconsecutive files. Explicit historical replay keeps its configured range.
3. Acquire proposed inputs into unique staging paths with bounded retries. Restart interrupted downloads rather than resuming ranges. An unavailable source is a pending input, not zero rows. A failed check does not update last-success time.
4. Hash and fully decode the original in bounded batches. Record byte count, decoded count, applicable footer count and acquisition provenance linking bytes to the official response. Synchronize the acquisition record before raw finalization.
5. Synchronize and finalize the original on the same filesystem, then register it transactionally. Verify an existing digest destination and reuse it; never overwrite it. Accepting readable bytes does not approve an unsupported schema or declare the data clean.
6. Freeze a plan only when all selected revisions are known and eligible for its named adapters. Newly observed revisions wait for a later plan. Report acquisition and decode cost separately from transformation cost.

Relevant verification: T01–T07, T25 and T29. A missing provenance record after a crash requires reacquisition or explicit review, not reconstruction from a filename.

## 5. Build, validation and local publication runbook

1. Acquire the writer lock, check expected parent/generation and budget preflight, and verify selected raw inputs/base. Build a separate candidate. No reader opens it.
2. Reconcile each source contribution. Recompute profiles and aggregates according to the frozen plan. Keep semantic exceptions and their denominators; do not deduplicate equal trips automatically or weaken checks after resource failure.
3. Apply the exact gate in the data contracts: every BLOCK result PASS; WARN failures allowed only as recorded findings; ERROR, missing/duplicate results, unknown statuses and forbidden NOT_APPLICABLE block selection. S07 checks the pre-seal inventory. S08 is not written into the DB/report it verifies.
4. Commit, checkpoint and close the candidate; inspect read-only and close again. Produce the pre-seal report, hash closed files and write the manifest. Synchronize files/directories and finalize at a unique destination. Sync failure stops selection.
5. Verify finalized bytes and append S08 attestation in control. Only complete successful phase gates establish VERIFIED. Do not alter artifacts to record verification or publication.
6. With the persisted immutable selection request, perform the short SQLite compare-and-swap transaction: verify status/attestation, update current and generation, append the event atomically. Confirm by reading the event. No external network publication is part of this transaction.
7. Release the writer lock and report the exact selected release, generation, included periods, warnings and validation provenance. The consumer resolves the new selection on refresh; a superseded eligible release can finish an existing query.

Relevant verification: T13–T20 and T26. Synchronization ordering is a proposed protocol, not evidence of power-loss durability. Preserve the actual host/filesystem and tested failure class in later reports.

## 6. Interruption and recovery runbook

First inspect control and artifacts without assuming that an absent acknowledgement means rollback. Acquire the writer lock before applying any recovery. Never infer which release is current from directory names or timestamps.

| Observed interruption/evidence | Permitted recovery | Forbidden shortcut |
| --- | --- | --- |
| Partial acquisition only | Keep unaccepted; new attempt restarts download | Register partial bytes or silently delete evidence |
| Complete raw object, missing registry row | Verify full bytes/readability and persisted acquisition record; explicitly register | Guess source from digest/path or add it to a frozen plan |
| Registry raw entry with missing/corrupt file | Block its use; reacquire into a new verified recovery process and document discrepancy | Trust registry alone or overwrite a content-addressed object |
| Candidate interrupted during SQL/checkpoint | Mark interrupted when supported by reconciliation; new attempt starts from verified inputs/base | Resume unknown partial model state or delete live WAL |
| Finalized release without completed registration/selection | Verify manifest, plan, provenance and bytes; register without selection; any selection uses an explicit request | Automatically adopt newest orphan |
| Selection outcome uncertain | Look up operation key; return historical event if present; otherwise reconcile request and expected generation | Create a replacement key just to repeat an uncertain selection |
| Generation changed before selection | Return CONFLICT; preserve candidate for review/reuse under a new explicit request | Replace expected generation silently |
| Current artifact missing, corrupt or rejected | Stop serving; investigate, then explicitly select a retained verified target | Silent fallback, editing released files or serving cached data as current |
| Registry integrity failure | Stop mutations/serving that rely on it; preserve evidence and evaluate a verified backup with artifact reconciliation | Restore a backup as if it necessarily describes the latest filesystem state |

A restored registry can predate successful selections. Restore is therefore incident recovery, not routine rollback; inspect artifact/event discrepancies before normal operations resume. No destructive workstation power interruption is authorized by this runbook.

## 7. Rejection, rollback and serving

Rejection records a reason and supporting evidence under the writer lock and marks the release REJECTED. It does not rewrite its contents, move current or erase historical success. Repeating the same already-established rejection may return NO_CHANGE; differing reasons remain additional audit evidence rather than overwriting history. There is no implicit un-reject operation in v1.

Every render/export checks eligibility outside cache; query completion checks it again before delivery. A cached or pinned rejected release cannot be served. Previously displayed information cannot be retroactively withdrawn. A superseded VERIFIED release remains readable; supersession alone is not rejection. A rejected current release leaves an unavailable view until an explicit successful selection.

For rollback, identify a retained target, verify its files and supported schema, inspect findings, record the reason and capture current generation. Submit a new rollback selection request using the same transactional protocol as publish. A corrupted/rejected target is not a valid rollback destination. Verify the resulting event and refresh the consumer. Cache entries do not establish eligibility or replace registry checks.

Relevant verification: T19, T20, T21 and T27. Reader query/cache/memory limits remain those in the low-level design; BUSY or resource failure is visible, never a silently sampled result.

## 8. Cleanup runbook

1. Stop the consumer and confirm no artifact handles remain open. Pause scheduling that would immediately contend with maintenance. Do not infer absence of readers from file timestamps.
2. Inventory exact candidate paths/identities and compute the protected set: current, two distinct prior successfully selected releases, raw evidence, acquisition provenance, manifests and audit/control evidence. Missing protected artifacts are incidents, not newly deletable space.
3. Produce a preview listing eligible failed staging content and older unprotected release payloads, retained metadata, proposed bytes reclaimed and reasons for every exclusion. Do not offer raw-evidence deletion as routine v1 cleanup.
4. Review that concrete preview. Under the writer lock, recompute protection and verify preview identity/generation and each target. Any changed target or protection set stops for a fresh preview; never follow path escapes or symlinks into unlisted content.
5. Remove only the exact reviewed, still-eligible payloads, retaining receipts and provenance/manifests. Record partial success per artifact if a filesystem operation fails. A retry re-inventories first; absence is recognized as previous completion only with matching evidence.
6. Verify all protected artifacts remain available; measure reclaimed bytes and resume normal operation. If the budget still cannot accommodate the workload, request a documented retention/storage amendment rather than silently reducing coverage.

Cleanup is not an atomic multi-file transaction. A deleted payload remains represented by its audit identity and cleanup receipt so reconciliation distinguishes intentional retirement from unexplained disappearance. Relevant verification: T22 and integrity checks from T20.

Persistence is specified in the low-level registry: `selection_requests` stores the immutable pre-commit request; `release_status_events` audits rejection and retirement; `cleanup_requests` and `cleanup_items` retain reviewed intent and per-file progress. Before old release payload deletion, record RETIRED and a PREPARED cleanup item. RETIRED cannot be read, selected or rehabilitated by verification. Finalize the item as DELETED after removal and synchronization; reconcile interrupted PREPARED items against exact identities. Keep the manifest/report even when the database payload is removed.

## 9. Failure taxonomy and retry policy

| Category | Examples | Default handling |
| --- | --- | --- |
| Availability | HTTP 404 for a requested month | Record unavailable/pending; keep current; future discovery may retry |
| Transient transport | Timeout, connection interruption, 429, transient 5xx | Bounded configured retry; exhausted attempts preserve unknown availability |
| Access/configuration | TLS failure, disallowed redirect, actionable 4xx, invalid path | Stop; correct cause without bypassing validation |
| Integrity | Digest mismatch, incomplete decode, missing registered bytes, corrupt registry | Block use/selection; preserve diagnostics and reconcile |
| Unsupported contract | Unknown source type/schema, unsupported model version | Stop normalization/use; explicit versioned amendment needed |
| Computation/gate | SQL error, missing rule, invalid count identity | Candidate blocked; new corrected attempt, no forced pass |
| Semantic finding | Negative recorded interval, unknown code, equality group | Preserve and expose according to WARN policy; not a transport retry |
| Concurrency | Writer/query lock busy, stale generation, changed cleanup preview | Bounded BUSY or explicit CONFLICT; never steal lock or auto-rebase selection |
| Resources | Disk, spill, memory or result-size limit | Stop affected work safely; preserve current; measure and review limits |
| Uncertain commit | Process dies after selection commit, acknowledgement lost | Query immutable operation key before any repeat |
| Revocation | Explicitly rejected release | Deny new serving/selection; explicit verified replacement required |

## 10. Handoff invariants

- Accepted original bytes, analytical schema eligibility, validated candidate, verified release and current selection are different facts.
- One frozen plan determines logical data results; historical execution order is not an implicit input.
- No state row alone proves present file integrity. No integrity hash alone proves source provenance or semantic truth.
- A selection event and current generation change commit together; artifacts become durable/finalized before that transaction.
- Validation never writes a digest-dependent assertion into the artifact whose digest it checks.
- New attempts can reuse verified originals, but cannot invent approvals, change frozen inputs or mutate published bytes.
- Rejection prevents serving even on cache hits; cleanup cannot remove active/protected content.
- Operation reports distinguish proposed behavior, fixture verification, real-scale evidence and remaining limitations. No runbook text means an operation has run.

Future implementation must map these logical contracts onto the verified dependency stack and add concrete invocations only after those interfaces exist. This document supplies no runnable commands and authorizes no new repository or deployment.
