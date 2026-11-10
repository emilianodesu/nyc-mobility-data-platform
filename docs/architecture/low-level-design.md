# HVFHV — Low-level design v1

Date: 2026-11-09. Status: design, not implemented or experimentally validated. Refines [Architecture v1](overview.md). Professional documentation is in English; no source-of-truth career claim is created. The original prototype remains deleted.

Companion specifications: [data and quality contracts](../reference/data-contracts.md), [verification and implementation gates](../verification/acceptance-plan.md).

## 1. System boundary and layout

The following is a proposed standalone project layout, not directories created by this design task. It must not share CareerBooster's environment, private profile or registry.

| Proposed path | Responsibility |
| --- | --- |
| `src/hvfhv/acquisition/` | Official-source discovery, bounded downloads, immutable original acceptance |
| `src/hvfhv/control/` | SQLite transactions, locks, reconciliation, publication |
| `src/hvfhv/transforms/` and `sql/` | Arrow batch ingestion, explicit SQL models and metrics |
| `contracts/` | Versioned source schemas, rule inventory and metric definitions |
| `src/hvfhv/orchestration/` | Dagster adapters around application operations |
| `src/hvfhv/consumer/` | Read-only Streamlit presentation |
| `tests/` | Small labeled fixtures; no fabricated business-source evidence |
| `docs/` | Decisions, dictionary, runbook, limitations and measured evidence |
| `$DATA_ROOT/control/registry.sqlite` | Application publication authority |
| `$DATA_ROOT/control/writer.lock` | Cross-process advisory lock, held by every mutating application operation |
| `$DATA_ROOT/raw/sha256/<digest>/source.parquet` | Immutable accepted source bytes; zone CSV uses its own media extension |
| `$DATA_ROOT/staging/<attempt_id>/` | Downloads and unpublished database candidate |
| `$DATA_ROOT/releases/<release_id>/` | `analytics.duckdb`, `manifest.json`, `validation.json` |
| `$DATA_ROOT/orchestrator/` | Dagster metadata, distinct from application state |

Data root is a local filesystem outside tracked source. Resolve and validate all application-generated paths beneath it; no path or executable SQL is accepted from source content. Candidate and releases share a filesystem. Unique destination directories are never overwritten. No symlink called `current` acts as a second authority.

## 2. Identifiers and immutable input plan

| Identifier | Definition |
| --- | --- |
| `logical_source_id` | `tlc/hvfhv/YYYY-MM`, or `tlc/taxi_zones` |
| `content_sha256` | SHA-256 of complete exact accepted bytes; no ETag substitution |
| `revision_id` | Hash of canonical pair `(logical_source_id, content_sha256)` |
| `row_locator` | `(revision_id, source_row_number)`; zero-based physical file row offset, never a trip ID |
| `attempt_id`, `release_id` | Independently generated UUIDs; attempt is execution, release is artifact |
| `derivation_id` | Hash of canonical immutable input plan described below |

Input plan includes sorted logical-source/revision pairs, explicit source-window months, zone revision, schema-adapter version, rule-set and metric-definition hashes, SQL/code bundle hash, dependency-lock hash, model schema version and semantic configuration. Paths, wall-clock run timestamps and UUIDs are excluded from derivation identity. Code bundle is a manifest of actual executable source bytes; a Git commit alone is insufficient for dirty worktrees. Contract serialization uses UTF-8, sorted keys, compact separators, no NaN, integer/string configuration values and explicit absent/null semantics. Hash algorithm and serialization version are recorded.

Inputs are frozen before candidate construction. Newly discovered revisions wait for a subsequent plan. `expected_parent_release_id` is captured independently for compare-and-swap publication; it is not part of logical result identity. Same derivation does not imply identical DuckDB file bytes. Already-current verified derivation is a no-op; a matching historical release is offered for explicit reuse, not silently reselected over newer data.

Comparative profiles in v1 compare only adjacent source months within the frozen plan, with the earliest month explicitly lacking a baseline. They do not read a previous release implicitly. Comparison policy is versioned in the plan. Future external baselines must enter derivation identity by exact digest; publication parent remains a separate concurrency concept.

## 3. Control registry schema

All identifiers below are TEXT unless noted. Times are UTC RFC3339 strings; source timestamps are never stored under this convention. SQLite foreign keys enabled on every connection; mutations use short transactions, parameterized SQL and a bounded busy timeout. No long download/transform takes place inside a SQLite transaction. Choose rollback journal with `synchronous=EXTRA` initially; read back effective settings. On macOS request/read back `fullfsync`; do not infer power-loss guarantees from this setting alone.

| Relation | Key and essential columns / constraints |
| --- | --- |
| `schema_migrations` | `version INTEGER PK`, migration hash, applied time; no in-place downgrade |
| `source_objects` | `logical_source_id PK`, kind, nullable source month, official URL; month required for HVFHV |
| `source_observations` | `observation_id PK`, source FK, observed time, GET/HEAD, HTTP status, ETag, last-modified, advertised bytes, outcome, nullable revision FK; observations append-only |
| `source_revisions` | `revision_id PK`, source FK, digest, byte count, relative path, accepted time, schema fingerprint, footer row count; UNIQUE(source,digest) |
| `attempts` | `attempt_id PK`, requested window/configuration, nullable derivation and frozen plan JSON/hash until INPUTS_READY, expected parent/generation, phase, outcome, start/end, structured error, optional Dagster run ID |
| `attempt_events` | `(attempt_id, sequence INTEGER) PK`, previous/new phase, event time, event type and bounded payload; append-only |
| `releases` | `release_id PK`, derivation, producing attempt FK, directory, manifest digest, database digest, validation digest, finalized time, status `FINALIZED/VERIFIED/REJECTED/RETIRED`; UNIQUE directory; RETIRED denotes intentional payload removal and cannot be served or selected |
| `release_sources` | `(release_id,logical_source_id) PK`, revision FK; exactly one revision per logical source per release |
| `release_verifications` | `verification_id PK`, release FK, manifest/database/validation digests, post-seal rule inventory/version and results, verifier version, verified time; append-only, recorded after artifact finalization |
| `current_release` | singleton PK constrained to 1, nullable release FK, `generation INTEGER` monotonically increasing |
| `publication_events` | `event_id PK`, `operation_key UNIQUE`, operation `publish/rollback`, previous/selected release, previous/new generation, attempt FK if applicable, reason, time; inserted in same transaction as pointer change |
| `selection_requests` | `operation_key PK`, immutable request hash/JSON, target release, expected parent/generation, operation kind, reason, created time; persisted before selection; a reused key with different request is CONFLICT |
| `release_status_events` | `event_id PK`, release FK, previous/new status, reason, evidence reference, time; append-only and transactionally paired with status changes |
| `cleanup_requests` | `cleanup_id PK`, reviewed preview digest/JSON, expected generation, protected-set digest, review reference, time; actual operator decision only |
| `cleanup_items` | `(cleanup_id,item_id) PK`, exact relative path, artifact hash/identity, byte count, outcome `PREPARED/DELETED/FAILED`, diagnostic; progress/audit records retained |

Acquisition starts from a requested window, not invented future revision hashes. Freeze the complete plan and derivation at INPUTS_READY; both become immutable for that attempt. Each publication or rollback request receives an operation key before selection and keeps it across acknowledgement retries. A committed operation remains historically successful even if a later operation selected another release; retry returns that historical outcome without reselecting it.

No row in `source_revisions` means trusted data without checking its actual file. The registry stores content identity, not a perpetual integrity guarantee. Check digests before using originals to build, before publication, and during explicit verification. Rejected releases cannot be selected. Release artifact contents stay immutable even if control status changes.

Registry migration: acquire writer lock, back up via SQLite backup API, validate backup, migrate transactionally, run foreign-key/integrity checks, retain backup. Existing releases retain their own model version. Unsupported versions fail explicitly rather than opening through accidental compatibility.

## 4. Application operations and orchestration

Interfaces below are contracts, not implemented CLI commands.

| Operation | Inputs → outputs | Failure/retry contract |
| --- | --- | --- |
| `discover(window, revalidate)` | Configured source periods → observations and acquisition proposals | No candidate/current mutation; distinguish unavailable from network failure |
| `acquire(source)` | Official URL → accepted revision | Incomplete bytes stay in staging; complete hash and readable format required |
| `plan(window, revisions, versions)` | Accepted inputs → frozen plan + impact set | Missing any required month blocks the configured complete-window plan |
| `build(plan, expected_parent)` | Originals/verified base → unpublished candidate | Writer lock; no consumer access; candidate failure leaves current intact |
| `validate(candidate)` | Models + exact rule inventory → results + blocking summary | Missing/errored required rule is failure, never pass |
| `finalize(candidate)` | Validated, checkpointed, closed files → immutable directory + hashes | Unique target; mismatch or filesystem error stops |
| `publish(release, expected_parent, expected_generation)` | Verified release → committed selection event | Compare-and-swap prevents stale selection; uncertain result queried before retry |
| `reconcile()` | Registry + filesystem → discrepancy report | Does not automatically publish or delete anything |
| `rollback(target, reason)` | Retained verified release → new pointer generation | Reverify and use same selection protocol; no byte edits |
| `inspect_current()` | Registry → release handle and separate live operational status | Fail closed on missing/corrupt artifact; no silent fallback |

Dagster has a daily discovery schedule and explicit materialization job. Monthly raw-source assets use the source month as partition key. A release build is one unpartitioned, serialized job carrying the full frozen input plan: it must not be represented as twelve independently publishable monthly releases. Each operation emits run/attempt/derivation metadata. Asset checks mirror canonical rule results; `publish` enforces the gate itself. Scheduler run keys reduce redundant scheduling but application identity and locks enforce idempotency. Resuming the workstation reconciles missing periods, not only the most recent scheduled tick.

Discovery checks three sets separately: existing active-window objects for revisions, missing/new source periods from the last included month through the last completed calendar month, and the zone lookup for revisions. At bootstrap, use the official listing to establish the latest observed published month and propose the twelve consecutive months ending there; record the listing observation. Never stop scanning after the first 404, and never advance the window solely because the calendar advanced. If a newer available month implies a window with a missing required month, report that pending window and preserve current; do not substitute twelve nonconsecutive files. Explicit older replay uses its configured range. Bounded HTTP failures leave source availability unknown, not absent.

HTTP policy: HTTPS with certificate validation, official hosts only, redirects checked against allowlist, streaming download to unique `.part`, finite connect/read timeouts (initial 10/60 seconds), at most 3 attempts with backoff and bounded Retry-After. Retry timeout/connection failures, 429 and transient 5xx; treat 404 as unavailable observation, other 4xx as actionable errors. No partial-range resume in v1: restart interrupted downloads. Content-length checked when supplied, but never substitutes for complete decode/hash. Request identity encoding. Compare request/response validators when available; upstream changes during acquisition cause restart. Same validator does not prove unchanged bytes. Metadata checks and forced full revalidation are distinct operations.

Original acceptance protocol: finish download, hash bytes, validate footer/header and decode all rows in bounded batches, record decoded count, synchronize file and staging directory, finalize into its content-addressed raw directory on the same filesystem, synchronize affected directories, then register revision/observation in a short SQLite transaction. Accepted means byte integrity plus complete readability, not supported analytical schema or clean data; schema eligibility is evaluated separately. The extra decode pass is intentional and included in acquisition cost measurements. Source revision metadata records both footer count and decoded count where applicable.

If the digest destination already exists, verify its bytes and readability and reuse it; never overwrite or trust its path alone. After interruption between raw finalization and registration, reconciliation may register an orphan only after verifying complete bytes, format and the persisted acquisition record linking them to an official source response. Missing acquisition provenance leaves an unassigned blob requiring reacquisition or explicit review; filename guesses are insufficient. Registration never inserts the revision into an already frozen plan. Registry entries with missing/corrupt raw bytes cannot be used to build. Interrupted `.part` files remain unaccepted and cleanup-eligible, not automatically deleted. Acquisition records include logical source, requested/final URL, response metadata, completed byte count and hash; store and synchronize this record before raw finalization.

## 5. Construction and dependency invalidation

Initial build reads selected original revisions. Incremental build copies a closed verified prior database into staging using a real copy or copy-on-write clone with independent subsequent writes, never a hard link. Apply a transaction per source contribution: delete old contribution by logical source identity, ingest the selected revision and reconcile row count. A crash makes the entire candidate unpublishable; v1 retries rebuild the candidate from a verified base instead of resuming partially applied models. Accepted raw files are reusable after integrity checks.

Arrow reads row groups in file order with bounded batches (initial 65,536 rows). Assign explicit cumulative physical row numbers before any SQL transformation; do not use unordered `row_number() over ()` as provenance. Register/consume each batch in DuckDB without materializing the full monthly file in Python. Batch size is tunable without changing row identity.

| Change | Required recomputation |
| --- | --- |
| New/revised/removed active source month | Replace/remove affected trip contribution; refresh its profiles, all affected event-time groups and cross-month comparisons |
| Window advances | Remove departed contributions from candidate only; incorporate arrival; rebuild window-level coverage/aggregates |
| Zone lookup revision | Replace lookup; recalculate zone qualification and all zone-enriched outputs for every active month |
| Rule, metric, adapter or model schema version | Full derivation refresh; use full rebuild for adapter/model changes in v1 |
| Display-only application change | Consumer package changes, no data recomputation if semantic contract unchanged |

For simplicity and correctness v1 recomputes all aggregate/profile relations from retained trip observations after contribution changes. Incremental raw ingestion does not imply incremental percentiles. Optimize this only after measuring the complete calculation. Full rebuild and incremental build must produce equal logical results under the same plan, allowing documented numeric tolerances where necessary.

## 6. Attempt state and publication protocol

Phase: `PLANNED → ACQUIRING → INPUTS_READY → BUILDING → VALIDATING → FINALIZING → SELECTING → COMPLETE`. Outcome is separate: `RUNNING/SUCCEEDED/FAILED/CANCELLED/INTERRUPTED/CONFLICT`. Existing accepted inputs can skip acquisition via an explicit event. A later retry gets a new attempt ID. Phase events describe actual completed transitions, not optimistic work intent. Heartbeats are diagnostics, not authority to steal a live lock.

All application mutation, rollback and cleanup acquire the same OS advisory writer lock. On initial supported host use POSIX `flock`; lock file contents are diagnostic only and the file is never deleted to break a lock. Readers need no writer lock for immutable databases. Windows/network filesystem support is not claimed.

Publication sequence:

1. Under the writer lock, confirm expected current release/generation and input hashes; build/validate unpublished candidate.
2. Commit DuckDB transactions, run CHECKPOINT, close writer. Reopen read-only for validation of model counts/version and close. Require no WAL dependency; never delete a live WAL as a shortcut.
3. Produce `validation.json` with every required pre-seal rule result (S01–S07, Q01–Q09) and phase-specific inventory hash. Hash the closed DB and validation file. Write `manifest.json` containing these digests, frozen plan, source references, schema/producer versions and coverage. Manifest does not contain its own digest; registry records that digest. DB does not contain its final file digest.
4. Flush and synchronize files, synchronize candidate directory, then rename the complete directory to a unique release destination on the same filesystem and synchronize the containing directories. Errors stop before selection. Platform-specific durable flush behavior must be verified during the compatibility pilot.
5. Recheck finalized artifact hashes and invariants (post-seal S08); insert/reconcile its finalized registry record and append a `release_verifications` attestation bound to the exact three digests. Do not modify DB, validation report or manifest to include this later check. Only a successful complete pre-seal inventory plus successful post-seal attestation makes a release VERIFIED. Start short SQLite selection transaction, assert expected parent AND generation, selected status and matching attestation, update singleton, append publication event and commit. No external side effect occurs inside this transaction.
6. Read back publication result. If acknowledgement is lost, query the unique operation key first; a committed event is success even when no longer current. If no event exists, reconcile artifacts and expected generation before retrying. Never change released files to record that they became current.

Directory rename and SQLite commit are not one transaction. On restart, finalized-but-unselected files are reported as orphans; they may be explicitly verified and selected using a fresh expected parent, or left untouched. Registry selection of a missing or corrupt release is an integrity incident: stop serving and require verified rollback. Process-interruption tests do not establish power-loss durability. Document actual filesystem, sync primitives and tested failure class before making stronger claims.

## 7. Reader contract and resource control

Consumer resolves `(release_id,generation)` once per page execution, verifies manifest/file identity on first use of that immutable release in the process, and opens a short-lived read-only DuckDB connection per query. No shared writable connection. Cache keys contain release ID, query-definition version and normalized filters. UI offers refresh to a newer release; in-progress queries retain their old release. Current operational checks/failures come from registry and are labeled separately from release-as-of profiles.

Eligibility is checked outside cached functions on every render/export and again before delivering a completed query: a pinned release must still be VERIFIED. Rejecting a release is an explicit control operation recording reason/time under the writer lock; it invalidates eligibility without editing its bytes or selecting a replacement. A rejected current release yields an unavailable view until explicit rollback/publication. A superseded but still VERIFIED release remains readable. Cached results must not bypass this check; data already displayed cannot be retroactively withdrawn, so the next render shows the incident. First-use digest verification is not continuous bit-rot detection: later corruption is detected on explicit verification or read failure, never advertised as impossible.

Consumer cache is in-memory only, bounded initially to 32 aggregate results, with at most 10,000 rows and 5 MiB serialized payload per result. Oversize results are not cached and the view requests narrower filters or pagination; metrics are not silently sampled or truncated. Account for copies and UI memory in measured RSS. Connection objects are not cache identity; release ID and semantic query version must be explicit hashed arguments.

Filters: selected source months, recorded pickup dates, license and pickup zone. Metrics are computed directly from eligible trip rows for the exact filter population when not already materialized at that exact grain. Never combine precomputed percentiles. Use parameterized queries and allowlisted sort/group expressions. Export only selected aggregate tables with release and denominator metadata, not bulk trip records by default.

Single writer; four DuckDB threads; 8 GiB engine target; 16 GiB spill cap; 80 GiB total data budget remain provisional. Preflight inventories raw, retained releases, candidate, WAL, spill and logs plus estimated headroom. Periodic size/RSS observation stops on limit and preserves current. Resource exhaustion cannot downgrade full-scan checks or silently shorten the requested window. A 12-month, three-release footprint is unproven; the scale gate may require an explicit storage/retention amendment, never an undisclosed reduction.

Reader concurrency is bounded too: one active analytical consumer query across local Streamlit processes, using a separate OS query lock with bounded waiting and a visible busy state. Reader connection initially uses one thread and a 1 GiB engine target. Thus writer-plus-reader engine targets sum to 9 GiB, not a claim of 9 GiB process RSS. Split the global 16 GiB spill cap into at most 14 GiB writer and 2 GiB reader directories, and measure both processes together. Cache hits need no DB query slot. These provisional reader allocations must pass the combined-workload scale gate.

Cleanup is manual and previewed. V1 cleanup requires the consumer stopped and no open artifact handles, then writer lock and verified protected set (current + two distinct prior successfully selected releases, raw evidence and manifests). Repeated selection of the same release does not shrink this set. OS unlink behavior is not a retention policy. No automated TTL or untrusted stale reader lease authorizes deletion.

Persist the reviewed cleanup request and PREPARED item before deleting any payload. An eligible old release becomes RETIRED, with status event, before its database is removed; preserve its manifest/report and audit records. After deletion synchronize affected directories and record DELETED. On interruption, a matching PREPARED item plus absent exact target is reconciled as completed removal; a surviving target must be reverified before retry. Absence without such intent is an integrity incident. RETIRED is terminal in v1; build a new release from retained originals if the historical result is needed again. These metadata records also supply the operation receipts required by [operations design](../reference/interfaces-and-operations.md).

## 8. Sources and open verification boundaries

Consulted 2026-11-09; source-derived behavior informs these decisions, but does not prove implementation.

- [DuckDB CHECKPOINT](https://duckdb.org/docs/2.0/sql/statements/checkpoint): synchronize WAL into database, complete transactions first.
- [SQLite pragmas](https://www.sqlite.org/pragma.html): journal/synchronization configuration and verification of effective settings.
- [Python filesystem operations](https://docs.python.org/3/library/os.html): file synchronization and rename primitives; host durability still requires verification.
- [Arrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html): bounded batch/row-group reading and metadata inspection.
- [Dagster partitioning](https://dagster.io/docs/guides/build/partitions-and-backfills/partitioning-assets): source partitions are orchestration units, not application publication authority.

Exact dependency versions, supported macOS/filesystem tuple, scale timings, peak disk/RSS and physical durability remain implementation acceptance gates. No packages are installed and no data pipeline is executed by this document.
