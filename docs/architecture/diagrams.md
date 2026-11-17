# Architecture views

Design notation inspired by C4; Mermaid diagrams are simplified views, not a formal C4 conformance claim. “Process” below means a local program, not a Docker container. Text contracts are authoritative.

## Context and runtime responsibilities

```mermaid
flowchart LR
    TLC[Official TLC monthly objects and zone lookup] -->|HTTPS| Worker[Python acquisition and transform process]
    Operator[Local operator] --> Scheduler[Dagster OSS]
    Operator -->|Explicit local operation| Worker
    Scheduler -->|Invoke application contracts| Worker
    Worker --> Raw[Immutable original files]
    Raw --> Worker
    Worker --> Candidate[Unpublished DuckDB candidate]
    Candidate -->|Validate and finalize| Releases[Immutable analytical releases]
    Worker -->|Short control transactions| Registry[SQLite selection and audit registry]
    Reader[Streamlit read-only consumer] -->|Pin identity and eligibility| Registry
    Reader -->|Read verified selected release| Releases
    Reviewer[Mobility analyst or technical reviewer] --> Reader
```

The control registry is the single current-release authority. Dagster metadata is scheduling/run history; it is not a second selection pointer. All processes run locally; external traffic is source acquisition. Services are intended for loopback, with no public deployment in v1.

## Publication sequence

```mermaid
sequenceDiagram
    participant W as Serialized worker
    participant D as Candidate database
    participant F as Local filesystem
    participant R as SQLite control
    W->>R: Persist immutable selection request
    W->>D: Build and validate pre-seal inventory
    W->>D: Commit, checkpoint, close
    W->>F: Write report and manifest, hash and sync
    W->>F: Finalize unique release directory
    W->>R: Register and append post-seal attestation
    W->>R: Transaction: compare generation, select, append event
    R-->>W: Committed event or conflict
```

The filesystem and SQLite are not one transaction. A finalized unselected release remains an orphan until explicit verified reconciliation. Loss of acknowledgement is resolved through the original operation key; no blind repeat.

## Model dependencies

```mermaid
flowchart TD
    Original[Exact monthly originals] --> Trips[Trip observations with physical provenance]
    Zone[Exact zone reference revision] --> Qualified[Qualified trip view]
    Trips --> Qualified
    Trips --> Profiles[Full-scan profiles and source reconciliation]
    Qualified --> Profiles
    Qualified --> Metrics[Daily and monthly interval aggregates]
    Profiles --> Gate[Pre-seal rule inventory]
    Metrics --> Gate
    Gate --> Seal[Closed DB and immutable report]
    Seal --> Verify[External post-seal attestation]
```

Source revision changes replace their contribution. Reference/rule/metric changes invalidate documented dependents. Recomputing aggregate relations after incremental ingestion is intentional. No computed percentile is merged from subgroup percentiles.

These diagrams summarize runtime responsibilities, publication sequence, and model lineage; consult the operations design for full failure-recovery paths.
