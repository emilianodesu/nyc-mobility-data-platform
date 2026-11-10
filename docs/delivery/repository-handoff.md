# HVFHV — Documentation handoff for a future clean repository

Date: 2026-11-09. Scope: documentation packaging, not repository initialization or implementation. There is no single universally mandatory industry directory tree. This project uses familiar, proportionate conventions: root orientation, requirements, architecture, decision records, reference contracts, operational procedures and verification evidence.

The structure draws on [C4](https://c4model.com/) for architectural views, [ADRs](https://adr.github.io/) for decision rationale, and [Diátaxis](https://diataxis.fr/) for separating explanation, reference and operational guidance. It does not claim certification or require all framework artifacts.

## Target documentation structure

```text
README.md
CONTRIBUTING.md
docs/
  README.md
  requirements.md
  architecture/
    overview.md
    low-level-design.md
    diagrams.md
    decisions/
      README.md
      0001-monthly-source-and-semantic-boundary.md
      0002-local-native-analytical-storage.md
      0003-immutable-releases-and-selection.md
      0004-quality-and-recorded-intervals.md
      0005-orchestration-and-consumer.md
  reference/
    data-contracts.md
    interfaces-and-operations.md
  verification/
    acceptance-plan.md
    design-review.md
    technical-readiness.md
    walkthrough-results.md
    review-checklist.md
  evidence/
    source-research.md
    source-register.md
  delivery/
    implementation-sequence.md
    repository-handoff.md
    package-manifest.json
```

Root README explains the problem, source, state and navigation. It must say design-only until executable behavior exists. CONTRIBUTING documents evidence, changes and local/public boundaries; it must not invent test commands. ADRs explain alternatives and consequences. Reference files specify interfaces and fields. Verification separates completed document checks, hand-worked examples and future runtime tests. Evidence retains source URLs, methods, dates and limitations. No tutorial is fabricated before a working workflow exists.

## Transfer and authority

Prepare this tree as a documentation staging folder under the private definition workspace. It is not a Git repository and contains no application scaffold. Exported files are a review snapshot; original definition files and research remain preserved. A manifest records input/output hashes and transformation of internal links. After the user elects to initialize the standalone project, its reviewed docs become the project's maintained source; avoid editing two competing copies.

Only project-relevant content crosses the boundary. Exclude CVs, LinkedIn observations, employment information, CareerBooster source/registry, personal paths, agent session memory, secrets, caches, original multi-million-row data, failed prototypes and unrelated Gemini career rules. Public source evidence may be summarized with precise limitations; keep private original research accessible in its existing workspace. Documentation remains local/private until public review.

Do not copy `.git`, initialize a remote, invent a license grant, generate GitHub Actions, create package configuration, lock dependencies or write SQL/Python tests during this phase. A later code license decision must be separate from any dataset use terms. This document itself is an instruction for the handoff process, not a claim these future actions occurred.

## Later implementation additions, explicitly absent now

`pyproject.toml`, a verified dependency lock, `src/`, `sql/`, executable contracts, `tests/` and CI workflows are added only with implementation. Every proposed command must become a tested real command before it appears as a quickstart. A future `.gitignore` excludes data roots, environments, secrets, caches, logs and private evidence; it must be verified against actual staged files, not trusted as the sole privacy control.

When implementation starts, begin G0 dependency compatibility and G1 acquisition; then prove one real monthly vertical slice. Do not create infrastructure to make the repository appear mature before the source/model contract works. Future CI should run small deterministic checks without downloading the yearly dataset; explicit full-source integration runs produce separate evidence. None of these workflows is implemented here.

## Packaging acceptance

The package must be self-contained: no local absolute paths, no links outside its root, no dependency on CareerBooster to understand the project. It must preserve selected architecture and source uncertainties. Every required topic has one navigable owner. Each requirement maps to an acceptance scenario or a documentary check. Manifest hashes and local link checks pass. Documentation review must explicitly report any incomplete deliverable; file existence alone is insufficient.
