# HVFHV Monthly Data Reliability Platform

A proposed monthly end-to-end pipeline for making NYC TLC High Volume For-Hire Vehicle trip data traceable, inspectable and reproducible. It combines source revision tracking, explicit quality findings, consistent analytical releases and a local read-only consumer for recorded request-to-pickup intervals.

**Status: documentation-only design snapshot, 2026-11-09. No application, repository initialization, runtime tests or deployment is included. Historical inspection worksheets are preserved as inert text in the archive.** This is a portfolio engineering project with a fictional consumer; it does not claim production adoption or customer impact.

## Review path

1. [Requirements and boundaries](docs/requirements.md).
2. [Architecture](docs/architecture/overview.md), [diagrams](docs/architecture/diagrams.md) and [decision log](docs/architecture/decisions/README.md).
3. [Low-level design](docs/architecture/low-level-design.md), [data contracts](docs/reference/data-contracts.md) and [operations](docs/reference/interfaces-and-operations.md).
4. [Evidence](docs/evidence/source-research.md), [acceptance plan](docs/verification/acceptance-plan.md) and [review checklist](docs/verification/review-checklist.md).
5. [Implementation sequence](docs/delivery/implementation-sequence.md) and [handoff](docs/delivery/repository-handoff.md).

## Selected design

Python, local immutable original Parquet, DuckDB plus versioned SQL, SQLite control, Dagster OSS and a Streamlit read-only consumer. Data arrives monthly. Twelve consecutive source months form the initial analytical window. No Kafka, Spark, cloud service, Docker requirement or fabricated streaming feed is included.

Original values remain intact; quality findings control metric eligibility rather than deleting all unusual observations. The recorded interval is not proven customer waiting time. Real source research is partial where labeled; yearly performance is not measured.

There is deliberately no installation command or success badge for software that has not been built. Start with the documentation index below. Public distribution and code/data license decisions remain separate reviews.

[Documentation index](docs/README.md) · [Contribution policy](CONTRIBUTING.md)

[Package review result](docs/verification/package-review.md): documentary readiness, with all runtime checks still pending.

Portable entry point: [START-HERE](START-HERE.md). Full research preservation: [archive](references/README.md).

[Structure research](docs/delivery/structure-research.md) · [arc42 coverage](docs/architecture/arc42-index.md) · [Export verification](docs/verification/zip-export-review.md)
