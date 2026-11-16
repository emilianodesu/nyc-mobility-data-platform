# Architectural decision log

Status for every record: recorded design choice, implementation pending; no separate user approval is fabricated. Date: 2026-11-09. Later replacements should supersede records with a new decision rather than erase rationale.

- [0001: Use monthly HVFHV with explicit semantic limits](0001-monthly-source-and-semantic-boundary.md)
- [0002: Use local DuckDB and original Parquet](0002-local-native-analytical-storage.md)
- [0003: Select immutable analytical releases transactionally](0003-immutable-releases-and-selection.md)
- [0004: Keep quality findings separate from metric eligibility](0004-quality-and-recorded-intervals.md)
- [0005: Use Dagster OSS and a bounded local read-only consumer](0005-orchestration-and-consumer.md)
- [0006: Amend retained analytical release policy to Option A (current plus one prior verified release)](0006-amend-release-retention-to-option-a.md)
