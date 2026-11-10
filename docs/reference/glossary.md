# Glossary

| Term | Meaning in this project |
| --- | --- |
| HVFHV | TLC high-volume for-hire vehicle trip publication selected as primary business source |
| Source month | Month encoded by the logical monthly source object; not a guarantee about every event timestamp |
| Event date | Date derived from a recorded pickup value without invented timezone conversion |
| Observation | A source check/capture with its time and HTTP evidence |
| Revision | Exact completed bytes associated with a logical source |
| Row locator | Revision plus zero-based physical row position, not a business trip key |
| Derivation | Hash-bound frozen inputs and transformation/rule/environment recipe |
| Attempt | One execution with recorded phase/outcome; retries get new attempts |
| Candidate | Unpublished analytical database under construction |
| Release | Immutable finalized analytical artifact set |
| Publication | Local transactional selection of a release, not internet publishing |
| Generation | Monotonic selection counter detecting intervening pointer changes |
| Rejection | Explicit control denial of future serving/selection |
| Retirement | Audited removal of an unprotected old release payload; retained evidence remains |
| Orphan | Finalized bytes not fully registered/selected; reconciliation never auto-publishes them |
| Recorded interval | Arithmetic pickup-minus-request difference on source values; not verified elapsed customer wait |
| Eligibility | Metric-specific inclusion policy, distinct from whether a whole source row is useful |
| Gate | Explicit acceptance condition; a future scenario is not a passed gate |
| Footers / samples | Metadata or partial observations; neither proves full-population data quality |
| ADR | Record of a significant architecture choice, rationale, alternatives and consequences |
