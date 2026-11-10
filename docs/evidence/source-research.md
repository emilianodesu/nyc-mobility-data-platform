# Source research evidence

Observed 2026-11-08/09. This is a curated summary of preserved read-only research, not a fresh acquisition or a full-month validation. Source: official monthly HVFHV Parquet files at the TLC-linked CloudFront publication endpoint.

| Source month | Footer row count | Object bytes | Footer on-scene null count |
| --- | ---: | ---: | ---: |
| 2024-12 | 21,068,851 | 507,534,115 | 5,121,638 |
| 2025-01 | 20,405,666 | 491,076,642 | 5,035,135 |
| 2026-09 | 20,987,681 | 509,720,685 | 2 |

Original object pattern: `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_YYYY-MM.parquet`. Counts above trust Parquet writer metadata. Complete object content hashes were not obtained by the partial-range research; no SHA of the full source is claimed.

December 2024 has 24 observed columns; January 2025 and September 2026 have 25, adding CBD congestion fee. Timestamps were represented without timezone. No business trip ID, driver ID or vehicle ID was established in these releases. Zone lookup inspection found 265 unique IDs including Unknown/Outside NYC and Newark Airport.

For September 2026, the first 20,000 projected rows of physical row group 10 contained 501 negative recorded request-to-pickup differences; the final row group 20 contained 16,161 sampled rows and 74 such differences. Exact recorded trip duration differed from source trip_time in 4,673 and 3,811 rows respectively. These are nonrandom time-local samples, not estimated monthly rates. Causes and discrepancy tolerances were not established. Near-complete on-scene presence in the September footer does not resolve the dictionary's narrower definition.

These findings motivate preserving values, typed exceptions, schema-version handling and explicit denominator reporting. They do not justify automatic deduplication, timestamp repair, a driver-income model or claims of true customer waiting time. There was no throughput benchmark, full-year data load or production deployment.

Research source records remain preserved privately with capture timestamps, ETags and methods. The transfer manifest records hashes of the consulted evidence summaries/records without copying personal context or agent-session files. The public URLs and stated methods make this package understandable without accessing CareerBooster.

[Official sources and limitations](source-register.md)

The augmented ZIP also preserves those detailed records in [the research archive](../../references/README.md); the curated summary is no longer the only transferred evidence.
