# Source register

Sources consulted during research on 2026-11-08/09. Dynamic documentation and package pages may change; future implementation must capture exact resolved versions. These are supporting references, not a license grant or an interoperability test.

| Source | Supports | Does not establish |
| --- | --- | --- |
| [TLC trip record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Official monthly acquisition entry point | Accuracy of every observation or guaranteed publication deadline |
| [HVFHV dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf) | Published field semantics | Resolution of observed on-scene coverage tension or timezone ambiguity |
| [Taxi zone lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) | Reference vocabulary | Historical validity intervals |
| [DuckDB concurrency](https://duckdb.org/docs/2.0/connect/concurrency) | Embedded access boundary | A tested multi-process application |
| [DuckDB checkpoint](https://duckdb.org/docs/2.0/sql/statements/checkpoint) | WAL/database checkpoint behavior | Cross-filesystem/SQLite transaction atomicity |
| [DuckDB workload tuning](https://duckdb.org/docs/2.0/guides/performance/how_to_tune_workloads) | Resource limits and out-of-core caveats | Guaranteed exact-aggregate memory fit |
| [SQLite pragmas](https://www.sqlite.org/pragma.html) | Synchronization and registry configuration | Power-loss validation on this host |
| [Arrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html) | Batch/row-group inspection interface | Pipeline implementation |
| [Dagster partitioning](https://dagster.io/docs/guides/build/partitions-and-backfills/partitioning-assets) | Orchestration partition concepts | Application publication correctness |
| [Streamlit cache](https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_data) | Cache execution semantics | Automatic rejection-aware cache validity |

See [technical readiness](../verification/technical-readiness.md) for package metadata sources and [source research](source-research.md) for original empirical observations. No external link is evidence of a completed local test.
