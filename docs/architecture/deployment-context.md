# Deployment context — proposed local runtime

No deployment exists. Initial target is one local macOS machine with an ordinary CPython environment and local filesystem. Previously observed hardware was 24 GiB RAM and 10 reported CPUs; that observation is not a required minimum or performance guarantee. Exact supported OS/filesystem/interpreter tuple is determined in G0. Windows and network filesystems are not claimed supported.

Dagster's local scheduler/web UI invokes a serialized Python worker. DuckDB is embedded in the worker and reader, not a separate database server. SQLite is the control file. Streamlit is a separate read-only process. Web interfaces bind to loopback; no public ingress, cloud account or container daemon is required. Outbound HTTPS accesses official TLC source endpoints. No private employment or user profile information belongs in this runtime.

The data root must be outside tracked source. Candidate and finalized release directories share a filesystem for the documented rename protocol. Originals, registry, audit, staging, spill and Dagster metadata have distinct responsibilities. Backup/restore and cleanup must use the operations runbooks; copying a live SQLite file arbitrarily is not an approved backup protocol.

One mutating application process and one analytical consumer query are permitted at a time under their respective locks. Combined memory/disk measurements include both, not just DuckDB settings. The workstation may sleep or stop; catch-up is explicit on restart. No local schedule implies continuous availability.

[Detailed layout and budgets](low-level-design.md) · [Operational contracts](../reference/interfaces-and-operations.md).
