# Research Archive & Schema Inspection Records

This directory preserves historical NYC TLC source inspection records, schema profiles (`fhvhv_tripdata` 24-column vs. 25-column formats, zone lookup tables), and exploratory methodology notes.

- Inspection summaries are stored as `.md.txt` / `.py.txt` alongside JSON schema profiles so exploratory scripts remain separate from production modules under `src/hvfhv/`.
- Large upstream Parquet datasets are not tracked in Git; URLs, observed metadata, and methodological bounds are documented for reproducibility.
- [Preservation ledger](archive-inventory.json) records archived artifacts and transformations.
