# CourtListener Bulk Case Law

**Source:** [https://www.courtlistener.com/help/api/bulk-data/](https://www.courtlistener.com/help/api/bulk-data/)
**Country:** US
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** Out of memory / resource constraints

**Technical reason:** `file_too_large`

**Details:** BLOCKED: Bulk opinion files are 30-50GB compressed each (S3 bucket). Exceeds 2GB download limit. Use US/CourtListener API source instead (needs free token from courtlistener.com). US/CaselawAccessProject already provides 6.7M cases. Issue #185.

## How you can help

The data source is too large to process with available resources.
- Streaming/chunked processing architecture needed
- If you have experience with large dataset processing, a PR would help

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
