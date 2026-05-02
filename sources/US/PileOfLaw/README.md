# Pile of Law (HuggingFace)

**Source:** [https://huggingface.co/datasets/pile-of-law/pile-of-law](https://huggingface.co/datasets/pile-of-law/pile-of-law)
**Country:** US
**Data types:** legislation, case_law, doctrine
**Status:** Blocked

## Why this source is blocked

**Category:** Out of memory / resource constraints

**Technical reason:** `too_large`

**Details:** 256GB total (xz-compressed JSONL on HuggingFace). Loading script no longer supported by datasets library. Also an aggregation of 35 primary sources (CourtListener, CFR, etc.) that should be fetched directly. Individual subsets available but still very large.

## How you can help

The data source is too large to process with available resources.
- Streaming/chunked processing architecture needed
- If you have experience with large dataset processing, a PR would help

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
