# US/VA-Law — Virginia Law Portal (Code of Virginia + Admin Code) REST API

**Source:** [https://law.lis.virginia.gov/developers/](https://law.lis.virginia.gov/developers/)
**Data types:** legislation

## Commands

```
python bootstrap.py test-api             # connectivity check
python bootstrap.py bootstrap            # full pull (CoV + VAC + Constitution)
python bootstrap.py bootstrap-fast       # alias for the full pull (fleet wrapper)
python bootstrap.py bootstrap --sample   # ~10 sample sections
python bootstrap.py update               # incremental refresh — changed sections only
```

## Incremental refresh (#1502)

The portal exposes **no upstream modified stamp anywhere**, so `fetch_updates`
cannot narrow on its `since` cutoff. Verified 2026-08-30:

- The complete API surface is published at `/jsonapi/` — 22 operations, none of
  which take a date or return one.
- Responses come back `Cache-Control: no-cache` with no `Last-Modified` and no
  `ETag`, so a conditional GET buys nothing.
- The bulk CSVs under `/CSV/` (which do carry `Last-Modified`) all share one
  batch stamp of 2025-08-13 and contain zero `2026, c.` citations, while the API
  already serves 2026 amendments — the dump lags the live corpus by a year, so
  its mtime is not a usable comparator either.

The comparator is therefore the **upstream body itself**: every run records
`sha1(text)` per section in `data/va_law_state.json`, and a refresh yields only
the sections whose upstream text no longer matches. That is
`incremental_comparator = "availability"` in `common/base_scraper.py` — an
upstream content hash, not a date read out of the document.

`data/` is gitignored, so **keep the working directory between refreshes**. On a
host with no state file the first refresh re-yields the whole corpus to build the
baseline (it says so in the log) and narrows from then on.

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
