# US/WA-Legislation — Washington State Legislative Web Services

## Overview
Full text of Washington state legislation via official SOAP/REST web services and the lawfilesext.leg.wa.gov file server.

## Data Sources
- **RCW (Revised Code of Washington)**: ~100 titles of codified statutes crawled from `lawfilesext.leg.wa.gov/law/RCW/` directory listings
- **Bills**: 6400+ bill documents per biennium via `LegislativeDocumentService.asmx` SOAP/REST API with HTML full text from `lawfilesext.leg.wa.gov`

## Authentication
None required. All endpoints are publicly accessible.

## Usage
```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records (10 RCW + 5 bills)
python bootstrap.py bootstrap --sample

# Full fetch (all RCW + current biennium bills)
python bootstrap.py bootstrap

# Same, with concurrent normalization
python bootstrap.py bootstrap-fast

# Incremental refresh — only what changed since the last run
python bootstrap.py update

# Incremental refresh from an explicit cutoff (dry count, no ingest)
python bootstrap.py update --since 2026-07-15
```

## Incremental refresh

Both collections expose an upstream *availability* timestamp, so a refresh
never walks the whole corpus (issue #1502):

- **RCW** — the IIS directory listings on `lawfilesext.leg.wa.gov` print an
  mtime for every entry. Section files are filtered on their own listed mtime;
  chapter directories whose mtime predates the cutoff are skipped without a
  request, because WA republishes a chapter by rewriting the files inside it,
  so the directory stamp is never older than its newest file. **Title**
  directories are deliberately not used for pruning — their mtimes lag their
  chapters' (title 44 reads 2026-07-11 while a chapter inside reads
  2026-07-16), so pruning there would silently drop real updates.
- **Bills** — `GetAllDocumentsByClass` returns `HtmLastModifiedDate` per
  document, so one request per biennium yields the whole change set. A refresh
  covers the current and previous biennium, since bills are amended across the
  boundary.

The comparison is against when a document became available to us, never
against a date inside the document — a section whose text is decades old still
comes through when WA republishes the file.

## License

[Public domain](https://www.law.cornell.edu/uscode/text/17/105) — US government works under 17 U.S.C. § 105.

## API Endpoints
- SOAP services: https://wslwebservices.leg.wa.gov/
- RCW file server: https://lawfilesext.leg.wa.gov/law/RCW/
- Bill documents: https://lawfilesext.leg.wa.gov/biennium/{biennium}/Htm/Bills/
