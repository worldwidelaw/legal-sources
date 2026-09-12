# EE/RiigiTeatajaLoomal - Estonian State Gazette

## Overview

Riigi Teataja (State Gazette) is Estonia's official legislation database, providing
free public access to all Estonian laws, regulations, and official documents.

**Website:** https://www.riigiteataja.ee
**Country:** Estonia (EE)
**Data Type:** Legislation
**License:** CC0 (Public Domain) - Free to use
**Language:** Estonian (et)

## Data Access Method

This scraper uses the official JSON API that the riigiteataja.ee front-end calls:

1. **Chronology API** for document discovery —
   `POST /api/v1/akt/kronoloogia` with
   `{"searchAfter": <offset>, "publicationDateStart": "YYYY-MM-DD", "publicationDateEnd": "YYYY-MM-DD"}`,
   returning `{"kokku": <total>, "tulemused": [...]}`. The server page size is
   fixed at 10 and `searchAfter` is a plain offset. Discovery walks one calendar
   month per window, from 1990 to the present (~244,000 acts).
2. **XML blob download** for full text —
   `GET /api/v1/akt/{id}/blob-xml` (`application/xml`).

Completed months are checkpointed to `data/chronology_checkpoint.json`, so a
re-launched run resumes instead of re-walking the archive.

> The legacy server-rendered `/kronoloogia_tulemus.html` chronology and the
> `/akt/{id}.xml` document URL stopped working when riigiteataja.ee was rebuilt
> as an Angular SPA — both now return the SPA HTML shell with HTTP 200, which is
> why the previous scraper silently discovered 0 documents (issue #1451).

The XML format is well-structured with full text content including:
- Complete legislation text with paragraph structure
- Metadata (issuer, dates, document type)
- Amendment history

## RT Parts

- **RT I:** Laws and national regulations (primary focus)
- **RT II:** International agreements
- **RT III:** Administrative acts
- **RT IV:** Local government acts

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample documents (10 records)
python bootstrap.py bootstrap --sample

# Fetch more samples
python bootstrap.py bootstrap --sample --sample-size 20

# Full bootstrap (WARNING: fetches from 1990 to present, ~244K acts)
python bootstrap.py bootstrap

# Full bootstrap with concurrent XML downloads (what the fleet runs)
python bootstrap.py bootstrap-fast

# Incremental update (last week)
python bootstrap.py update
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Document ID (e.g., "521052015001") |
| `_source` | "EE/RiigiTeatajaLoomal" |
| `_type` | "legislation" |
| `title` | Document title |
| `text` | Full text content (MANDATORY) |
| `date` | Enactment/effective date (ISO 8601) |
| `url` | Link to original document |
| `issuer` | Issuing authority (e.g., "Riigikogu") |
| `document_type` | Type (e.g., "seadus" = law) |
| `abbreviation` | Official abbreviation |
| `effective_date` | When the law takes effect |
| `expired_date` | When the law expires (if applicable) |

## Notes

- Documents are stored in XML format with clean structured text
- The database covers legislation from 1990 to present
- Full text is extracted from `<sisu>` (content) elements
- Rate limiting is applied to respect server resources

## License

[CC0 1.0 Universal — Public Domain](https://creativecommons.org/publicdomain/zero/1.0/) — Estonian legislation is free to use.
