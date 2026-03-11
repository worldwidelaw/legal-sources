# EE/Riigikogu - Estonian Parliament Legislative Drafts

## Overview

Fetches legislative drafts (bills) from the Estonian Parliament (Riigikogu)
Open Data API. This source provides access to bills before they become enacted law.

**Website:** https://www.riigikogu.ee
**API:** https://api.riigikogu.ee
**Country:** Estonia (EE)
**Data Type:** Legislation (drafts/bills)
**License:** CC BY-SA 3.0
**Language:** Estonian (et)

## Data Access Method

This scraper uses the official Riigikogu REST API:
1. **Drafts list:** `/api/volumes/drafts` - paginated list of all legislative drafts
2. **Draft details:** `/api/volumes/drafts/{uuid}` - full details including attached files
3. **File download:** `/api/files/{uuid}/download` - download docx files for text extraction

The API provides data from 2012 onwards (earlier data may be incomplete).

## Full Text Extraction

Text is obtained from two sources (in priority order):
1. **DOCX files:** Draft texts attached as Word documents are downloaded and parsed
2. **Introduction field:** Summary/explanation text provided in the API response

The API rate limits file downloads, so some records may use the introduction
text as fallback.

## Draft Types

- **SE** - Seaduseelnõu (Law draft/bill) - primary legislation
- **OE** - Otsuse eelnõu (Resolution draft) - parliamentary resolutions
- **AE** - Avalduse eelnõu (Declaration draft)

## Relationship to EE/RiigiTeatajaLoomal

- **EE/RiigiTeatajaLoomal** covers **enacted legislation** (laws that have been passed)
- **EE/Riigikogu** covers **legislative drafts** (bills in progress)

Use both sources for complete coverage of the Estonian legislative process.

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample documents (12 records)
python bootstrap.py bootstrap --sample

# Fetch more samples
python bootstrap.py bootstrap --sample --sample-size 20

# Full bootstrap (all drafts from 2012+)
python bootstrap.py bootstrap

# Incremental update (drafts from last week)
python bootstrap.py update
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Draft identifier (e.g., "758_SE") |
| `_source` | "EE/Riigikogu" |
| `_type` | "legislation" |
| `_fetched_at` | ISO 8601 timestamp |
| `title` | Bill title |
| `text` | Full text content (MANDATORY) |
| `date` | Date initiated (ISO 8601) |
| `url` | Link to Riigikogu web page |
| `uuid` | API UUID |
| `mark` | Bill number (e.g., 758) |
| `draft_type` | Type code (SE, OE, AE) |
| `draft_type_name` | Human-readable type name |
| `status` | Current status (e.g., MENETLUSSE_VOETUD, VASTU_VOETUD) |
| `stage` | Legislative stage |
| `initiators` | List of bill initiators |
| `leading_committee` | Responsible committee name |
| `membership` | Parliamentary session number |
| `introduction` | Summary/explanation text |

## Notes

- API provides data from 2012 onwards
- File downloads are rate limited; use adequate delays between requests
- Some drafts may only have introduction text if docx files are unavailable
- Total of ~8,700+ drafts available (as of Feb 2026)
