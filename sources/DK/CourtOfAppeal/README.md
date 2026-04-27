# DK/CourtOfAppeal - Danish Courts Database (Domsdatabasen)

## Overview
This source fetches court decisions from the Danish Courts Administration's public database (Domsdatabasen).

**URL:** https://domsdatabasen.dk

## Data Coverage
- **Supreme Court** (Højesteret)
- **Eastern High Court** (Østre Landsret)
- **Western High Court** (Vestre Landsret)
- **District Courts** (Byretter)
- **Maritime and Commercial High Court**

Contains both civil and criminal cases published from January 2022 onwards.

## API Endpoints
The source uses the public web API:
- RSS Feed: `GET /webapi/api/Case/rss` - List of cases
- Case Detail: `GET /webapi/api/Case/get/{id}` - Case metadata and documents
- Document: `GET /webapi/api/Case/document/{id}` - Full document content (HTML)

## License

Public domain — Danish court decisions are not subject to copyright under [Danish Copyright Act §9](https://www.retsinformation.dk/eli/lta/2023/164).

## Usage
```bash
# Test the fetcher
python3 bootstrap.py

# Fetch sample data (12 cases)
python3 bootstrap.py bootstrap --sample

# Fetch full dataset
python3 bootstrap.py bootstrap
```

## Data Schema
| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (DK-DDB-{id}) |
| `title` | Case headline |
| `text` | Full judgment text (cleaned from HTML) |
| `date` | Verdict date |
| `ecli` | European Case Law Identifier |
| `court` | Court name |
| `subjects` | Case topics |

## Notes
- Full text is provided as HTML and cleaned to plain text
- Some cases are anonymized to protect personal information
- Rate limiting: 1 request per second recommended
