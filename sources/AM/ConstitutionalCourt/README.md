# AM/ConstitutionalCourt — Armenian Constitutional Court

## Overview

This source fetches decisions from the Constitutional Court of Armenia (Սdelays Դdelays) from their official website at [concourt.am](https://www.concourt.am).

## Data Coverage

- **Type:** Case law (constitutional decisions)
- **Years:** 2000-present
- **Volume:** ~1,800+ decisions
- **Language:** Armenian (full text), English (selected translations)
- **Update frequency:** Several decisions per month

## Data Access Method

HTML scraping of the decisions listing pages + PDF text extraction using PyMuPDF.

The Armenian version (`/decisions/cc-decision/`) provides full decision PDFs for all cases, while the English version (`/en/decisions/cc-decision/`) only has translations for selected major decisions.

## Decision Types

- **SDO/DCC:** Regular constitutional decisions
- **SDV:** Procedural decisions

## Technical Details

### URL Patterns

- Decisions listing: `https://www.concourt.am/decisions/cc-decision/?year=YYYY`
- PDF documents: `https://www.concourt.am/decision/decisions/{hash}_{filename}.pdf`
- Pagination: `?page=N` query parameter

### Sample Record

```json
{
  "_id": "AM/ConstitutionalCourt/SDO-1765",
  "_source": "AM/ConstitutionalCourt",
  "_type": "case_law",
  "decision_number": "SDO-1765",
  "title": "ON THE CASE CONCERNING...",
  "date": "2024-12-17",
  "text": "Հdelays DELAYS...",
  "url": "https://www.concourt.am/decision/decisions/...",
  "court": "Constitutional Court of Armenia",
  "jurisdiction": "AM",
  "language": "hy"
}
```

## Usage

```bash
# Fetch sample records (12 decisions)
python bootstrap.py bootstrap --sample

# Fetch all decisions
python bootstrap.py bootstrap

# Fetch updates since date
python bootstrap.py updates --since 2024-01-01
```

## Dependencies

- requests
- beautifulsoup4
- PyMuPDF (fitz)

## License

Public domain — official government publication.
