# EU/ERA - European Union Agency for Railways

## Overview

This fetcher collects opinions, technical advice, and recommendations from the European Union Agency for Railways (ERA).

## Data Types

- **doctrine**: Official guidance documents from ERA

## Document Categories

1. **Opinions** (ERA/OPI/YYYY-N): Non-binding expert guidance to Member States on national railway rules
2. **Technical Advice** (ERA/ADV/YYYY-N): Implementation guidance issued to the European Commission
3. **Recommendations** (ERANNNN): Formal recommendations on railway regulations and standards

## Sources

- Opinions & Technical Advice: https://www.era.europa.eu/library/documents-regulations/opinions-and-technical-advices
- Recommendations: https://www.era.europa.eu/library/documents-regulations/era-recommendations_en

## Implementation

The fetcher:
1. Scrapes paginated listing pages for document links
2. Visits each document content page to extract metadata
3. Downloads PDF attachments
4. Extracts full text using pdfplumber/PyPDF2
5. Normalizes to standard schema

## Usage

```bash
# Test mode (3 documents)
python3 bootstrap.py

# Sample mode (15 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (50 documents)
python3 bootstrap.py bootstrap
```

## Schema

| Field | Type | Description |
|-------|------|-------------|
| document_number | string | ERA reference (e.g., "ERA/OPI/2026-1") |
| document_type | string | opinion, technical_advice, or recommendation |
| title | string | Document title |
| date | date | Publication date |
| addressee | string | Recipient (Member State or Commission) |
| text | text | Full text from PDF |

## Rate Limiting

1 request per 2 seconds to respect ERA server capacity.
