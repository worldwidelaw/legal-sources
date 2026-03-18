# DE/BaFin - German Federal Financial Supervisory Authority

## Overview

BaFin (Bundesanstalt für Finanzdienstleistungsaufsicht) is Germany's integrated financial regulatory authority, supervising banks, insurance companies, investment funds, and securities trading.

## Data Source

This fetcher retrieves **regulatory circulars (Rundschreiben)** from BaFin's official website. These circulars provide binding regulatory guidance including:

- **MaRisk** - Minimum Requirements for Risk Management
- **MaComp** - Minimum Compliance Requirements
- **Anti-Money Laundering** - High-risk country lists and AML guidance
- **Supervisory practices** - Fit and proper requirements, capital requirements

## Technical Details

- **Method**: HTML scraping of search results and document pages
- **Documents**: ~126 active circulars
- **URL Pattern**: `https://www.bafin.de/SharedDocs/Veroeffentlichungen/DE/Rundschreiben/{YEAR}/{filename}.html`
- **Rate Limit**: 1.5 seconds between requests

## Usage

```bash
# Test mode (3 documents)
python3 bootstrap.py

# Full bootstrap (100 documents)
python3 bootstrap.py bootstrap

# Sample bootstrap (15 documents)
python3 bootstrap.py bootstrap --sample
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique document identifier |
| `_source` | Always "DE/BaFin" |
| `_type` | Always "regulatory_decisions" |
| `title` | Circular title |
| `text` | Full text content |
| `date` | Publication date (ISO 8601) |
| `url` | Original document URL |
| `topic` | Subject area (e.g., Risk Management, Compliance) |
| `reference` | Reference number if available |
| `authority` | Always "BaFin" |
| `language` | Always "de" |

## License

Documents are official government works and public domain under German law (§ 5 UrhG).
