# BE/VlaamseCodex - Flemish Legislation Database

## Overview

The Vlaamse Codex is an unofficial consolidation of all Flemish laws and regulations maintained by the Flemish Government. It provides daily-updated, consolidated legislation texts.

## Data Source

- **Portal**: https://codex.vlaanderen.be
- **API**: https://codex.opendata.api.vlaanderen.be
- **API Docs**: https://codex.opendata.api.vlaanderen.be/docs/
- **License**: Open Data (Gratis hergebruik)
- **Language**: Dutch (nl)

## Coverage

- **Document types**: Decreten (Decrees), Besluit van de Vlaamse Regering (BVR), Koninklijk Besluit, Omzendbrieven, etc.
- **Total documents**: ~39,000
- **Time range**: 1970 - present
- **Update frequency**: Daily

## API Endpoints Used

| Endpoint | Description |
|----------|-------------|
| `/api/WetgevingDocument` | List all documents with pagination |
| `/api/v2/WetgevingDocument/{id}/VolledigDocument` | Get full document with article text |
| `/api/WetgevingDocument/BijgewerktTot` | Get last update date |

## Usage

```bash
# Check API status
python3 bootstrap.py status

# Fetch sample data (15 documents)
python3 bootstrap.py bootstrap --sample

# Fetch all documents (long operation)
python3 bootstrap.py bootstrap
```

## Sample Output

Each record contains:
- `_id`: Unique identifier (e.g., "BE/VlaamseCodex/1000001")
- `title`: Document title (Opschrift)
- `text`: Full text of all articles
- `date`: Document date
- `document_type`: Type (Decreet, BVR, etc.)
- `numac`: Belgian Official Gazette identifier
- `url`: Link to original document

## Notes

- The API is public and requires no authentication
- Rate limit: ~1 request/second recommended
- Not all documents have full text (`HeeftInhoud` flag)
- Text is extracted from `ArtikelVersies[].ArtikelVersie.Tekst`
