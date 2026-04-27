# DE/Bundesrat - German Federal Council

## Overview

The Bundesrat (Federal Council) is the legislative body representing the 16 German states (Länder) at the federal level. This data source provides access to parliamentary documents from the Bundesrat.

## Data Source

**DIP API** (Dokumentations- und Informationssystem für Parlamentsmaterialien)
- Portal: https://dip.bundestag.de
- API Base: https://search.dip.bundestag.de/api/v1

## Coverage

- ~100,000 Bundesrat documents (Drucksachen)
- Document types: Anträge (motions), Unterrichtungen (notifications), Empfehlungen (recommendations), etc.
- Full text of all parliamentary documents

## Authentication

A public demo API key is included (valid until May 2026):
```
OSOegLs.PR2lwJ1dwCeje9vTj7FPOt3hvpYKtwKkhw
```

To request your own key, email: parlamentsdokumentation@bundestag.de

## Usage

```bash
# Test fetch (3 documents)
python3 bootstrap.py

# Bootstrap sample data (12 documents with full text)
python3 bootstrap.py bootstrap --sample
```

## Data Schema

Each normalized document contains:

| Field | Description |
|-------|-------------|
| `_id` | DIP document ID |
| `_source` | `DE/Bundesrat` |
| `_type` | `legislation` |
| `title` | Document title |
| `text` | Full text content |
| `date` | Publication date (YYYY-MM-DD) |
| `url` | Link to DIP portal |
| `pdf_url` | Link to PDF version |
| `document_number` | Official document number (e.g., "91/26") |
| `document_type` | Type (Antrag, Unterrichtung, etc.) |
| `electoral_period` | Wahlperiode number |
| `authors` | List of originating entities |

## API Endpoints Used

- `/drucksache-text` - List documents with full text (BR only)
- Filter: `f.zuordnung=BR` to get Bundesrat documents only

## Rate Limits

- Max 25 concurrent requests
- 500ms delay between requests (built into fetcher)

## License

Open Data — [Data licence Germany – attribution – Version 2.0](https://www.govdata.de/dl-de/by-2-0).
