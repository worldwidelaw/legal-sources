# DE/Bundestag - German Federal Parliament

## Overview

The Bundestag is the German federal parliament (lower house). This data source provides access to parliamentary documents including bills, motions, and reports.

## Data Source

**DIP API** (Dokumentations- und Informationssystem für Parlamentsmaterialien)
- Portal: https://dip.bundestag.de
- API Base: https://search.dip.bundestag.de/api/v1

## Coverage

- ~184,000 Bundestag documents (Drucksachen)
- Document types: Gesetzentwürfe (bills), Anträge (motions), Anfragen (inquiries), Berichte (reports), etc.
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
| `_source` | `DE/Bundestag` |
| `_type` | `legislation` |
| `title` | Document title |
| `text` | Full text content |
| `date` | Publication date (YYYY-MM-DD) |
| `url` | Link to DIP portal |
| `pdf_url` | Link to PDF version |
| `document_number` | Official document number (e.g., "20/12345") |
| `document_type` | Type (Gesetzentwurf, Antrag, etc.) |
| `electoral_period` | Wahlperiode number |
| `authors` | List of originating entities |

## API Endpoints Used

- `/drucksache-text` - List documents with full text (BT only)
- Filter: `f.zuordnung=BT` to get Bundestag documents only

## Rate Limits

- Max 25 concurrent requests
- 500ms delay between requests (built into fetcher)

## License

Open Data - Parliamentary documents are publicly available through the DIP API.
