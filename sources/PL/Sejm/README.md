# PL/Sejm - Polish Parliament (Sejm)

## Overview

This data source fetches parliamentary proceedings from the Polish Sejm (Lower House of Parliament) using the official Sejm API.

## Data Types

- **Transcripts**: Full text of parliamentary speeches and debates from plenary sessions
- **Interpellations**: Questions from Members of Parliament to government ministers, with full text of both questions and ministerial replies

## API Endpoints

Base URL: `https://api.sejm.gov.pl/sejm`

| Endpoint | Description |
|----------|-------------|
| `/term{N}/proceedings` | List of all proceedings for term N |
| `/term{N}/proceedings/{num}/{date}/transcripts` | Statements for a proceeding day |
| `/term{N}/proceedings/{num}/{date}/transcripts/{statementNum}` | HTML text of a statement |
| `/term{N}/interpellations` | List of interpellations |
| `/term{N}/interpellations/{num}/body` | HTML text of an interpellation |
| `/term{N}/interpellations/{num}/reply/{key}/body` | HTML text of a reply |

## Coverage

- **Term 10** (2023-present): Current parliamentary term
- **Terms 7-9** (2011-2023): Historical data available via API

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full data pull
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Authentication

No authentication required - open government data.

## Rate Limiting

The scraper uses a 2 requests/second rate limit with burst capacity of 5.

## Data Format

Documents are normalized to the standard schema with:
- `_id`: Unique identifier (e.g., `PL/Sejm/T10/INT1` or `PL/Sejm/T10/P11/2024-05-08/S5`)
- `title`: Document title with speaker/proceeding info
- `text`: Full text content (mandatory)
- `date`: Date in ISO 8601 format
- `url`: Link to original document on sejm.gov.pl
- `doc_type`: Either "transcript" or "interpellation"

## License

[Public Domain](https://dane.gov.pl) — Polish government open data.

## References

- [Sejm API Documentation](https://api.sejm.gov.pl/)
- [OpenAPI Specification](https://api.sejm.gov.pl/sejm/openapi/)
- [Swagger UI](https://api.sejm.gov.pl/sejm/openapi/ui/)
- [Sejm Website](https://www.sejm.gov.pl)
