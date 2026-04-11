# IS/Reglugerdir - Icelandic Regulations (Reglugerðir)

## Overview

This source fetches regulations from the Icelandic government via the island.is portal.

## Data Source

- **Website**: https://island.is/reglugerdir
- **API**: https://island.is/api/graphql
- **Type**: Regulations
- **Language**: Icelandic
- **License**: Public Domain (official government regulations)
- **Volume**: ~2,487 in-force regulations

## Access Method

GraphQL API at `https://island.is/api/graphql`. Key queries:

- `getRegulationsYears` - Lists available years
- `getRegulationsSearch` - Search/paginate regulations by year
- `getRegulation` - Fetch individual regulation text and metadata

Regulation numbers follow the format `NNNN/YYYY` (e.g., `0006/2026`).

## Fields

| Field | Description |
|-------|-------------|
| _id | Regulation number with underscore (e.g., "0006_2026") |
| regulation_number | Regulation number (e.g., "0006/2026") |
| title | Regulation title |
| date | Signature date (ISO 8601) |
| text | Full regulation text (HTML stripped) |
| ministry | Issuing ministry name |
| law_chapters | Associated law chapter classifications |
| effective_date | Date the regulation takes effect |

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Fetch all records
python3 bootstrap.py fetch

# Fetch updates since a date
python3 bootstrap.py update --since 2024-01-01

# Show source info and available years
python3 bootstrap.py info
```

## Notes

- Regulations are issued by ministries and published in Stjórnartíðindi B
- The GraphQL API returns regulation text as HTML which is stripped to plain text
- Regulations may reference and amend other regulations (history field)
- Some regulations span back to the early 20th century
- Rate limit: 1.0 second delay between API requests
