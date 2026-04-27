# DK/DTIL - Danish Data Protection Authority (Datatilsynet)

## Overview

This source fetches GDPR enforcement decisions from the Danish Data Protection Authority (Datatilsynet).

**Website:** https://www.datatilsynet.dk
**Data Type:** Regulatory Decisions
**Language:** Danish
**Authentication:** None required (Open Data)

## Data Source

Datatilsynet publishes all GDPR enforcement decisions on their website. Decisions include:
- Supervisory inspections (tilsyn)
- Complaint decisions (klagesager)
- Data breach reports (brud på persondatasikkerheden)
- Fines and sanctions (bødesager)

## Strategy

1. **Discovery:** Parse the XML sitemap at `/Handlers/Sitemap.ashx`
2. **Filter:** Extract URLs matching pattern `/afgoerelser/afgoerelser/YYYY/mon/slug`
3. **Fetch:** Download each HTML decision page
4. **Extract:** Parse HTML to extract full text, title, date, and case number

## URL Patterns

- **Sitemap:** `https://www.datatilsynet.dk/Handlers/Sitemap.ashx`
- **Decision:** `https://www.datatilsynet.dk/afgoerelser/afgoerelser/2024/jan/decision-slug`

## Fields

| Field | Description |
|-------|-------------|
| `_id` | Unique ID (DK-DTIL-{case_number} or DK-DTIL-{year}-{month}-{slug}) |
| `title` | Decision title |
| `text` | Full text of the decision |
| `date` | Decision date (YYYY-MM-DD) |
| `case_number` | Journal number (e.g., 2023-431-0001) |
| `url` | Link to original decision |
| `authority` | "Datatilsynet" |
| `language` | "da" (Danish) |

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Rate Limiting

- 1 request per second
- Burst of 3 requests allowed

## Coverage

~400 decisions available from 2018-present, covering:
- GDPR Article violations
- Data breach handling
- Consent requirements
- Data subject rights
- International transfers

## License

Public domain — Danish government decisions are not subject to copyright under [Danish Copyright Act §9](https://www.retsinformation.dk/eli/lta/2023/164).
