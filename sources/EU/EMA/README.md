# EU/EMA - European Medicines Agency

Data source for EU medicines authorization data from the European Medicines Agency.

## Data Types

- **doctrine**: EPAR summaries and medicine authorization information

## Source

- Website: https://www.ema.europa.eu
- JSON Data: https://www.ema.europa.eu/en/about-us/about-website/download-website-data-json-data-format
- Data updated: Twice daily (06:00 and 18:00 CET)

## Coverage

- ~1,500+ authorized human medicines (centralized procedure)
- EPAR (European Public Assessment Report) summaries
- Therapeutic indications, active substances, authorization status

## Full Text Content

Text is extracted from:
1. **EPAR PDF summaries** - "Medicine overview" documents (primary source)
2. **Therapeutic indication** field - fallback when EPAR summary unavailable

## Authentication

None required - public open data.

## Usage

```bash
# Test connection
python3 bootstrap.py test

# Count available medicines
python3 bootstrap.py count

# Fetch sample records (12 records)
python3 bootstrap.py bootstrap --sample

# Fetch all records
python3 bootstrap.py fetch
```

## Rate Limiting

2 second delay between PDF downloads to respect server capacity.

## Schema

Key fields:
- `ema_product_number`: EMA product identifier (e.g., EMEA/H/C/006284)
- `medicine_name`: Commercial name
- `active_substance`: Active pharmaceutical ingredient
- `therapeutic_area`: MeSH therapeutic area classification
- `medicine_status`: Authorization status (Authorised, Withdrawn, etc.)

## License

[EUR-Lex legal notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — EU agency public data, reuse authorised with attribution.
