# ES/CNMV - Spanish Securities Commission Sanctions

## Overview

This source fetches sanctions resolutions from the Spanish National Securities Market Commission (CNMV - Comisión Nacional del Mercado de Valores).

## Data Source

- **Registry URL**: https://www.cnmv.es/Portal/Consultas/RegistroSanciones/verRegSanciones
- **Data Type**: Regulatory decisions (sanctions)
- **Language**: Spanish
- **License**: Open Government License (see [License](#license) below)

## What's Included

The CNMV public sanctions registry contains:
- Sanctions for very serious infractions (infracciones muy graves)
- Sanctions for serious infractions (infracciones graves)
- Information about administrative appeals filed
- Resolution dates and sanctioned entities

## Technical Details

### Fetching Strategy

1. Scrapes the paginated HTML sanctions registry
2. Extracts PDF links for each sanction resolution
3. Downloads PDFs (published in the Official State Gazette - BOE)
4. Extracts full text using pdfminer

### API/Access Method

- HTML scraping of paginated table
- PDF documents via CNMV webservices endpoint
- No authentication required

### Rate Limiting

- 1 request per second
- 0.5s delay between PDF downloads

## Schema

Each record includes:
- `_id`: Unique identifier
- `title`: Resolution title
- `text`: Full text of the sanction resolution
- `date`: Resolution date (ISO 8601)
- `url`: Link to PDF document
- `description`: Summary of the sanction
- `sanctioned_entity`: Name of sanctioned party
- `infraction_type`: muy_grave, grave, or leve
- `appeals_info`: Information about appeals

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample data (10 records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Notes

- Sanctions are retained in the registry for 5 years after publication
- Full text is extracted from BOE PDFs
- The registry is paginated, typically 10-20 pages

## License

Open government data under [Spanish Reuse of Public Sector Information regulations](https://datos.gob.es/en/terms).
