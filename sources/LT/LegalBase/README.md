# LT/LegalBase - Lithuanian Legal Database (TAR)

## Overview

This source fetches Lithuanian legislation from the official Register of Legal Acts (Teises aktu registras - TAR) via the Lithuanian Open Data Portal API.

## Data Source

- **API Endpoint**: `https://get.data.gov.lt/datasets/gov/lrsk/teises_aktai/Dokumentas`
- **Dataset Page**: https://data.gov.lt/datasets/2613/
- **Official Portal**: https://e-tar.lt
- **License**: CC BY 4.0 (Creative Commons Attribution)

## Data Coverage

The database includes:
- Constitutional documents (Konstitucija)
- Laws (Istatymas)
- Government resolutions (Nutarimas)
- Ministerial orders (Isakymas)
- Municipal legal acts
- International treaties
- Court judgments

## API Details

The API provides:
- Full text content in the `tekstas_lt` field
- Rich metadata (dates, institutions, validity status)
- Cursor-based pagination
- JSON format responses

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch 12 sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Schema

Key fields:
- `dokumento_id`: Unique document identifier
- `pavadinimas`: Document title (Lithuanian)
- `tekstas_lt`: Full text content (Lithuanian)
- `rusis`: Document type
- `priimtas`: Adoption date
- `nuoroda`: URL to e-tar.lt

## Notes

- Data is updated daily from the official TAR registry
- All document types have full text available
- Validity status indicates whether the act is currently in force
