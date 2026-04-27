# LT/SupremeCourt - Lithuanian Supreme Court of Cassation

## Overview

Case law from Lithuania's Supreme Court of Cassation (Lietuvos Aukščiausiasis Teismas),
the highest court for civil and criminal matters in Lithuania.

## Data Source

- **Primary**: LITEKO public decisions database (liteko.teismai.lt)
- **Dataset**: https://data.gov.lt/datasets/1938/
- **CSV files**: Monthly CSV exports at `liteko.teismai.lt/csv/viesi_sprendimai_YYYYMM.csv`
- **Full text**: HTML pages at `liteko.teismai.lt/viesasprendimupaieska/tekstas.aspx`

## Coverage

- Cases from 2010 onwards
- Civil and criminal cassation cases
- Full text available in Lithuanian

## Data Fields

- `case_number`: Official case number (e.g., "e3K-3-123/2025")
- `doc_id`: GUID for full text retrieval
- `court`: Always "Lietuvos Aukščiausiasis Teismas"
- `case_type`: Civil/criminal case type
- `instance`: Cassation instance
- `result`: Case outcome/decision
- `judges`: Judge name(s)
- `panel`: Panel composition
- `parties`: Case parties (anonymized)
- `categories`: Legal categories
- `text`: Full decision text (mandatory)

## Authentication

None required — public data under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) license.

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch 12 sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (all available months)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Lithuanian open government data.

## Notes

- The Supreme Court website (lat.lt) is Cloudflare-protected
- LITEKO database provides the same data via CSV + HTML endpoints
- Full text is fetched separately for each document
- Rate limiting: 2 second delay between requests
