# PL/ConstitutionalCourt

**Polish Constitutional Court (Trybunał Konstytucyjny)**

## Overview

This source fetches case law from the Polish Constitutional Court via the SAOS API
(System Analizy Orzeczeń Sądowych - Court Judgments Analysis System).

## Data Source

- **Official Website**: https://trybunal.gov.pl
- **API Provider**: SAOS (https://www.saos.org.pl)
- **API Documentation**: https://www.saos.org.pl/help/index.php/dokumentacja-api

## Coverage

- Constitutional Court judgments from 1985 to present
- Approximately 8,000+ judgments available
- Includes:
  - Wyroki (Judgments/Sentences)
  - Postanowienia (Decisions)
  - Uchwały (Resolutions)

## API Details

The SAOS API provides:
- Search endpoint: `GET /api/search/judgments?courtType=CONSTITUTIONAL_TRIBUNAL`
- Detail endpoint: `GET /api/judgments/{id}`
- Full text in `textContent` field
- Pagination with max 100 items per page

## Data Fields

- `case_number`: Official case reference (e.g., "K 7/94", "P 1/21")
- `judgment_date`: Date of the judgment
- `judgment_type`: DECISION, SENTENCE, RESOLUTION, or REASONS
- `judges`: List of judges with roles (presiding, reporting)
- `text`: Full text of the judgment
- `keywords`: Subject matter keywords
- `referenced_regulations`: Laws and regulations cited

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — SAOS database, cite source.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```
