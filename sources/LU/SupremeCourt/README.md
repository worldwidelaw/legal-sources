# LU/SupremeCourt - Luxembourg Court of Cassation

## Overview

This source fetches case law decisions from the Luxembourg Court of Cassation (Cour de Cassation) via the data.public.lu Open Data portal.

## Data Source

- **Dataset**: [Cour de Cassation](https://data.public.lu/en/datasets/cour-de-cassation/)
- **Organization**: Administration judiciaire (AJUD)
- **Format**: PDF documents (pseudonymized and accessible)
- **Total Records**: ~2,346 decisions
- **Coverage**: 1976 - present
- **Update Frequency**: Weekly

## Data Access

The source uses the data.public.lu API to:
1. Fetch the list of PDF resources from the dataset
2. Download individual PDF files
3. Extract text using pdfplumber

API endpoint: `https://data.public.lu/api/1/datasets/cour-de-cassation/`

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (e.g., LU-CASS-2025-00151-32) |
| `_source` | Source ID: "LU/SupremeCourt" |
| `_type` | Document type: "case_law" |
| `_fetched_at` | ISO 8601 timestamp of fetch |
| `title` | Decision title (e.g., "Arrêt N° 32/2026 du 29.01.2026") |
| `text` | Full text of the decision (extracted from PDF) |
| `date` | Decision date in ISO format |
| `url` | URL to original PDF |
| `case_number` | Case registry number (e.g., CAS-2025-00151) |
| `decision_number` | Decision number |
| `court` | "Cour de Cassation" |
| `jurisdiction` | "Luxembourg" |
| `language` | "fr" |

## License

[CC BY-ND 4.0](https://creativecommons.org/licenses/by-nd/4.0/) (Creative Commons Attribution — No Derivatives). Commercial use is permitted, but derivative works are not allowed.

## Usage

```bash
# Fetch sample data (15 records)
python3 bootstrap.py bootstrap --sample

# Fetch specific number of records
python3 bootstrap.py fetch --limit 100

# Test connection
python3 bootstrap.py test
```

## Notes

- The Court of Cassation reviews decisions from tribunals and appellate courts
- Decisions cover both criminal (pénal) and civil matters
- All documents are pseudonymized (personal names replaced with PERSONNE1, etc.)
- Text extraction uses pdfplumber for reliable PDF parsing
