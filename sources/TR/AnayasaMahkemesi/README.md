# TR/AnayasaMahkemesi - Turkish Constitutional Court

**Country:** Turkey (TR)
**Data Type:** Case Law
**Status:** Complete

## Overview

The Turkish Constitutional Court (Anayasa Mahkemesi) is the highest legal authority for constitutional review in Turkey. This scraper fetches decisions from two official databases:

1. **Norm Review Decisions** (Norm Denetimi Kararları) - ~5,470 decisions
   - Reviews constitutionality of laws, decrees, and regulations
   - Coverage: 1961 onwards
   - URL: https://normkararlarbilgibankasi.anayasa.gov.tr

2. **Individual Applications** (Bireysel Başvuru Kararları) - ~16,464 decisions
   - Constitutional complaints from individuals
   - Coverage: 2012 onwards (when individual application was introduced)
   - URL: https://kararlarbilgibankasi.anayasa.gov.tr

## Data Access

Both databases provide:
- Paginated search results (10 decisions per page)
- Full HTML text on decision detail pages
- PDF/Word downloads available

### URL Patterns

- Norm Review: `/ND/{year}/{decision_number}` (e.g., `/ND/2025/256`)
- Individual Applications: `/BB/{year}/{application_number}` (e.g., `/BB/2023/78445`)

## Fields Captured

| Field | Description |
|-------|-------------|
| `decision_id` | Unique ID (ND/2025/256 or BB/2023/78445) |
| `database` | Source database (norm_review or individual_applications) |
| `title` | Decision title or applicant name |
| `text` | **Full text of the decision** |
| `date` | Decision date (ISO 8601) |
| `case_number` | Case reference (Esas Sayısı) |
| `decision_number` | Decision reference (Karar Sayısı) |
| `applicant` | Applicant name (individual apps only) |
| `decision_type` | Type (violation, annulment, rejection, etc.) |
| `official_gazette_date` | Publication date in Official Gazette |
| `official_gazette_number` | Official Gazette issue number |

## Usage

```bash
# Sample mode (12 records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Notes

- Language: Turkish
- Encoding: UTF-8
- Rate limit: 1 request/second
- Total estimated decisions: ~22,000
- Full text is embedded in HTML pages (not just PDF)
