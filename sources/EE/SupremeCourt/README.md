# EE/SupremeCourt - Estonian Supreme Court (Riigikohus)

## Overview

This source fetches case law decisions from the Estonian Supreme Court (Riigikohus).

- **Website:** https://www.riigikohus.ee
- **Data type:** Case law
- **Access method:** RSS feeds + HTML scraping
- **Authentication:** None required (public access)
- **License:** Public Domain (Estonian State)

## Coverage

The Supreme Court publishes decisions from four chambers:

1. **Administrative Chamber** (Halduskolleegium) - case numbers starting with `3-`
2. **Civil Chamber** (Tsiviilkolleegium) - case numbers starting with `2-`
3. **Criminal Chamber** (Kriminaalkolleegium) - case numbers starting with `1-`
4. **Constitutional Review Chamber** (Pohiseaduslikkuse jarelevalve kolleegium) - case numbers starting with `5-`

Misdemeanor cases (`4-XX-XXXX`) are also included.

## Data Access

### Discovery
- RSS feed at `https://www.riigikohus.ee/lahendid/rss.xml`
- Provides case numbers, descriptions, and publication dates
- Separate feeds available for each chamber type

### Full Text
- Retrieved from `https://rikos.rik.ee/?asjaNr={case_number}`
- HTML format with structured metadata tables
- Full judgment text in Estonian

## ECLI Support

Estonia has implemented ECLI since 2016. The format is:
```
ECLI:EE:RK:YYYY:case_number_with_dots
```

Example: `ECLI:EE:RK:2024:5.24.28.1` for case `5-24-28/1`

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records (12 by default)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Output Schema

Each record includes:

- `_id`: Case number (e.g., "2-24-1408/38")
- `_source`: "EE/SupremeCourt"
- `_type`: "case_law"
- `title`: Case subject/description
- `text`: Full judgment text
- `date`: Decision date (YYYY-MM-DD)
- `url`: Link to judgment on riigikohus.ee
- `case_number`: Original case number
- `chamber`: Court composition
- `chamber_type`: Chamber type (administrative/civil/criminal/constitutional)
- `ecli`: European Case Law Identifier

## References

- Riigikohus website: https://www.riigikohus.ee
- Riigi Teataja (State Gazette): https://www.riigiteataja.ee
- Court Information System: https://www.rik.ee/en/international/court-information-system
