# CoE/HUDOC - European Court of Human Rights Case Law

## Overview

HUDOC is the official case-law database of the European Court of Human Rights (ECHR), part of the Council of Europe. It contains judgments, decisions, and other documents from the Court since its establishment.

## Data Coverage

- **Total documents:** 227,000+ judgments and decisions
- **Date range:** 1960 to present (earliest: Lawless v. Ireland, 14 Nov 1960)
- **Languages:** English, French, and 15+ other language translations
- **Document types:** Judgments, decisions, advisory opinions, commission reports

### Document Type Counts (approximate)
- Judgments (HEJUD/HFJUD + translations): ~58,000
- Decisions (HEDEC/HFDEC): ~170,000

## API Access

The source uses HUDOC's internal JSON API:

### Search Endpoint
```
GET https://hudoc.echr.coe.int/app/query/results
Parameters:
  - query: Search query (e.g., "contentsitename:ECHR AND doctype=HEJUD")
  - select: Fields to return
  - sort: Sort order (e.g., "kpdate Ascending" for historical, "kpdate Descending" for recent)
  - start: Pagination offset
  - length: Page size
```

### Full Text Endpoint
```
GET https://hudoc.echr.coe.int/app/conversion/docx/html/body
Parameters:
  - library: ECHR
  - id: Document item ID (e.g., "001-248399")
```

## Document Types

| Code | Description |
|------|-------------|
| HEJUD | Judgments (English) |
| HFJUD | Judgments (French) |
| HJUD* | Judgments (other languages: GER, RUS, SPA, TUR, etc.) |
| HEDEC | Decisions (English) |
| HFDEC | Decisions (French) |
| HECOMOLD | Commission reports (English) |
| HFCOMOLD | Commission reports (French) |

## Historical Data

The HUDOC API provides full historical coverage from November 1960 to present.
The bootstrap script fetches a mix of oldest and newest records by default to verify
this coverage. Use `--chronological` flag to fetch oldest records first for
systematic historical bootstrap.

## Sample Data

16 sample records with average text length of 42,375 characters per document.
Includes records from 1960 (Lawless v. Ireland) through 2026.

## License

Open access. ECHR case law is in the public domain.

## Usage

```bash
# Fetch sample records (mix of historical and recent)
python bootstrap.py bootstrap --sample --count 16

# Validate samples
python bootstrap.py validate

# Fetch updates since date
python bootstrap.py fetch --since 2026-01-01 --count 100

# Fetch oldest records first (for historical bootstrap)
python bootstrap.py fetch --chronological --count 1000
```
