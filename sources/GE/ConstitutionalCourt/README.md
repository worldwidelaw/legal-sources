# GE/ConstitutionalCourt — Georgian Constitutional Court

## Overview

Data source for decisions from the Constitutional Court of Georgia (საქართველოს საკონსტიტუციო სასამართლო).

- **URL**: https://www.constcourt.ge
- **Language**: Georgian (ka), some documents available in English
- **Data types**: Constitutional complaints, rulings, judgments, dissenting opinions, concurring opinions, amicus curiae briefs
- **License**: Public

## Coverage

- Constitutional complaints (კონსტიტუციური სარჩელი)
- Constitutional submissions (კონსტიტუციური წარდგინება)
- Rulings (განჩინება)
- Recording notices (შეტყობინება)
- Judgments (გადაწყვეტილება)
- Dissenting opinions (განსხვავებული აზრი)
- Concurring opinions (თანმხვედრი აზრი)
- Amicus curiae briefs

Estimated total: 3000+ documents across 270+ pages.

## Data Access

The Constitutional Court website provides:
- HTML pages with full text of decisions
- Pagination with configurable items per page (10, 20, 50, 100)
- Search and filter functionality
- Sitemap.xml with decision URLs
- DOCX downloads for some documents

## Usage

```bash
# Fetch sample data (12 decisions)
python3 bootstrap.py bootstrap --sample

# Fetch all decisions
python3 bootstrap.py fetch --limit 100

# Fetch updates since a date
python3 bootstrap.py updates --since 2026-01-01
```

## Schema

Key fields:
- `legal_id`: Internal database ID
- `case_number`: Court case number (e.g., N1935, N2/1/1494)
- `document_type`: Type of document
- `chamber`: Court chamber (I კოლეგია, II კოლეგია, პლენუმი)
- `author`: Complainant/petitioner
- `date`: Decision date (ISO 8601)
- `publication_date`: Publication date on website

## Notes

- Full text is in Georgian for most decisions
- Some landmark decisions are translated to English
- Rate limiting: 2 seconds between requests
- The court has been active since 1996
