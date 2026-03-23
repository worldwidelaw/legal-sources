# GR/CourtOfAudit - Hellenic Court of Audit (Ελεγκτικό Συνέδριο)

Greece's supreme audit institution responsible for auditing state finances, public procurement, and pension matters.

## Data Source

- **URL**: https://www.elsyn.gr
- **Data Type**: Case Law
- **Language**: Greek
- **Coverage**: 2016 - present

## Document Types

| Greek | English | Description |
|-------|---------|-------------|
| Απόφαση | Decision | Court decisions on specific cases |
| Πράξη | Act | Administrative acts by court chambers (Κλιμάκιο) |
| Γνωμοδότηση | Opinion | Advisory opinions on legal matters |

## Data Access

The Court of Audit publishes decisions on a Drupal 10 website with paginated listings. Full text is embedded directly in the HTML - no PDF extraction needed.

### Technical Notes

- Uses HTTP (SSL certificate issues with HTTPS)
- Full text available in `<div class="legal-page-hidden">` elements
- PDFs also available for download
- Pagination: ~47 pages, ~10 decisions per page

## Usage

```bash
# Test connection
python bootstrap.py test

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Schema

| Field | Type | Description |
|-------|------|-------------|
| _id | string | Unique identifier (e.g., "ELSYN-Απόφαση-62-2026") |
| title | string | Formatted title with type, number, year, chamber |
| text | string | Full text of the decision |
| date | string | ISO date (approximated from year) |
| year | string | Year of decision |
| decision_type | string | Greek type (Απόφαση, Πράξη, Γνωμοδότηση) |
| decision_type_en | string | English type (decision, act, opinion) |
| number | string | Decision number |
| chamber | string | Chamber/division name |
| pdf_urls | array | URLs to PDF downloads |

## Topics Covered

- Public contract pre-audit control
- State procurement disputes
- Public financial management
- Pension and retirement fund matters
- EU co-financed project audits
