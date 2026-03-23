# GR/DPA - Greek Data Protection Authority (HDPA)

## Overview

Data source for the Hellenic Data Protection Authority (Αρχή Προστασίας Δεδομένων Προσωπικού Χαρακτήρα), Greece's national GDPR enforcement body.

## Data Coverage

- **Type**: Doctrine (regulatory decisions, opinions, guidelines)
- **Documents**: ~2,260 decisions
- **Years**: 1997 to present
- **Language**: Greek (EL)

## Document Categories

| Greek | English | Description |
|-------|---------|-------------|
| Απόφαση | Decision | Formal enforcement decisions (fines, orders) |
| Γνωμοδότηση | Opinion | Advisory opinions on data protection matters |
| Οδηγία | Guideline | Regulatory guidance documents |
| Σύσταση | Recommendation | Non-binding recommendations |

## Technical Details

### Endpoints

- **List page**: `https://www.dpa.gr/el/enimerwtiko/prakseisArxis?page=N`
- **Decision page**: `https://www.dpa.gr/el/enimerwtiko/prakseisArxis/[slug]`
- **PDF files**: `https://www.dpa.gr/sites/default/files/YYYY-MM/N_YYYY%20anonym.pdf`

### Data Extraction

1. Scrape paginated list of decisions (10 per page)
2. Visit each decision page to get metadata and PDF URL
3. Download PDF and extract text using pypdf

### Rate Limiting

- 1 request per second
- Burst: 3 requests

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample data (12 records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Sample Output

```json
{
  "_id": "HDPA/2/2026",
  "_source": "GR/DPA",
  "_type": "doctrine",
  "title": "Επιβολή προστίμου σε πάροχο υπηρεσιών τηλεπικοινωνίας",
  "date": "2026-02-11",
  "decision_number": 2,
  "year": 2026,
  "category": "Απόφαση",
  "category_en": "Decision",
  "subject_area": "10. Υπηρεσίες ηλεκτρονικής επικοινωνίας",
  "provisions": ["Article 12", "Article 15", "Article 18"],
  "text": "Full decision text extracted from PDF...",
  "language": "el"
}
```

## Dependencies

- `pypdf` - PDF text extraction
- `requests` - HTTP client
- `beautifulsoup4` (optional) - HTML parsing fallback

## Notes

- PDFs are anonymized versions (anonym suffix in filename)
- Some older decisions may have poor OCR quality
- Decision dates are in Greek format (DD/MM/YYYY)
