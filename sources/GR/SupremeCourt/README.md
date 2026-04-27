# GR/SupremeCourt -- Greek Supreme Court (Areios Pagos)

Fetches case law from the Greek Supreme Court (Άρειος Πάγος).

## Data Source

- **URL**: https://www.areiospagos.gr
- **Type**: Case law
- **Coverage**: 2006 to present
- **Language**: Greek
- **Auth**: None (public access)

## Endpoint Strategy

No official API exists. This scraper uses the public search interface:

1. **Search by Year**: POST to `/nomologia/apofaseis_result.asp?S=1`
   - Returns list of all decisions for a given year
   - Includes decision number, code (cd), and chamber

2. **Fetch Decision**: GET `/nomologia/apofaseis_DISPLAY.asp?cd={cd}&apof={num}_{year}`
   - Returns full HTML page with decision text
   - Encoded in windows-1253 (Greek Windows codepage)

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12+ documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all years)
python bootstrap.py bootstrap

# Update (recent years only)
python bootstrap.py update
```

## Output Schema

```json
{
  "_id": "AP/740_2024",
  "_source": "GR/SupremeCourt",
  "_type": "case_law",
  "_fetched_at": "2026-02-12T...",
  "title": "ΑΡΕΙΟΣ ΠΑΓΟΣ - ΑΠΟΦΑΣΗ 740/2024 (Α2, ΠΟΛΙΤΙΚΕΣ)",
  "text": "Αριθμός 740/2024 ΤΟ ΔΙΚΑΣΤΗΡΙΟ ΤΟΥ ΑΡΕΙΟΥ ΠΑΓΟΥ...",
  "date": "2023-01-30",
  "url": "https://www.areiospagos.gr/nomologia/...",
  "number": 740,
  "year": 2024,
  "chamber": "Α2' Πολιτικό Τμήμα",
  "language": "el",
  "court": "Άρειος Πάγος",
  "court_en": "Supreme Court of Greece"
}
```

## License

Public domain — official court decisions of the Hellenic Republic.

## Notes

- The website uses windows-1253 encoding (Greek Windows codepage)
- Rate limiting: 1-2 requests per second recommended
- Decisions are organized by chamber (civil/criminal) and division (Α, Β, Γ, etc.)
- Full text includes complete judgment reasoning
