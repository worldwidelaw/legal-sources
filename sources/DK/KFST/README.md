# DK/KFST - Danish Competition and Consumer Authority

Fetches competition law decisions from **Konkurrence- og Forbrugerstyrelsen** (KFST), the Danish Competition and Consumer Authority.

## Data Source

- **Website**: https://kfst.dk
- **Decisions**: https://kfst.dk/konkurrenceforhold/afgoerelser
- **Data Types**: Competition decisions, merger control rulings
- **Coverage**: 2024-present (initial)
- **Language**: Danish (da)
- **License**: Public domain (Danish government publication)

## Decision Types

The Competition Council (Konkurrencerådet) and the Competition and Consumer Authority decide approximately 50 cases annually, including:

- **Merger Control** (Fusionskontrol) - Approval of mergers and acquisitions
- **Cartel Prohibition** (Kartellverbot) - Anti-competitive agreements
- **Abuse of Dominance** (Misbrug) - Market dominance violations

## Technical Details

### Discovery
- Parses XML sitemap at `/sitemap`
- Filters URLs matching `/raads-og-styrelsesafgoerelser/YYYY/`

### Full Text Extraction
- Decision pages contain metadata and link to PDF
- PDF URL pattern: `/media/{hash}/{filename}.pdf`
- Text extracted using pypdf library

### Rate Limiting
- 1 request/second
- Respects robots.txt crawl-delay

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

## Requirements

- pypdf (for PDF text extraction)

```bash
pip install pypdf
```

## Output Schema

```json
{
  "_id": "DK-KFST-24-06957",
  "_source": "DK/KFST",
  "_type": "regulatory_decision",
  "title": "Vedtagelse om markedsdeling i Botex",
  "text": "Full decision text...",
  "date": "2025-03-26",
  "url": "https://kfst.dk/...",
  "pdf_url": "https://kfst.dk/media/.../decision.pdf",
  "case_number": "24/06957",
  "authority": "Konkurrence- og Forbrugerstyrelsen",
  "country": "DK",
  "language": "da"
}
```

## License

Public domain — Danish government decisions are not subject to copyright under [Danish Copyright Act §9](https://www.retsinformation.dk/eli/lta/2023/164).
