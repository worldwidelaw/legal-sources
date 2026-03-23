# EU/EASA - European Union Aviation Safety Agency

## Overview

This source fetches safety publications from the EASA Safety Publications Tool at ad.easa.europa.eu. These include Airworthiness Directives (ADs), Safety Information Bulletins (SIBs), Safety Directives (SDs), and related mandatory continuing airworthiness information.

## Data Source

- **Website**: https://ad.easa.europa.eu
- **Agency**: European Union Aviation Safety Agency (EASA)
- **Document Count**: ~18,000+ publications
- **Update Frequency**: Continuous (new ADs issued as needed)

## Document Types

| Type | Description |
|------|-------------|
| AD | Airworthiness Directive - mandatory corrective actions |
| SIB | Safety Information Bulletin - advisory information |
| EAD | Emergency Airworthiness Directive - urgent safety action |
| PAD | Proposed Airworthiness Directive - consultation phase |
| SD | Safety Directive - safety measures |
| PSD | Preliminary Safety Directive |

## Data Access Method

1. **HTML Scraping**: Paginated lists at `/ad-list/page-{n}`
2. **PDF Download**: Full text documents linked from each entry
3. **Text Extraction**: pdfplumber used to extract text from PDFs

## Schema

Key fields extracted:
- `document_number`: Official publication number (e.g., "2024-0182", "CASA-2026-02")
- `document_type`: AD, SIB, EAD, PAD, SD, or PSD
- `title`: Document subject/title
- `issuing_authority`: Country or authority that issued the directive
- `issue_date`: Publication date (YYYY-MM-DD)
- `effective_date`: When the directive becomes effective
- `approval_holder`: Aircraft manufacturer/TC holder
- `aircraft_type`: Aircraft type affected
- `text`: Full text content from PDF

## Usage

```bash
# Fetch 15 sample documents
python3 bootstrap.py bootstrap --sample

# Fetch 50 documents
python3 bootstrap.py bootstrap

# Quick test (3 documents)
python3 bootstrap.py
```

## Rate Limiting

- 2 second delay between PDF downloads
- 1 second delay between list page fetches
- Uses standard requests with retry logic

## Dependencies

- requests
- beautifulsoup4
- pdfplumber (or PyPDF2 as fallback)

## Notes

- Full text is extracted from PDF attachments
- Some older documents may have OCR quality issues
- Bilingual documents (often English/French) have text in both languages
