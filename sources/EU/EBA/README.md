# EU/EBA - European Banking Authority

## Overview
The European Banking Authority (EBA) is an independent EU authority that works to ensure
effective and consistent prudential regulation and supervision across the European banking sector.

## Data Types
- **Guidelines** - Detailed guidance on regulatory implementation
- **Opinions** - EBA's views on regulatory matters
- **Decisions** - Formal decisions and administrative acts
- **Reports** - Analysis and assessment reports
- **Recommendations** - Non-binding guidance
- **Draft RTS** - Draft Regulatory Technical Standards
- **Draft ITS** - Draft Implementing Technical Standards

## Approach
This fetcher:
1. Scrapes the EBA publications listing pages for each document type
2. Extracts PDF links and basic metadata from the HTML
3. Downloads PDFs and extracts full text using pdfplumber
4. Normalizes data to our standard schema

## Dependencies
- `requests` - HTTP client
- `beautifulsoup4` - HTML parsing
- `pdfplumber` - PDF text extraction (preferred)
- `PyPDF2` - Fallback PDF extraction

## Usage
```bash
# Quick test (3 documents)
python3 bootstrap.py

# Sample mode (15 documents)
python3 bootstrap.py bootstrap --sample

# Standard bootstrap (50 documents)
python3 bootstrap.py bootstrap
```

## Data Schema
Each normalized document contains:
- `_id` - Unique document identifier
- `_source` - Source identifier ("EU/EBA")
- `_type` - Document category ("regulatory_decision")
- `title` - Document title
- `text` - Full text content extracted from PDF
- `date` - Publication date (ISO 8601)
- `url` - Original PDF URL
- `document_type` - Specific type (guideline, opinion, decision, etc.)

## License

[EUR-Lex legal notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — EU agency publications, reuse authorised with attribution.

## Notes
- EBA publishes primarily in PDF format
- Rate limiting is set to 2 seconds between requests
- Some older documents may have OCR quality issues
