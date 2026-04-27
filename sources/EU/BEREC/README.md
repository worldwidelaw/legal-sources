# EU/BEREC - Body of European Regulators for Electronic Communications

## Overview

BEREC (Body of European Regulators for Electronic Communications) is the EU body
that brings together national regulatory authorities (NRAs) from all EU member states.
It publishes guidelines, opinions, reports, and decisions on electronic communications.

## Data Source

- **Website**: https://www.berec.europa.eu
- **Document Register**: https://www.berec.europa.eu/en/search-documents
- **Total Documents**: ~8,500+ (as of March 2026)

## Document Types

- Guidelines & Recommendations
- Opinions
- Reports
- Decisions
- Regulatory Best Practices
- Public Consultations
- Board of Regulators Meeting materials

## Implementation

### Approach

1. Scrape the document search page (paginated)
2. For each document, visit the detail page to find PDF links
3. Download PDFs and extract text using pdfplumber/PyPDF2
4. Normalize to standard schema

### Dependencies

- requests
- beautifulsoup4
- pdfplumber (or PyPDF2 as fallback)

### Usage

```bash
# Test fetch (3 documents)
python3 bootstrap.py

# Sample fetch (15 documents)
python3 bootstrap.py bootstrap --sample

# Full fetch (50 documents)
python3 bootstrap.py bootstrap
```

## Schema

| Field | Description |
|-------|-------------|
| _id | BEREC-{document_number} |
| _source | EU/BEREC |
| _type | doctrine |
| document_number | BEREC registration number (e.g., "BoR (26) 28") |
| title | Document title |
| text | Full text extracted from PDF |
| date | Publication date (ISO 8601) |
| url | Link to document page |
| pdf_url | Direct PDF download link |
| author | Document author (usually "BEREC" or "BEREC Office") |

## Rate Limiting

- 2 seconds between requests
- Maximum 3 retries per request
- 120 second timeout

## License

[EUR-Lex legal notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — EU agency publications, reuse authorised with attribution.

## Notes

- PDFs are stored under `/system/files/` path
- Document search supports filtering by date range, subject matter, and tags
- Documents go back to 2012 (when electronic register started)
