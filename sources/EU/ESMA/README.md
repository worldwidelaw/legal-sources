# EU/ESMA - European Securities and Markets Authority

## Source Information

- **Name**: European Securities and Markets Authority (ESMA)
- **Country**: EU
- **URL**: https://www.esma.europa.eu
- **Data Types**: Regulatory decisions, guidelines, Q&As, opinions, technical standards

## Description

ESMA is the EU's financial markets regulator and supervisor. This source fetches regulatory documents from the ESMA Library including:

- Guidelines & Recommendations
- Final Reports
- Opinions
- Decisions
- Technical Standards (RTS/ITS)
- Q&As (Questions and Answers)
- Consultation Papers
- Public Statements

## Data Access Method

The fetcher scrapes the ESMA Library webpage and downloads PDF documents, extracting full text using pdfplumber/PyPDF2.

## Requirements

```
requests
beautifulsoup4
pdfplumber (recommended) or PyPDF2
```

## Usage

```bash
# Fetch sample documents (15 records)
python3 bootstrap.py bootstrap --sample

# Fetch more documents (50 records)
python3 bootstrap.py bootstrap

# Test mode (3 records)
python3 bootstrap.py
```

## Schema

| Field | Type | Description |
|-------|------|-------------|
| _id | string | Unique document identifier (ESMA-{reference}) |
| _source | string | "EU/ESMA" |
| _type | string | "regulatory_decision" |
| _fetched_at | string | ISO 8601 timestamp |
| document_id | string | ESMA document ID |
| reference | string | ESMA reference number |
| title | string | Document title |
| text | string | Full text content extracted from PDF |
| date | string | Publication date (YYYY-MM-DD) |
| url | string | URL to document page |
| pdf_url | string | Direct URL to PDF file |

## License

[EUR-Lex legal notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — EU agency publications, reuse authorised with attribution.

## Notes

- Rate limited to 2 seconds between requests
- PDFs are downloaded and text is extracted
- Documents without extractable text are skipped
