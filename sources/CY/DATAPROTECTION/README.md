# CY/DATAPROTECTION - Cyprus Data Protection Authority

## Overview

This source fetches decisions from the **Commissioner for Personal Data Protection of Cyprus** (Επίτροπος Προστασίας Δεδομένων Προσωπικού Χαρακτήρα).

## Data Source

- **Website**: https://www.dataprotection.gov.cy
- **Decisions Page**: https://www.dataprotection.gov.cy/dataprotection/dataprotection.nsf/dp06/dp06?opendocument
- **Language**: Greek (ELL), some decisions in English
- **Format**: PDF documents
- **Coverage**: GDPR enforcement decisions since 2018

## Document Types

- GDPR enforcement decisions
- Administrative fines
- Decisions on data subject complaints
- Regulatory guidance

## Data Access Method

1. Scrape the decisions index page to get decision period pages
2. For each period page, extract links to PDF decision files
3. Download PDFs and extract full text using pdfplumber

## Requirements

- Python 3.8+
- pdfplumber (for PDF text extraction)
- requests
- beautifulsoup4

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample data (15 records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Data Schema

Each record contains:

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier |
| `_source` | "CY/DATAPROTECTION" |
| `_type` | "regulatory_decision" |
| `title` | Decision title |
| `text` | Full text of the decision |
| `date` | Decision date (YYYY-MM-DD) |
| `url` | URL to the original PDF |
| `language` | "ell" (Greek) |

## Notes

- SSL verification is disabled due to certificate issues on the official site
- PDFs are primarily in Greek
- Some decisions reference English translations or include English summaries
- Decision filenames typically follow pattern: YYYYMMDD ΑΠΟΦΑΣΗ SUBJECT.pdf

## License

Open Government Data - Cyprus
