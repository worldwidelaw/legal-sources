# BE/BIPT - Belgian Institute for Postal Services and Telecommunications

## Overview

This source fetches regulatory decisions from BIPT (Institut belge des services postaux et des télécommunications / Belgisch Instituut voor postdiensten en telecommunicatie), the Belgian telecom and postal regulator.

## Data Source

- **Website**: https://www.bipt.be
- **RSS Feed**: https://www.bipt.be/operators/search.xml?s=publication_date&tgGroup=operators&type[0]=publication_type:decision
- **Total Records**: ~632 decisions (as of March 2026)

## Access Method

1. XML RSS feed provides metadata for all decisions (title, date, description, URL)
2. Individual decision pages contain PDF download links
3. PDFs are downloaded and text is extracted using pdfplumber/pypdf

## Document Languages

- French (primary)
- Dutch (most decisions available in both languages)

## Data Types

- Regulatory decisions (spectrum allocation, market analysis, penalties, etc.)
- Areas: telecommunications, postal services, consumer protection, digital services

## Usage

```bash
# Fetch sample of 15 records
python bootstrap.py bootstrap --sample

# Full fetch of all decisions
python bootstrap.py bootstrap --full

# Custom sample size
python bootstrap.py bootstrap --sample --count 25
```

## Schema

Key fields in normalized records:
- `_id`: Unique identifier (e.g., "BE/BIPT/decision-of-5-march-2026...")
- `title`: Decision title
- `text`: Full text extracted from PDF
- `date`: Publication date (YYYY-MM-DD)
- `url`: Link to decision page on BIPT website
- `description`: Brief summary from RSS feed
- `themes`: Topics/categories
- `language`: Document language (fr/nl)
- `pdf_url`: Direct link to PDF

## License

Open Data - Belgian Federal Government
