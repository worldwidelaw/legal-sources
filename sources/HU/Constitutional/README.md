# HU/Constitutional - Hungarian Constitutional Court (Alkotmánybíróság)

## Overview

This source provides case law from the Hungarian Constitutional Court (Alkotmánybíróság).
Decisions are fetched from the official website and full text is extracted from PDF documents.

**Website:** https://alkotmanybirosag.hu  
**English site:** https://www.hunconcourt.hu  
**License:** Public domain

## Data Access

The Constitutional Court website uses Next.js for server-side rendering. Decisions are
available as structured data embedded in the HTML pages with PDFs containing the full text.

### Endpoints

- **Recent Decisions:** `https://alkotmanybirosag.hu/a-legfrissebb-dontesek/`
- **Media Server:** `https://media.alkotmanybirosag.hu/{year}/{month}/sz_{section}_{number}_{year}.pdf`
- **Gazette Archive:** `https://alkotmanybirosag.hu/az-alkotmanybirosag-hatarozatai-ab-kozlony/`

### Data Structure

Decision metadata is extracted from Next.js `__NEXT_DATA__` with these fields:
- `acf.decision_number` - Case number (e.g., "IV/1867/2025")
- `acf.lead` - Summary/lead paragraph (HTML)
- `acf.attachments_pdf` - Full text PDF URL
- `acf.link_attachments_view` - Link to official document database

Full text is extracted from PDF documents using pdfplumber.

## Coverage

- **Decision types:** Határozatok (decisions), Végzések (orders)
- **Time range:** Recent decisions (approximately last 50)
- **Historical data:** Available via official gazette PDFs (2011-present)

For comprehensive historical data (1990-2021), see the HUNCOURT academic database:
https://osf.io/6aek9/

## Usage

```bash
# Fetch sample records with full text
python3 bootstrap.py bootstrap --sample --count 12

# Fetch all available decisions
python3 bootstrap.py fetch

# Fetch updates since a date
python3 bootstrap.py updates --since 2026-01-01
```

## Dependencies

- requests
- pdfplumber (for PDF text extraction)

Install with:
```bash
pip install requests pdfplumber
```

## License

Public domain — official decisions of the Hungarian Constitutional Court.

## Notes

- Rate limiting: 1.5 second delay between PDF downloads
- PDF extraction may take time for large documents
- The website provides decisions in Hungarian only
- English translations available on hunconcourt.hu but with less structured data
