# SE/SupremeAdministrativeCourt

Swedish Supreme Administrative Court (Högsta förvaltningsdomstolen - HFD) case law.

## Overview

The Supreme Administrative Court is Sweden's highest court for administrative law cases.
It handles appeals from administrative courts (kammarrätt) and issues precedential decisions
on matters including tax, social insurance, immigration, and public administration.

## Data Source

- **RSS Feed**: `https://www.domstol.se/feed/1092/?searchPageId=1092&scope=decision`
- **Decision Pages**: `https://www.domstol.se/hogsta-forvaltningsdomstolen/avgoranden2/{year}/{id}/`
- **PDF Documents**: `https://www.domstol.se/globalassets/filer/domstol/hogstaforvaltningsdomstolen/{year}/domar-och-beslut/{case}.pdf`

## Coverage

- **Years**: 2008-present
- **Decision Types**: Dom (judgment), Beslut (decision), Referat (report), Notis (notice)
- **Legal Areas**: Tax (Skatt), Social insurance (Socialförsäkring), Immigration, Administrative law, etc.

## Data Structure

Each record contains:
- `_id`: Unique identifier (e.g., "HFD-5734-24")
- `case_number`: Case reference number (e.g., "5734-24")
- `title`: Case title
- `text`: Full judgment text extracted from PDF
- `date`: Decision date (ISO 8601)
- `court`: "Högsta förvaltningsdomstolen"
- `court_code`: "HFD"
- `decision_type`: Type of decision
- `legal_areas`: List of applicable legal areas
- `summary`: Brief summary of the case
- `url`: Link to decision page
- `pdf_url`: Direct link to PDF document

## Usage

```bash
# Test fetcher (2 documents)
python3 bootstrap.py

# Bootstrap sample data (12 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (100 documents)
python3 bootstrap.py bootstrap
```

## Technical Notes

- PDF text extraction uses PyPDF2/pypdf library
- Rate limited to 1 request per 2 seconds
- RSS feed provides most recent decisions first
- Some older PDFs may have OCR quality issues

## License

[Public Domain](https://www.domstol.se/om-webbplatsen-och-digitala-kanaler/oppna-data/) — Swedish court decisions are public records not subject to copyright (Upphovsrattslag 1960:729, Section 9).
