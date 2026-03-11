# AM/CourtOfCassation - Armenian Court of Cassation

## Overview

Data source for Armenian Court of Cassation (Vchrabek) decisions.

- **Website**: https://cassationcourt.am
- **Data types**: Case law (civil, criminal, administrative, anti-corruption)
- **Format**: PDF documents with text extraction
- **Language**: Armenian (hy)
- **License**: Public Domain (government decisions)
- **Coverage**: ~1,300+ decisions

## Technical Details

### API Endpoint

The Court of Cassation provides an undocumented JSON API for fetching decision details:

```
GET /api/precedent-single-decision/{chamber}/{id}
```

Chamber types:
- `civil-cases`
- `criminal-cases`
- `administrative-cases`
- `administrative-cases-intermediate`
- `corruption-civil-cases`
- `corruption-crimes-cases`

### Data Flow

1. Paginate through `/en/decisions/?page=N` to get decision IDs
2. For each decision, call the API to get HTML content
3. Extract PDF URL from HTML response
4. Download PDF and extract text using pdfplumber

## Usage

```bash
# Fetch sample records for validation
python3 bootstrap.py bootstrap --sample

# Fetch all records
python3 bootstrap.py bootstrap

# Fetch updates since a date
python3 bootstrap.py updates --since 2026-01-01
```

## Sample Data

The `sample/` directory contains 12+ sample records demonstrating:
- Full text extraction from PDFs
- Metadata from decision list and detail pages
- Various case types (civil, criminal, administrative)
