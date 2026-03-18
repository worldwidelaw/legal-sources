# AU/FederalRegister — Australian Federal Register of Legislation

Data source for Australian Commonwealth legislation from the official Federal Register of Legislation.

## Overview

- **Country**: Australia (AU)
- **Data Type**: Legislation
- **Source URL**: https://www.legislation.gov.au
- **API**: https://api.prod.legislation.gov.au/v1/
- **Authentication**: None (Open Data)
- **License**: Creative Commons (most content)

## Data Coverage

- **Acts of Parliament** from 1901 to present
- **Legislative Instruments** (regulations, rules, etc.)
- **Notifiable Instruments**
- **Administrative Arrangements Orders**
- **Constitution**
- **Continued Laws** (pre-Federation)
- **Prerogative Instruments**

Estimated total: 50,000+ legislative titles

## API Features

The Federal Register provides a free REST API compliant with OpenAPI v3.0.1:

- **OData query support**: $filter, $top, $skip, $expand, $count
- **Document formats**: Word (.docx), PDF, EPUB
- **Version tracking**: Full compilation history with start/end dates
- **No authentication**: Just send HTTP requests

### Key Endpoints

| Endpoint | Description |
|----------|-------------|
| `/v1/titles` | List all legislation titles |
| `/v1/titles/{id}` | Get single title by ID |
| `/v1/versions` | List all document versions |
| `/v1/documents/find(...)` | Download document files |

## Full Text Extraction

The scraper downloads Word documents (.docx) and extracts text from the embedded XML:

1. Download `.docx` file via `/v1/documents/find(...)`
2. Extract `word/document.xml` from the ZIP archive
3. Parse XML and extract all `<w:t>` text elements
4. Clean and normalize whitespace

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch 12 sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (all legislation)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Rate Limiting

- Conservative: 1 request/second with burst of 3
- Avoid bulk downloads during 0800-2000 AEST (UTC+10)
- Contact feedback@legislation.gov.au before large-scale crawls

## Data Schema

| Field | Description |
|-------|-------------|
| `_id` | Register ID (e.g., C2024C00838) |
| `title` | Full title of the legislation |
| `text` | **Full text** extracted from Word document |
| `date` | Version start date (ISO 8601) |
| `url` | Link to legislation.gov.au |
| `collection` | Type: Act, LegislativeInstrument, etc. |
| `status` | InForce, Repealed, Ceased, NeverEffective |
| `title_id` | Parent title ID |
| `compilation_number` | Version/compilation number |

## References

- [API Documentation](https://api.prod.legislation.gov.au/swagger/index.html)
- [Data Share and Reuse Policy](https://www.legislation.gov.au/help-and-resources/using-the-legislation-register/data-share-and-reuse)
- [About the Register](https://www.legislation.gov.au/help-and-resources/using-the-legislation-register/about-the-federal-register-of-legislation)
- [Office of Parliamentary Counsel](https://www.opc.gov.au/FRL)
