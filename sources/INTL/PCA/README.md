# INTL/PCA — Permanent Court of Arbitration Case Repository

## Overview
Fetches international arbitration awards and decisions from the Permanent Court of Arbitration (PCA) at The Hague.

## Data Access
- **Case listing**: JSON API at `pcacases.com/web/api/cases/search`
- **Case details**: HTML scraping of `pca-cpa.org/en/cases/{id}/`
- **Documents**: PDF downloads from `pcacases.com/web/sendAttach/{doc_id}`
- **Text extraction**: PyMuPDF (fitz)

## Coverage
- ~288 cases (inter-state, investor-state, contract-based arbitrations)
- Awards, decisions, and procedural orders as PDFs
- Full text extracted from award/decision PDFs only

## Usage
```bash
python bootstrap.py test                # Connectivity test
python bootstrap.py bootstrap --sample  # Fetch 15 sample records
python bootstrap.py bootstrap           # Full fetch
python bootstrap.py update              # Incremental update
```

## License

[PCA Terms](https://pca-cpa.org/en/terms-of-use/) — published awards are publicly available. Verify PCA terms before commercial redistribution.

## Dependencies
- requests, beautifulsoup4, PyMuPDF (fitz)
