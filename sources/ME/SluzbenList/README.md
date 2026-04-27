# ME/SluzbenList - Montenegro Official Gazette

**Source:** Službeni list Crne Gore (Official Gazette of Montenegro)  
**URL:** https://www.sluzbenilist.me  
**Type:** Legislation  
**Authentication:** None (open access)

## Overview

The Official Gazette of Montenegro publishes all laws, decrees, regulations, and
other official government documents. This source provides full-text access to
Montenegrin legislation from 1991 to present.

## Data Access Method

Documents are accessed via PDF download:
1. Registry pages (`/registri?type=0&year=YYYY`) list gazette issues
2. Each issue page (`/registri/{uuid}`) lists individual documents
3. Documents can be downloaded as PDF (`/propisi/{uuid}/download`)
4. Text is extracted from PDFs using pdfplumber

## Document Types

- ZAKON (Laws)
- ODLUKA (Decisions)
- UREDBA (Decrees/Regulations)
- PRAVILNIK (Rulebooks)
- NAREDBA (Orders)
- UKAZ (Decrees - Presidential)
- RJEŠENJE (Decisions/Rulings)

## Usage

```bash
# Fetch sample documents
python3 bootstrap.py bootstrap --sample

# List recent gazette issues
python3 bootstrap.py list --year 2025
```

## Requirements

- Python 3.8+
- requests
- beautifulsoup4
- pdfplumber (for PDF text extraction)

## License

Open government data — official gazette freely accessible.

## Notes

- Documents are in Montenegrin (Latin script)
- Rate limiting: 2 second delay between requests
- PDF quality varies; some older scanned documents may have OCR issues
