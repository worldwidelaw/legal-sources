# DE/BfArM - German Federal Institute for Drugs and Medical Devices

## Overview

This source fetches **Rote-Hand-Briefe** (Red Hand Letters) from BfArM, which are urgent safety communications about pharmaceuticals distributed to healthcare professionals in Germany.

## Data Source

- **Authority**: Bundesinstitut für Arzneimittel und Medizinprodukte (BfArM)
- **Website**: https://www.bfarm.de/
- **Data Type**: Regulatory decisions / safety communications
- **Language**: German

## What are Rote-Hand-Briefe?

Rote-Hand-Briefe are urgent safety letters that pharmaceutical companies are required to send to healthcare professionals when newly identified drug-associated risks are discovered. They include:

- Safety warnings about medications
- Quality defects in pharmaceutical products
- Updated usage instructions
- Withdrawal notifications
- Changes to storage conditions

## Data Volume

- **Archive**: ~540+ documents from 2007 to present
- **Frequency**: 2-10 new documents per month
- **Document size**: Typically 3-10 pages each

## Access Methods

1. **RSS Feed**: https://www.bfarm.de/SiteGlobals/Functions/RSSFeed/DE/Pharmakovigilanz/Rote-Hand-Briefe/RSSNewsfeed.xml
   - Contains ~20 most recent entries

2. **HTML Pagination**: 54+ pages of historical entries
   - URL pattern: `?gtp=964792_list%253D{page_number}`

3. **PDF Documents**: Full text available as downloadable PDFs
   - Text extraction via pdfminer

## Usage

```bash
# Test the fetcher (3 documents)
python3 bootstrap.py

# Bootstrap sample data (15 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (100 documents)
python3 bootstrap.py bootstrap
```

## Dependencies

- requests
- pdfminer.six (for PDF text extraction)

## License

Public domain - Official German government regulatory communications under § 5 UrhG.
