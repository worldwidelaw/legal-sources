# IT/AGCM - Italian Competition Authority

**Source**: Autorità Garante della Concorrenza e del Mercato (AGCM)
**URL**: https://www.agcm.it
**Country**: Italy
**Data Types**: Competition decisions, Consumer protection decisions, Merger decisions

## Overview

The Italian Competition Authority (AGCM) publishes all decisions adopted since its establishment in 1990. This scraper fetches decisions from both the competition law database and the consumer protection database.

## Data Coverage

- **Competition Cases**: Cartels (Art. 101 TFUE), Abuse of Dominance (Art. 102 TFUE), Mergers
- **Consumer Protection**: Unfair commercial practices, Misleading advertising
- **Time Range**: 1990 - present
- **Update Frequency**: Weekly (decisions published in the Bollettino Settimanale)

## Technical Details

### API Endpoints

1. **List Decisions**:
   ```
   /dotcmsCustom/getDomino?urlStr=10.200.70.10:8080/{db_id}/{agent}?openagent&view=vw0601&anno={year}&start={offset}&maxresults={count}
   ```

2. **Decision Detail**:
   ```
   /dotcmsCustom/getDominoDetail?p={uid}&urlStr=10.200.70.10:8080/{db_id}/ag0102dot?openagent
   ```

3. **PDF Attachments**:
   ```
   /dotcmsCustom/getDominoAttach?urlStr=192.168.14.10:8080/{db_id}/0/{uid}/$File/{filename}.pdf
   ```

### Database IDs

- **Competition (dbconc)**: `41256297003874BD`
- **Consumer Protection (dbcons)**: `C12560D000291394`

## Full Text Extraction

Full text is extracted from PDF attachments using `pdfplumber`. Each decision typically has:
- `p{number}.pdf`: The main decision document
- `p{number}_all.pdf`: Complete version with annexes

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample --count 15

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Requirements

- Python 3.8+
- pdfplumber (for PDF text extraction)
- beautifulsoup4 (for HTML parsing)
- requests

## License

Italian public data - decisions are published as open data by AGCM.

## Notes

- Rate limiting: 1 request per second to avoid overloading the server
- PDF text extraction requires `pdfplumber` to be installed
- Some older decisions may have poor PDF quality affecting text extraction
