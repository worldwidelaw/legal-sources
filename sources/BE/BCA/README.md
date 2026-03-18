# BE/BCA - Belgian Competition Authority

## Overview

This source fetches competition/antitrust decisions from the Belgian Competition Authority (Autorité belge de la concurrence / Belgische Mededingingsautoriteit).

**Website:** https://www.belgiancompetition.be
**Decisions page:** https://www.belgiancompetition.be/en/decisions

## Data Types

- **regulatory_decisions**: Competition authority decisions including:
  - Merger/concentration approvals (CC, CCS)
  - Restrictive practices rulings (RPR)
  - Antitrust decisions

## Coverage

- Decisions from 1993 to present
- Approximately 1,500+ decisions
- Available in the procedural language (Dutch, French, or German)

## Case Number Format

Decisions follow a standardized format: `YY-TYPE-NN`

- **YY**: Year (2-digit)
- **TYPE**: Case classification
  - `CC`: Concentration (merger)
  - `CCS`: Concentration simplified
  - `RPR`: Restrictive practices
  - `C`: Concentration (older format)
- **NN**: Sequential number

Examples: `26-CC-01`, `25-RPR-40`, `24-CCS-05-AUD`

## Usage

```bash
# Fetch sample records (15 decisions)
python bootstrap.py bootstrap --sample

# Fetch custom sample size
python bootstrap.py bootstrap --sample --count 30

# Full fetch (all decisions)
python bootstrap.py bootstrap --full
```

## Technical Details

- **Access method**: HTML scraping + PDF download
- **Rate limiting**: 2 second delay between requests
- **PDF text extraction**: Uses pypdf library
- **Pagination**: ~155 pages of decisions

## Dependencies

- requests
- beautifulsoup4
- pypdf

## License

Belgian Federal Government Open Data
