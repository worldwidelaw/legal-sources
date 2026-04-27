# AT/VfGH - Austrian Constitutional Court (Verfassungsgerichtshof)

## Overview

This source fetches case law from the Austrian Constitutional Court (VfGH) via the RIS OGD API v2.6.

## Data Source

- **Name**: Verfassungsgerichtshof (VfGH)
- **URL**: https://www.vfgh.gv.at
- **API**: https://data.bka.gv.at/ris/api/v2.6/Judikatur
- **Records**: ~24,000 decisions
- **Type**: Case law only
- **Auth**: None required
- **License**: CC BY 4.0

## Content

VfGH decisions include:
- Constitutional review cases
- Individual rights complaints (Beschwerden)
- Election disputes
- Conflicts between authorities

Each record contains:
- Full decision text (from XML/HTML content URLs)
- ECLI (European Case Law Identifier)
- Case number (Geschaeftszahl)
- Decision date
- Legal norms cited
- Keywords and indices
- Summary (Leitsatz)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (12 records)
python bootstrap.py bootstrap --sample

# Full bootstrap (24K+ records)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Technical Notes

- Uses RIS OGD API v2.6 with application "Vfgh"
- Full text extracted from XML content URLs (preferred) or HTML fallback
- For Rechtssatz (legal principle) records, links to corresponding decision text documents
- Rate limited to 2 seconds between requests

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Austrian Open Government Data.
