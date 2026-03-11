# CZ/eSbirka - Czech Collection of Laws (e-Sbírka)

Official open data source for Czech legislation from the Ministry of Interior.

## Overview

- **Source**: [e-Sbírka](https://www.e-sbirka.cz) / [zakony.gov.cz](https://zakony.gov.cz)
- **Data Type**: Legislation
- **Language**: Czech (cs)
- **License**: CC BY 4.0
- **Authentication**: None required
- **Update Frequency**: Daily

## Data Access

This fetcher uses bulk JSON downloads from the official open data endpoint:

```
https://opendata.eselpoint.cz/datove-sady-esbirka/
```

### Available Datasets

| File | Size | Description |
|------|------|-------------|
| 002PravniAkt.json.gz | ~5MB | Legal act metadata |
| 004PravniAktFragment.json.gz | ~503MB | Text fragments with content |
| 003PravniAktZneniFragment.json.gz | ~1.1GB | Version-fragment links |

## How It Works

1. Downloads the acts metadata and text fragments files
2. Matches fragments to acts using ELI (European Legislation Identifier) patterns
3. Reassembles fragments into complete document text
4. Normalizes to standard schema with full text

## Usage

```bash
# Test mode - fetch and display a few documents
python3 bootstrap.py

# Bootstrap mode - save 10 sample documents
python3 bootstrap.py bootstrap --sample

# Full bootstrap - save 100 sample documents
python3 bootstrap.py bootstrap
```

## Data Structure

Each normalized document contains:

- `_id`: Act code (e.g., "SB-1918-00008")
- `_source`: "CZ/eSbirka"
- `_type`: "legislation"
- `title`: Act title in Czech
- `citation`: Citation format (e.g., "8/1918 Sb.")
- `text`: Full text of the legislation
- `year`: Year of publication
- `number`: Act number
- `date`: Approximate date (January 1st of the year)
- `url`: Link to e-Sbírka portal

## Notes

- The e-Sbírka system launched in January 2024
- Contains 43,000+ legal acts from 1918 to present
- Full text is stored in fragments that need reassembly
- Data is available in Czech language only
- Some SSL certificate issues may require verification disabled

## References

- [Open Data Documentation](https://zakony.gov.cz/gov/otevrena-data-a-verejna-api-systemu-e-sbirka-od-15-ledna/)
- [e-Sbírka Portal](https://www.e-sbirka.cz)
- [Czech National Open Data Catalog](https://data.gov.cz/english/)
