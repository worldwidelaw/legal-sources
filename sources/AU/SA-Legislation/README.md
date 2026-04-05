# AU/SA-Legislation — South Australia Legislation

Fetches South Australia Acts and Regulations from the data.sa.gov.au CKAN portal.

## Data Source

- **Publisher**: Office of Parliamentary Counsel, South Australia
- **Portal**: https://data.sa.gov.au/data/dataset/database-update-package-xml
- **Format**: XML (SAOPC Exchange DTD) in ZIP packages
- **License**: CC BY 4.0
- **Update frequency**: Fortnightly
- **Auth**: None required

## How It Works

1. Queries the CKAN API for the "database-update-package-xml" dataset
2. Downloads ZIP packages (each contains inner A.zip for Acts, R.zip for Regulations)
3. Extracts XML files from inner ZIPs
4. Parses SAOPC Exchange DTD XML for title, date, and full text
5. Deduplicates by doc_id (year.number)

## Usage

```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # Full pull (all 217+ packages)
python bootstrap.py update             # Latest 3 packages only
```

## Notes

- 217+ fortnightly packages from Nov 2017 to present
- Each package contains only legislation changed in that fortnight
- Full corpus requires processing all packages (dedup by doc_id keeps latest)
- XML uses custom SAOPC Exchange DTD with `<exdoc>` root element
