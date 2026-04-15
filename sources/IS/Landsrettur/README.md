# IS/Landsrettur - Icelandic Court of Appeals (Landsréttur)

## Overview

This source fetches case law from the Icelandic Court of Appeals (Landsréttur).

## Data Source

- **Website**: https://www.landsrettur.is/
- **Type**: Case law
- **Language**: Icelandic
- **License**: Public Domain (official court decisions)
- **Volume**: ~6,000 decisions (2018-present)

## Access Method

HTML scraping of the official Court of Appeals website. The site uses AJAX pagination for the decision listing. Individual decisions are accessed via:
`https://www.landsrettur.is/domar-og-urskurdir/domur-urskurdur/?Id={UUID}`

Case numbers follow the format `N/YYYY` (e.g., `102/2025`).

## Fields

| Field | Description |
|-------|-------------|
| _id | Case number (e.g., "102/2025") |
| case_number | Same as _id |
| title | Case title |
| date | Decision date (ISO 8601) |
| text | Full decision text |
| parties | Party names (plaintiff gegn defendant) |
| keywords | Legal keywords (lykilorð) |
| abstract | Decision summary (útdráttur) |
| court | "Landsréttur" |

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Fetch all records
python3 bootstrap.py fetch

# Fetch updates since a date
python3 bootstrap.py update --since 2024-01-01

# Show source info
python3 bootstrap.py info
```

## Notes

- Landsréttur was established in 2018 as Iceland's intermediate appellate court
- Located in Reykjavík, the court hears appeals from the district courts
- Three judges typically hear each case
- The robots.txt blocks some paths but AJAX endpoints are accessible
- Verdict full text is accessible within a sr-only div on the decision detail page
- Rate limit: 1.5 second delay between requests
