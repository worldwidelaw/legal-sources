# IS/Felagsdomur - Icelandic Labour Court (Félagsdómur)

## Overview

This source fetches case law from the Icelandic Labour Court (Félagsdómur).

## Data Source

- **Website**: https://www.felagsdomur.is/
- **Type**: Case law
- **Language**: Icelandic
- **License**: Public Domain (official court decisions)
- **Volume**: ~200 decisions (2010-present)

## Access Method

HTML scraping of the official Labour Court website. The site uses AJAX load-more pagination for the decision listing. Individual decisions are accessed via:
`https://www.felagsdomur.is/domar-og-urskurdir/domur-urskurdur/?id={UUID}`

Case numbers follow the format `F-N/YYYY` (e.g., `F-1/2024`).

## Fields

| Field | Description |
|-------|-------------|
| _id | Case number (e.g., "F-1/2024") |
| case_number | Same as _id |
| title | Case title |
| date | Decision date (ISO 8601) |
| text | Full decision text |
| plaintiff | Plaintiff/union party |
| defendant | Defendant/employer party |
| keywords | Legal keywords |
| abstract | Decision summary (útdráttur) |
| court | "Félagsdómur" |

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

- Félagsdómur handles disputes arising from collective bargaining agreements
- Five judges hear each case
- Decisions are rendered in Icelandic
- The court was established by the Act on Trade Unions and Industrial Disputes (nr. 80/1938)
- Verdict full text is accessible within a sr-only div on the decision detail page
