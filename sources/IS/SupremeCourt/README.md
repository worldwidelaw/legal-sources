# IS/SupremeCourt - Icelandic Supreme Court (Hæstiréttur)

## Overview

This source fetches case law from the Icelandic Supreme Court (Hæstiréttur Íslands).

## Data Source

- **Website**: https://www.haestirettur.is/domar/
- **Type**: Case law
- **Language**: Icelandic
- **License**: Public Domain (official court decisions)

## Access Method

HTML scraping of the official Supreme Court website. Individual decisions are accessed via:
`https://www.haestirettur.is/domar/_domur/?id={UUID}`

## Fields

| Field | Description |
|-------|-------------|
| _id | Case number (e.g., "1/2026") |
| case_number | Same as _id |
| title | Case title with parties |
| date | Decision date (ISO 8601) |
| text | Full decision text including summary |
| keywords | Legal keywords (lykilorð) |
| appellants | Appellant parties |
| plaintiffs | Plaintiff/respondent parties |
| summary | Decision summary (reifun) |
| court | "Hæstiréttur Íslands" |
| uuid | Internal document UUID |

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Fetch all records
python3 bootstrap.py fetch
```

## Notes

- The Supreme Court sits in Reykjavík
- Seven justices sit on the court; five hear each case (seven for important cases)
- Decisions are published online after they are rendered
- No API is available; HTML scraping is required
