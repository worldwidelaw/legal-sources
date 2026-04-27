# CY/SupremeCourt - Cyprus Supreme Court Case Law

## Overview

This source fetches case law from the Cyprus Supreme Court via CyLaw (cylaw.org).

- **Data type:** case_law
- **Coverage:** 1961-present (35,485+ decisions as of 2024)
- **License:** Open Access - Cyprus Bar Association
- **Update frequency:** Continuous (new decisions added regularly)

## Data Source

- **URL:** http://www.cylaw.org/apofaseis/aad/
- **Organization:** Cyprus Bar Association / KINOP (Cyprus Institute of Legal Information)
- **Method:** HTML scraping via year index pages and document detail pages

## Document Structure

Documents are organized by year and "meros" (part/division):
- Part 1: Civil cases
- Part 2: Criminal cases
- Part 3: Administrative cases
- Part 4: Other

File path pattern: `/apofaseis/aad/meros_{part}/{year}/{filename}.htm`

## Metadata

Metadata is embedded in HTML comments:
- `<!--sino date DD/MM/YYYY-->` - Decision date
- `<!--number ...-->` - Case number
- `<!--court ...-->` - Court name
- `<!--plaintiff ...-->` - Plaintiff/Appellant
- `<!--defendant ...-->` - Defendant/Respondent
- `<!--jurisdiction ...-->` - Legal jurisdiction/area

## Usage

```bash
# Fetch 12 sample records
python bootstrap.py bootstrap --sample

# Fetch all records (warning: 35,485+ documents)
python bootstrap.py bootstrap

# Fetch updates since a date
python bootstrap.py update
```

## Notes

- Documents are in Greek (windows-1253 encoding)
- Full text is extracted from HTML, with styles/scripts removed
- Average document size: ~60KB HTML, ~20K+ characters text

## License

[Open Access](http://www.cylaw.org) — Cyprus court decisions are freely accessible via the Cyprus Bar Association.
