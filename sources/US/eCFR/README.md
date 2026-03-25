# US/eCFR — Electronic Code of Federal Regulations

**Source:** [eCFR](https://www.ecfr.gov/)

## Overview

The complete US Code of Federal Regulations (50 titles, thousands of parts).
Updated daily by the Office of the Federal Register. Public domain.

## Data Access

Official REST/XML API at `ecfr.gov/api/versioner/v1/`. No authentication.

**Endpoints:**
- `GET /titles` — list all 50 CFR titles with current dates
- `GET /structure/{date}/title-{N}.json` — part structure for a title
- `GET /full/{date}/title-{N}.xml?part={P}` — full XML text for a part

## Record Fields

| Field | Description |
|-------|-------------|
| `cfr_citation` | Citation (e.g., "1 CFR Part 1") |
| `title` | Part heading |
| `text` | Full text of the regulation |
| `date` | Current-as-of date |
| `title_number` | CFR title number (1-50) |
| `part_number` | Part number within the title |

## Usage

```bash
python bootstrap.py test-api             # Test connectivity
python bootstrap.py bootstrap --sample   # Fetch 15 sample parts
python bootstrap.py bootstrap            # Full pull (all 50 titles)
```

## License

Public domain (US government works).
