# PT/ConstitutionalCourt — Portuguese Constitutional Court

**Source:** Tribunal Constitucional de Portugal
**URL:** https://www.tribunalconstitucional.pt
**Data Type:** Case Law
**License:** Public (Open Government Data)
**Language:** Portuguese

## Overview

This source fetches decisions (Acórdãos) from the Portuguese Constitutional Court. The court publishes all decisions on its official website in HTML format, with decisions available from 1983 onwards.

## Data Access

Decisions are accessed via direct URL patterns:

```
https://www.tribunalconstitucional.pt/tc/acordaos/{YYYY}{NNNN}.html
```

Where:
- `YYYY` = 4-digit year (1983-present)
- `NNNN` = 4-digit decision number (zero-padded)

Example: `https://www.tribunalconstitucional.pt/tc/acordaos/20240001.html`

## Coverage

- **Time range:** 1983 to present
- **Decision count:** ~500-1200 decisions per year (varies)
- **Total decisions:** ~40,000+ decisions

## Case Types

The Constitutional Court handles:
- **Abstract judicial review** (fiscalização abstracta) - reviewing constitutionality of laws
- **Concrete judicial review** (fiscalização concreta) - constitutional questions from lower courts
- **Electoral matters** - election disputes and verification
- **Political party matters** - party registration and financing
- **Presidential vacancy** - confirming incapacity

## Fields Extracted

| Field | Description |
|-------|-------------|
| `_id` | Unique ID: `TC-{year}-{number}` |
| `decision_number` | Decision number within the year |
| `decision_year` | Year of the decision |
| `case_number` | Case file number (Processo n.º) |
| `rapporteur` | Reporting judge (Relator) |
| `formation` | Deciding body (Plenário, 1ª/2ª/3ª Secção) |
| `text` | Full text of the decision |
| `url` | Link to official decision page |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12 decisions)
python bootstrap.py bootstrap --sample

# Full bootstrap (all decisions)
python bootstrap.py bootstrap

# Update with recent decisions
python bootstrap.py update
```

## Technical Notes

- Rate limiting: 1.5 seconds between requests
- Full text extracted from `div.textoacordao` element
- HTML entities decoded and tags stripped for clean text
- Binary search used to efficiently find decision ranges per year

## Search Interface

For reference, the court also provides a search interface at:
https://acordaosv22.tribunalconstitucional.pt/

This search interface requires form submission and is not used by this scraper.
