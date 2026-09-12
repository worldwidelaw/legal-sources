# DK/DTIL - Danish Data Protection Authority (Datatilsynet)

## Overview

This source fetches GDPR enforcement decisions from the Danish Data Protection Authority (Datatilsynet).

**Website:** https://www.datatilsynet.dk
**Data Type:** Regulatory Decisions
**Language:** Danish
**Authentication:** None required (Open Data)

## Data Source

Datatilsynet publishes all its enforcement decisions on its website, split into three
sections that share the same page layout:

- `afgoerelser` — decisions taken under the GDPR (2018–present)
- `historiske-afgoerelser` — decisions under the pre-GDPR persondatalov (2000–2018)
- `tilladelser` — permits issued under databeskyttelsesloven § 10 for research/statistics

Decisions include supervisory inspections (tilsyn), complaint decisions (klagesager),
data breach reports (brud på persondatasikkerheden) and fines (bødesager).

## Strategy

1. **Discovery:** Parse the XML sitemap at `/sitemap.xml`
2. **Filter:** Extract URLs matching `/afgoerelser/{section}/YYYY/mon/slug`, then
   round-robin the three sections so a truncated run still covers all of them
3. **Fetch:** Download each HTML decision page
4. **Extract:** Read the article body from `div.news-page` (`p.lead` plus the
   `div.rich-text` blocks), and the date from the `data-date` attribute

## URL Patterns

- **Sitemap:** `https://www.datatilsynet.dk/sitemap.xml`
- **Decision:** `https://www.datatilsynet.dk/afgoerelser/afgoerelser/2024/jan/decision-slug`
- **Historical:** `https://www.datatilsynet.dk/afgoerelser/historiske-afgoerelser/2003/apr/decision-slug`
- **Permit:** `https://www.datatilsynet.dk/afgoerelser/tilladelser/2019/nov/2019-522-0168`

The site was rebuilt in 2025: the old `/Handlers/Sitemap.ashx` handler now returns
HTTP 404 and decision pages no longer use `<article>`.

## Fields

| Field | Description |
|-------|-------------|
| `_id` | Unique ID (DK-DTIL-{AFG\|HIST\|TILL}-{case_number}, or -{year}-{month}-{slug}) |
| `title` | Decision title |
| `text` | Full text of the decision |
| `date` | Decision date (YYYY-MM-DD) |
| `case_number` | Journal number (e.g., 2023-431-0001) |
| `section` | `afgoerelser`, `historiske-afgoerelser` or `tilladelser` |
| `url` | Link to original decision |
| `authority` | "Datatilsynet" |
| `language` | "da" (Danish) |

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Full bootstrap, concurrent (what the fleet runner invokes)
python bootstrap.py bootstrap-fast

# Incremental update
python bootstrap.py update
```

## Rate Limiting

- 1 request per second
- Burst of 3 requests allowed

## Coverage

~867 decisions listed in the sitemap: 369 GDPR decisions (2018–present), 343
historical decisions (2000–2018) and 155 § 10 permits (2019–present). Topics cover:
- GDPR Article violations
- Data breach handling
- Consent requirements
- Data subject rights
- International transfers
- Permits for processing in research and statistics

## License

Public domain — Danish government decisions are not subject to copyright under [Danish Copyright Act §9](https://www.retsinformation.dk/eli/lta/2023/164).
