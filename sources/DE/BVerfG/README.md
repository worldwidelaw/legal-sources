# DE/BVerfG - German Federal Constitutional Court

## Overview

This fetcher retrieves case law from the **Bundesverfassungsgericht** (German Federal Constitutional Court) via the official rechtsprechung-im-internet.de portal.

## Data Source

- **Portal**: https://www.rechtsprechung-im-internet.de
- **Provider**: Federal Ministry of Justice and Consumer Protection / Federal Office of Justice
- **Coverage**: Decisions from 1998 onwards (with selected older decisions)
- **Data type**: Case law

## Access Method

The fetcher uses:

1. **RSS Feed** (`bsjrs-bverfg.xml`): Provides recent decisions with document IDs
2. **HTML Scraping**: Fetches full text (Langtext) from decision pages

### URL Patterns

- RSS feed: `https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bverfg.xml`
- Full text: `https://www.rechtsprechung-im-internet.de/jportal/portal/page/bsjrsprod.psml?doc.id={id}&doc.part=L`

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| _id | string | Document ID (e.g., "KVRE464552601") |
| _source | string | Always "DE/BVerfG" |
| _type | string | Always "case_law" |
| _fetched_at | ISO8601 | Fetch timestamp |
| title | string | Decision title/summary |
| text | string | Full decision text |
| date | ISO8601 | Decision date |
| url | string | Permalink to original |
| ecli | string | European Case Law Identifier |
| aktenzeichen | string | Case file number |
| court | string | Court name (BVerfG + Senate) |
| decision_type | string | Type of decision (Urteil, Beschluss, etc.) |

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).

## Usage

```bash
# Test fetcher
python3 bootstrap.py

# Fetch sample data (10 documents)
python3 bootstrap.py bootstrap --sample
```

## Limitations

- The RSS feed only contains recent decisions
- For historical coverage, would need to implement search interface scraping
- Some decisions may only have "Kurztext" (short text) without full reasoning
