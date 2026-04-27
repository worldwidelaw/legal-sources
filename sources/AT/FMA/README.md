# AT/FMA - Austrian Financial Market Authority

**Source:** Austrian Financial Market Authority (Finanzmarktaufsicht)
**URL:** https://www.fma.gv.at
**Country:** Austria (AT)

## Overview

The Austrian Financial Market Authority (FMA) is Austria's integrated financial regulatory authority, responsible for supervising banks, insurance companies, pension funds, investment services providers, and the securities market.

## Data Access

This source uses the FMA's public RSS feed to fetch regulatory decisions:

- **RSS Feed:** `https://www.fma.gv.at/feed/`
- **Pagination:** `?paged=N` (approximately 380 pages available)
- **Full Text:** Available via `content:encoded` element in RSS items

### Categories Available

- **Sanktion** - Administrative sanctions and penalties
- **Warnung** - Warnings about unauthorized providers
- **Pressemitteilung** - Press releases
- **Information** - Informational announcements
- **Publikation** - Publications and reports

## Data Types

| Type | Description |
|------|-------------|
| `regulatory_decisions` | Sanctions, warnings, and regulatory actions |

## Authentication

No authentication required. The RSS feed is publicly accessible.

## Rate Limiting

- 1 request per second
- 3 request burst capacity

## Technical Notes

- The FMA website is protected by Cloudflare, but the main RSS feed remains accessible
- Category-specific feeds (e.g., `/category/news/sanktion/feed/`) are blocked by Cloudflare
- Full text content is embedded in `content:encoded` XML element
- Dates are in RFC 2822 format (e.g., "Fri, 13 Mar 2026 08:18:17 +0000")

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Sample Record Structure

```json
{
  "_id": "FMA-31242865",
  "_source": "AT/FMA",
  "_type": "regulatory_decisions",
  "_fetched_at": "2026-03-15T12:00:00+00:00",
  "title": "Bekanntmachung: FMA verhängt Sanktion...",
  "text": "Die österreichische Finanzmarktaufsichtsbehörde (FMA) hat...",
  "date": "2026-03-11",
  "url": "https://www.fma.gv.at/bekanntmachung-fma-verhaengt-sanktion...",
  "category": "Sanktion",
  "post_id": "31242865",
  "guid": "https://www.fma.gv.at/?p=31242865"
}
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Austrian Open Government Data.
