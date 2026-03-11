# CZ/SupremeCourt - Czech Supreme Court (Nejvyšší soud)

## Source Information

- **URL**: https://sbirka.nsoud.cz
- **Name**: Sbírka soudních rozhodnutí a stanovisek (Collection of Judicial Decisions and Opinions)
- **Country**: Czech Republic (CZ)
- **Data Type**: Case Law
- **Language**: Czech (cs)
- **License**: Open Government Data

## Description

The Collection of Judicial Decisions and Opinions ("Green Collection") is published by the Czech Supreme Court. Since January 2022, the collection is available exclusively in electronic form, free of charge.

The collection contains significant Supreme Court decisions that have been approved by the Criminal Collegium or Civil and Commercial Collegium. Each decision includes a "právní věta" (legal principle) that summarizes the key legal holding.

## Data Access

### Discovery
- WordPress sitemaps at `https://sbirka.nsoud.cz/sitemap_index.xml`
- Collection sitemaps: `collection-sitemap.xml` through `collection-sitemap10.xml`

### Individual Decisions
- URL pattern: `https://sbirka.nsoud.cz/sbirka/{id}/`
- Full text available in HTML format
- PDF and ePUB downloads available

### Updates
- RSS feed: `https://sbirka.nsoud.cz/feed/`
- Announcements of new issues with links to individual decisions

## Decision Types

- **Rozsudek** - Judgment
- **Usnesení** - Resolution
- **Stanovisko** - Legal Opinion/Position

## Collegia (Divisions)

- **Trestní kolegium** - Criminal Collegium
- **Občanskoprávní a obchodní kolegium** - Civil and Commercial Collegium

## Identifiers

- **ECLI**: European Case Law Identifier
  - Format: `ECLI:CZ:NS:{year}:{senate}.{type}.{number}.{year}.{ordinal}`
  - Example: `ECLI:CZ:NS:2025:3.TZ.17.2025.1`
- **Spisová značka**: Case reference number
  - Example: `3 Tz 17/2025`
- **Sbírkové číslo**: Collection number
  - Example: `1/2026`

## Normalized Schema

```json
{
  "_id": "ECLI:CZ:NS:2025:3.TZ.17.2025.1",
  "_source": "CZ/SupremeCourt",
  "_type": "case_law",
  "_fetched_at": "2026-02-11T...",
  "title": "Rozsudek Nejvyššího soudu ze dne 28. 5. 2025, sp. zn. 3 Tz 17/2025",
  "case_reference": "3 Tz 17/2025",
  "ecli": "ECLI:CZ:NS:2025:3.TZ.17.2025.1",
  "text": "...(full text of decision)...",
  "legal_principle": "...(právní věta)...",
  "decision_type": "Rozsudek",
  "court": "Nejvyšší soud",
  "date": "2025-05-28",
  "keywords": ["Neslučitelnost trestních sankcí", "Souhrnný trest"],
  "url": "https://sbirka.nsoud.cz/sbirka/25170/",
  "language": "cs"
}
```

## Usage

```bash
# Test fetcher
python3 bootstrap.py

# Bootstrap with 12 sample records
python3 bootstrap.py bootstrap --sample

# Full bootstrap
python3 bootstrap.py bootstrap
```

## Notes

- Rate limiting: 1 request per second
- Decisions are organized by year and issue (sešit)
- Full text includes the main decision text without procedural headers
- The "právní věta" (legal principle) is extracted separately
