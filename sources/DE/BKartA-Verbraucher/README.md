# DE/BKartA-Verbraucher — German Consumer Protection (vzbv)

Consumer protection publications from the Verbraucherzentrale Bundesverband (vzbv).

## Data Access

- **Method**: XML Sitemap discovery + HTML article scraping
- **Sitemap**: `https://www.vzbv.de/sitemap.xml` (3 pages)
- **Content**: Full text extracted from HTML article pages
- **Rate limit**: 1.5s between requests
- **Estimated total**: ~3,761 documents

## Content Types

| URL Prefix | Type | Description |
|---|---|---|
| `/meldungen/` | Policy statements | Consumer policy news and analysis |
| `/urteile/` | Court ruling summaries | Summaries of vzbv litigation outcomes |
| `/publikationen/` | Publications | Position papers, fact sheets, reports |
| `/pressemitteilungen/` | Press releases | Official announcements |
| `/stellungnahmen/` | Position papers | Formal policy positions |

## Usage

```bash
# Fetch sample (15 records)
python3 bootstrap.py bootstrap --sample

# Fetch all records
python3 bootstrap.py bootstrap --full
```

## License

Public domain official works under German law (§ 5 UrhG).
