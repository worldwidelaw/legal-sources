# DE/NRW - North Rhine-Westphalia State Law

Official state legislation from recht.nrw.de, covering all state laws, ordinances, and administrative directives for Germany's most populous state.

## Coverage

- **State Laws (Gesetze)**: Including the NRW Constitution, state codes, and acts
- **Legal Ordinances (Rechtsverordnungen)**: Implementing regulations
- **Administrative Directives (Verwaltungsvorschriften)**: Internal guidelines
- **Official Announcements (Bekanntmachungen)**: State treaties, inter-state agreements

## Data Access

Uses the official sitemap (47 pages) to discover all legislation URLs:
- Sitemap index: `https://recht.nrw.de/sitemap.xml`
- Document URLs: `/lrgv/{type}/{date}-{title}/` or `/lrmb/{type}/{date}-{title}/`

Full text is available in HTML format with structured article markup.

## URL Patterns

```
/lrgv/gesetz/{DDMMYYYY}-{slug}/        - State laws
/lrgv/rechtsverordnung/{DDMMYYYY}-{slug}/ - Ordinances
/lrgv/bekanntmachung/{DDMMYYYY}-{slug}/   - Announcements
/lrmb/verwaltungsvorschrift/{DDMMYYYY}-{slug}/ - Admin directives
```

## Usage

```bash
# Test the fetcher
python3 bootstrap.py status

# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Full bootstrap (all ~47K documents)
python3 bootstrap.py bootstrap
```

## Sample Record

```json
{
  "_id": "NRW-16012026-verfassung-fuer-das-land-nordrhein-westfalen",
  "_source": "DE/NRW",
  "_type": "legislation",
  "title": "Verfassung für das Land Nordrhein-Westfalen",
  "doc_type": "gesetz",
  "date": "2026-01-16",
  "text": "Das Land Nordrhein-Westfalen...",
  "vollzitat": "Verfassung für das Land Nordrhein-Westfalen vom 28. Juni 1950...",
  "jurisdiction": "North Rhine-Westphalia (Nordrhein-Westfalen)"
}
```

## License

Public Domain under German Copyright Act (§ 5 UrhG). German government works (amtliche Werke) including laws, ordinances, and official announcements are not protected by copyright.

## Notes

- Rate limited to 1 request/second to respect server resources
- Documents include version history with links to previous versions
- PDF versions also available but HTML provides cleaner text extraction
- Gesetz- und Verordnungsblatt (GV. NRW.) archived since 1946
